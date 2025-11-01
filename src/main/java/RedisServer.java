import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisServer {
    // BASe64
    private static final String RDB =
            "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    private final UUID id;
    // In-memory hashmap to store key:value,expiration
    private HashMap<String, Value<?>> map;
    private int clientCount;
    // List of Replicas
    private List<Client> replicas;
    private int port;
    private String replicaOf;
    private String replicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private boolean replicationStatus;
    private long offset = 0;
    private CommandHandler commandHandler;
    private List<String> enqueuedOutputs = new ArrayList<>();
    private List<String> commands = new ArrayList<>();
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    private Client master; // Master client. Initiated when handshake is completed.

    public RedisServer(UUID id) {
        this.id = id;
        clientCount = 0;
        map = new HashMap<>();
        port = 6379;
        commandHandler = new CommandHandler(this, map);
        replicas = new ArrayList<>();
        replicationStatus = false;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getReplicaOf() {
        return this.replicaOf;
    }

    public void setReplicaOf(String replicaOf) {
        this.replicaOf = replicaOf;
    }

    public int getClientCount() {
        return this.clientCount;
    }

    public String getReplicationId() {
        return this.replicationId;
    }

    public synchronized long getOffset() {
        return this.offset;
    }

    public void start() {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        //System.out.println("Logs from your program will appear here!");

        //  Uncomment the code below to pass the first stage
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(port);
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);

            // Replica
            if (replicaOf != null) {
                String ip = getMasterIp();
                int port = getMasterPort();
                if (ip == null || port == -1) {
                    System.out.println("Replica does not have master configured");
                }
                Socket masterSocket = new Socket(ip, port);

                initiateHandshake(masterSocket);
                startReceivingCommandsFromMaster();
            }

            int id = 0;

            while (true) {
                // Wait for connection from client.
                Socket clientSocket = serverSocket.accept();
                ++clientCount;
                ++id;
                Socket assignedSocket = clientSocket;

                Client client = new Client(id, assignedSocket);

                CompletableFuture.runAsync(() -> {
                    try {
                        handleClient(client);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Used by Replicas to initiate connection with master.
    private void initiateHandshake(Socket masterSocket) throws Exception {
        try {
            // Send PING to master
            String pingResponse = sendPing(masterSocket);
            if (pingResponse.equals("+PONG")) {
                // Send REPLCONF to master
                String replConfResponse = sendReplconfMessage(masterSocket);
                if (replConfResponse.equals("+OK")) {
                    // Send REPLCONF CAPA PSYNC2 to master
                    String replConfCapaPsync2 = sendReplconfCapaPsync2(masterSocket);
                    if (replConfCapaPsync2.equals("+OK")) {
                        String psync = sendPsync(masterSocket);
                        if (psync.contains("FULLRESYNC")) {
                            String RDB = receiveRDB(masterSocket);
                            // TODO: Handle the response here
                            //  System.out.println("Received RDB file");
                            replicationStatus = true;

                            master = new Client(0, masterSocket);
                            System.out.println("Master Handshake complete");
                        }
                    } else {
                        System.out.println("Failed on REPLCONF capa psync");
                    }
                } else {
                    System.out.println("Failed on REPLCONF listening-port");
                }
            } else {
                System.out.println("Replica failed to PING master");
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    // Replica
    public void startReceivingCommandsFromMaster() throws IOException, InterruptedException {
        System.out.println("Starting to receive commands from master = " + master);
        CompletableFuture.runAsync(() -> {
            try {
                handleClient(master);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void handleClient(Client client) throws IOException, InterruptedException {
        try {
            while (true) {
                String input = client.read();

                List<String> inputs = populateInputs(input);
                for (String i : inputs) {
                    String cleanedInput = i.strip();
                    if (!cleanedInput.isBlank() && !cleanedInput.equals("")) {
                        String output = commandHandler.processInput(cleanedInput, client);
                        if (output != null && !output.isBlank()) {
                            if (replicaOf != null) {
                                if (!RespResponseUtility.shouldSendToReplica(cleanedInput)) {
                                    client.send(output);
                                }
                                synchronized (this) {
                                    offset += input.length();
                                }
                            } else {
                                client.send(output);
                                executorService.execute(() -> {
                                    try {
                                        propagateToReplicas(input);
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("InterrupedtedException");
            throw new RuntimeException(e);
        } catch (Exception e) {
            System.out.println("Caught exception: " + e.getMessage());
        } finally {
            --clientCount;
            client.closeSocket();
        }
    }

    private void propagateToReplicas(String input) throws IOException {
        input = input.replaceAll("\0+$", "");
        if (!RespResponseUtility.shouldSendToReplica(input)) return;

        for (int i = 0; i < replicas.size(); ++i) {
            replicas.get(i).send(input);
        }
    }

    void registerReplica(Client replica) {
        replicas.add(replica);
    }

    private String getMasterIp() {
        if (replicaOf == null) return null;
        String[] hostIp = replicaOf.split(" ");
        return hostIp[0];
    }

    private int getMasterPort() {
        if (replicaOf == null) return -1;
        String[] hostIp = replicaOf.split(" ");
        return Integer.parseInt(hostIp[1]);
    }

    private String sendPing(Socket socket) throws IOException {
        InputStream inputStream = socket.getInputStream();
        OutputStream outputStream = socket.getOutputStream();
        outputStream.write(RespResponseUtility.getRespArray(Collections.singletonList("PING")).getBytes());

        byte[] buffer = new byte[1024];
        try{
            inputStream.read(buffer);
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
        String output = new String(buffer);
        output = output.replaceAll("\0+$", "").strip();

        return output;
    }

    private String sendReplconfMessage(Socket socket) throws IOException {
        InputStream inputStream = socket.getInputStream();
        OutputStream outputStream = socket.getOutputStream();

        List<String> replconf = new ArrayList<>();
        replconf.add("REPLCONF");
        replconf.add("listening-port");
        replconf.add(String.valueOf(this.port));

        outputStream.write(RespResponseUtility.getRespArray(replconf).getBytes());

        byte[] buffer = new byte[1024];
        try{
            inputStream.read(buffer);
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
        String output = new String(buffer);
        output = output.replaceAll("\0+$", "").strip();

        return output;
    }

    private String sendReplconfCapaPsync2(Socket socket) throws IOException {
        InputStream inputStream = socket.getInputStream();
        OutputStream outputStream = socket.getOutputStream();

        List<String> capa = new ArrayList<>();
        capa.add("REPLCONF");
        capa.add("capa");
        capa.add("psync2");

        outputStream.write(RespResponseUtility.getRespArray(capa).getBytes());

        byte[] buffer = new byte[1024];
        try{
            inputStream.read(buffer);
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
        String output = new String(buffer);
        output = output.replaceAll("\0+$", "").strip();

        return output;
    }

    private String sendPsync(Socket socket) throws IOException {
        InputStream inputStream = socket.getInputStream();
        OutputStream outputStream = socket.getOutputStream();

        List<String> psync = new ArrayList<>();
        psync.add("PSYNC");
        psync.add("?");
        psync.add("-1");

        outputStream.write(RespResponseUtility.getRespArray(psync).getBytes());

        int data;
        StringBuilder output = new StringBuilder();
        while((data = inputStream.read()) != -1) {
            char c = (char) data;
            output.append(c);

            if (c == '\n') break;
        }

        output = new StringBuilder(output.toString().replaceAll("\0+$", "").strip());

        return output.toString();
    }

    private String receiveRDB (Socket socket) throws IOException {
        InputStream inputStream = socket.getInputStream();
        int data = inputStream.read(); // $

        // Get RDB size
        StringBuilder size = new StringBuilder();
        while((data = inputStream.read()) != -1) {
            char c = (char) data;
            size.append((char)data);

            if (c == '\n') break;
        }

        int rdbSize = Integer.parseInt(size.toString().strip());

        byte[] input = new byte[rdbSize];
        try{
            inputStream.read(input);
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
        String output = new String(input);

        return output;
    }

    private List<String> populateInputs(String input) {
        if (!input.contains("*")) return Collections.singletonList(input);
        if (input.contains("*\r")) return Collections.singletonList(input);
        String[] words = input.split("\r\n");
        List<String> result = new ArrayList<>();

        String word = "*";
        for (int i = 1; i < words.length; ++i) {
            if (words[i].startsWith("*")) {
                result.add(word);
                word = "*";
            } else {
                word += "\r\n";
                word += words[i];
            }
        }
        result.add(word);

        return result;
    }
}
import java.io.BufferedReader;
import java.io.IOException;
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
    private long offset = 0;
    private CommandHandler commandHandler;
    private List<String> enqueuedOutputs = new ArrayList<>();
    private List<String> commands = new ArrayList<>();
    private int cnt;

    public RedisServer(UUID id) {
        this.id = id;
        clientCount = 0;
        map = new HashMap<>();
        port = 6379;
        commandHandler = new CommandHandler(this, map);
        replicas = new ArrayList<>();
        cnt = 1;
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

    public long getOffset() {
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

            if (replicaOf != null) {
                try {
                    initiateHandshake();
                    System.out.println("Master Handshake complete");
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage());
                }
            }

            int id = 0;

            while (true) {
                // Wait for connection from client.
                Socket clientSocket = serverSocket.accept();
                ++clientCount;
                ++id;
                Socket assignedSocket = clientSocket;

                Client client = new Client(id, assignedSocket, assignedSocket.getInputStream(),
                        assignedSocket.getOutputStream());

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
        }
    }

    // Used by Replicas to initiate connection with master.
    private void initiateHandshake() throws Exception {
        String ip = getMasterIp();
        int port = getMasterPort();
        if (ip == null || port == -1) {
            throw new Exception("Replica does not have master configured");
        }

        Socket replicaSocket = new Socket(ip, port);
        BufferedReader reader = new BufferedReader(new InputStreamReader(replicaSocket.getInputStream()));
        OutputStream outputStream = replicaSocket.getOutputStream();

        try {
            // Send PING to master
            outputStream.write(RespResponseUtility.getRespArray(Collections.singletonList("PING")).getBytes());
            if (reader.readLine().equals("+PONG")) {
                // Send REPLCONF to master
                sendReplconfMessage(outputStream);
                if (reader.readLine().equals("+OK")) {
                    // Send REPLCONF CAPA PSYNC2 to master
                    sendReplconfCapaPsync2(outputStream);
                    if (reader.readLine().equals("+OK")) {
                        sendPsync(outputStream);
                        // TODO: Handle the response here
                        //startReceivingCommands(reader);
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
        } finally {
            replicaSocket.close();
        }
    }

//    private void startReceivingCommands(BufferedReader reader) {
//        System.out.println("Replica ready to receive commands from master");
//        String input = "";
//
//        while (true) {
//            try{
//                input = reader.readLine();
//            } catch (IOException e) {
//                System.out.println("IOException: " + e.getMessage());
//            }
//        }
//    }

    private void handleClient(Client client) throws IOException, InterruptedException {
        try {
            while (true) {
                String input = new String(client.read());
                //String output = processInput(input, client);
                if (input != null) {
                    String output = commandHandler.processInput(input, client);
                    if (output != null && !output.isBlank()) {
                        client.send(output);
                        CompletableFuture.runAsync(() -> {
                            try {
                                propagateToReplicas(input);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
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
        String result = "";
        cnt++;

        for (int i = 0; i < replicas.size(); ++i) {
            result = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n";
            result += "*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n";
            result += "*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n";
            if (cnt % 3 == 0) replicas.get(i).send(result);
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

    private void sendReplconfMessage(OutputStream outputStream) throws IOException {
        List<String> replconf = new ArrayList<>();
        replconf.add("REPLCONF");
        replconf.add("listening-port");
        replconf.add(String.valueOf(this.port));

        outputStream.write(RespResponseUtility.getRespArray(replconf).getBytes());
    }

    private void sendReplconfCapaPsync2(OutputStream outputStream) throws IOException {
        List<String> capa = new ArrayList<>();
        capa.add("REPLCONF");
        capa.add("capa");
        capa.add("psync2");

        outputStream.write(RespResponseUtility.getRespArray(capa).getBytes());
    }

    private void sendPsync(OutputStream outputStream) throws IOException {
        List<String> psync = new ArrayList<>();
        psync.add("PSYNC");
        psync.add("?");
        psync.add("-1");

        outputStream.write(RespResponseUtility.getRespArray(psync).getBytes());
    }
}
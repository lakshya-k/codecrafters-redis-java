import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Client {
    private int id;
    private Socket socket;
    private boolean inTransaction;
    private List<String[]> enqueuedCommands;
    private BufferedReader reader;

    public Client(int id, Socket socket) throws IOException {
        this.id = id;
        this.socket = socket;
        enqueuedCommands = new ArrayList<>();
        inTransaction = false;
        InputStream inputStream = socket.getInputStream();
        InputStreamReader isr = new InputStreamReader(inputStream, "UTF-8");
        reader = new BufferedReader(isr);
    }

    public int getId() {
        return id;
    }

    public String read() throws IOException {
        //int data = inputStream.read(); // * or $
        int data = reader.read();

        if(data == -1) return null;
        if (!String.valueOf((char) data).equals("*")) return null;

        // Get input size
        int inputSize = Integer.parseInt(readNext(reader).strip());

        StringBuilder command = new StringBuilder("*" + inputSize + "\r\n");

        for (int i = 0; i < 2 * inputSize; ++i) {
            String word = readNext(reader);
            while(word == null || word.isBlank()) word = readNext(reader);
            command.append(word);
        }
        return command.toString();
    }

    public String readNext(BufferedReader reader) throws IOException {
        StringBuilder result = new StringBuilder();
        int data;
        while((data = reader.read()) != -1) {
            char c = (char) data;
            result.append((char)data);

            if (c == '\n') break;
        }
        return result.toString();
    }

    public boolean send(String output) throws IOException {
        synchronized (this) {
            try {
                OutputStream outputStream = socket.getOutputStream();
                outputStream.write(output.getBytes());
                outputStream.flush();
                return true;
            } catch (IOException e) {
                return false;
            }
        }
    }

    public boolean send(byte[] output) throws IOException {
        try{
            if (output != null) {
                OutputStream outputStream = socket.getOutputStream();
                outputStream.write(output);
                outputStream.flush();
            }
            return true;
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
            return false;
        }
    }

    public boolean closeSocket() {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }

        return true;
    }

    public boolean beginTransaction() {
        if (inTransaction) return false;
        return inTransaction = true;
    }

    public boolean endTransaction() {
        if (!inTransaction) return false;
        enqueuedCommands.clear();
        return inTransaction = false;
    }

    public boolean isInTransaction() {
        return this.inTransaction;
    }

    public List<String[]> getEnqueuedCommands() {
        return this.enqueuedCommands;
    }

    public boolean enqueueCommand(String[] args) {
        enqueuedCommands.add(args);
        return true;
    }
}

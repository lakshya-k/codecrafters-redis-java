import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Client {
    private int id;
    private Socket socket;
    private boolean inTransaction;
    private List<String[]> enqueuedCommands;

    public Client(int id, Socket socket) throws IOException {
        this.id = id;
        this.socket = socket;
        enqueuedCommands = new ArrayList<>();
        inTransaction = false;
    }

    public String read() throws IOException {
        InputStream inputStream = socket.getInputStream();
        int data = inputStream.read(); // * or $

        if(data == -1) return null;

        // Get input size
        int inputSize = Integer.parseInt(readNext(inputStream).strip());

        StringBuilder command = new StringBuilder("*" + inputSize + "\r\n");

        for (int i = 0; i < 2 * inputSize; ++i) {
            String word = readNext(inputStream);
            command.append(word);
        }
        return command.toString();
    }

    public String readNext(InputStream inputStream) throws IOException {
        StringBuilder result = new StringBuilder();
        int data;
        while((data = inputStream.read()) != -1) {
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

    private int getWordSize(InputStream inputStream) throws IOException {
        int data = inputStream.read(); // $

        // Get word size
        StringBuilder size = new StringBuilder();
        while((data = inputStream.read()) != -1) {
            char c = (char) data;
            size.append((char)data);

            if (c == '\n') break;
        }

        return Integer.parseInt(size.toString().strip());
    }

    public Socket getSocket() {
        return this.socket;
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

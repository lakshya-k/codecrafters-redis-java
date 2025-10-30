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

    public byte[] read() {
        byte[] input = new byte[1024];
        try{
            InputStream inputStream = socket.getInputStream();
            inputStream.read(input);
            return input;
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }

        return input;
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

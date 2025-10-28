import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.LinkedHashMap;

public class Client {
    private int id;
    private Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;
    private boolean inTransaction;
    private LinkedHashMap<String, String[]> enqueuedCommands;

    public Client(int id, Socket socket, InputStream inputStream, OutputStream outputStream) {
        this.id = id;
        this.socket = socket;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        enqueuedCommands = new LinkedHashMap<>();
        inTransaction = false;
    }

    public String read() {
        String result = "";
        try{
            byte[] input = new byte[1024];
            inputStream.read(input);

            return new String(input);
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }

        return result;
    }

    public boolean send(String output) throws IOException {
        outputStream.write(output.getBytes());
        return true;
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

    public LinkedHashMap<String, String[]> getEnqueuedCommands() {
        return this.enqueuedCommands;
    }

    public boolean enqueueCommand(String command, String[] args) {
        enqueuedCommands.put(command, args);
        return true;
    }
}

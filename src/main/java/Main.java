import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    //  Uncomment the code below to pass the first stage
        ServerSocket serverSocket = null;
        int port = 6379;
        try {
          serverSocket = new ServerSocket(port);
          // Since the tester restarts your program quite often, setting SO_REUSEADDR
          // ensures that we don't run into 'Address already in use' errors
          serverSocket.setReuseAddress(true);


          while(true) {
              // Wait for connection from client.
              Socket clientSocket = serverSocket.accept();

              Thread thread = new Thread(new ClientHandler(clientSocket));
              thread.start();
          }

        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        }
  }

    public static class ClientHandler implements Runnable {
        Socket clientSocket;

        public ClientHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }
        public void run() {
            try {
                while (true) {
                    byte[] input = new byte[1024];
                    clientSocket.getInputStream().read(input);
                    String output = RespParser.parse(new String(input));
                    clientSocket.getOutputStream().write(output.getBytes());
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            } finally {
                try {
                    if (clientSocket != null) {
                        clientSocket.close();
                    }
                } catch (IOException e) {
                    System.out.println("IOException: " + e.getMessage());
                }
            }
        }
    }

    public static class RespParser {
        public static String parse (String input) {
            String[] words = input.split("\r\n");
            String output = "";
            if (words[2].equals("PING")) {
                output = "+PONG\r\n";
            }
            else if (words[2].equals("ECHO")) {
                output = "$" + words[4].length() + "\r\n" + words[4] + "\r\n";
            }

            return output;
        }
    }
}

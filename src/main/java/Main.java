import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;

public class Main {
    static HashMap<String, String> map = new HashMap<>();
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

              Runnable task = () -> {
                  try {
                      while (true) {
                          byte[] input = new byte[1024];
                          clientSocket.getInputStream().read(input);
                          String output = parse(new String(input));
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
              };

              Thread thread = new Thread(task);
              thread.start();
          }

        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        }
  }

    public static String parse(String input) {
        String[] words = input.split("\r\n");
        System.out.println("words[2] = " + words[2]);
        String output = "";
        if (words[2].equals("PING")) {
            output = "+PONG\r\n";
        } else if (words[2].equals("ECHO")) {
            output = "$" + words[4].length() + "\r\n" + words[4] + "\r\n";
        } else if (words[2].equals("SET")) {
            map.put(words[4], words[6]);
            output = "+OK\r\n";
        } else if (words[2].equals("GET")) {
            if (map.containsKey(words[4])) {
                output = map.get(words[4]);
                System.out.println("words[4]: " + words[4] + " map[words[4]] = " + map.get(words[4]));
                output = "$" + output.length() + "\r\n" + output + "\r\n";
            } else {
                output = "$-1\r\n";
            }
        }

        return output;
    }
}

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;

public class Main {
    static HashMap<String, String> keyValueMap = new HashMap<>();
    static HashMap<String, Long> keyExpiryMap = new HashMap<>();

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
        System.out.println("input: " + input + " end input");
        String[] words = input.split("\r\n");
        String output = "";
        if (words[2].equals("PING")) {
            output = "+PONG\r\n";
        } else if (words[2].equals("ECHO")) {
            output = "$" + words[4].length() + "\r\n" + words[4] + "\r\n";
        } else if (words[2].equals("SET")) {
            keyValueMap.put(words[4], words[6]);
            if (words.length > 8) {
                if (words[8].equals("PX")) {
                    keyExpiryMap.put(words[4], Long.parseLong(words[10]) + System.currentTimeMillis());
                } else if (words[8].equals("P")) {
                    keyExpiryMap.put(words[4], Long.parseLong(words[10]) * 1000 + System.currentTimeMillis());
                }
            }
            output = "+OK\r\n";
        } else if (words[2].equals("GET")) {
            if (keyValueMap.containsKey(words[4])) {
                if (keyExpiryMap.containsKey(words[4]) && keyExpiryMap.get(words[4]) < System.currentTimeMillis()) {
                    keyValueMap.remove(words[4]);
                    keyExpiryMap.remove(words[4]);
                    output = "$-1\r\n";
                } else {
                    output = keyValueMap.get(words[4]);
                    output = "$" + output.length() + "\r\n" + output + "\r\n";
                }
            } else {
                output = "$-1\r\n";
            }
        }

        return output;
    }
}

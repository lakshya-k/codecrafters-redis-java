import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class Main {
    // In-memory hashmap to store key:value,expiration
    static HashMap<String, MyPair<?>> map = new HashMap<>();
    // In-memory hashmap to store key:list
    static HashMap<String, List<String>> list = new HashMap<>();
    static Object lock = new Object();

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
                  } catch (InterruptedException e) {
                      throw new RuntimeException(e);
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

    public static String parse(String input) throws InterruptedException {
        String[] words = input.split("\r\n");
        String output = switch (words[2].toLowerCase()) {
            case "ping" -> ping();
            case "echo" -> echo(words);
            case "set" -> set(words);
            case "get" -> get(words);
            case "rpush" -> rpush(words);
            case "lrange" -> lrange(words);
            case "lpush" -> lpush(words);
            case "llen" -> llen(words);
            case "lpop" -> lpop(words);
            case "blpop" -> blpop(words);
            default -> "";
        };

        return output;
    }

    public static String ping() {
        return getSimpleString("PONG");
    }

    public static String echo(String[] words) {
        return getBulkString(words[4]);
    }

    public static String set(String[] words) {
        String key = words[4];
        MyPair<String> value;
        if (words.length > 8) {
            if (words[8].equals("PX")) {
                value = new MyPair<>(ValueType.STRING, words[6],
                        Long.parseLong(words[10]) + System.currentTimeMillis());
            } else {
                value = new MyPair<>(ValueType.STRING, words[6],
                        Long.parseLong(words[10]) * 1000 + System.currentTimeMillis());
            }
        } else {
            value = new MyPair<>(ValueType.STRING, words[6], -1L);
        }
        synchronized (lock) {
            map.put(key, value);
        }

        return getSimpleString("OK");
    }

    public static String get(String[] words) {
        String output = null;
        synchronized (lock) {
            if (map.containsKey(words[4])) {
                String value = (String) map.get(words[4]).getValue();
                Long expiry = map.get(words[4]).getExpiry();
                if (expiry == -1 || expiry > System.currentTimeMillis()) {
                    output = value;
                } else {
                    map.remove(words[4]);
                }
            }
        }

        return getBulkString(output);
    }

    public static String rpush(String[] words) {
        String key = words[4];
        List<String> elements = new ArrayList<>();
        for (int i = 6; i < words.length - 1; i = i + 2) {
            elements.add(words[i]);
        }
        List<String> values = new ArrayList<>();
        synchronized (lock) {
            if (map.containsKey(key)) {
                if (map.get(key).getValueType() == ValueType.LIST) {
                    values = (List<String>) map.get(key).getValue();
                } else {
                    return getErrorMessage("Value is not a list");
                }
            }
            values.addAll(elements);

            map.put(key, new MyPair<>(ValueType.LIST, values, -1L));
            lock.notifyAll();
        }
        return getRespInteger(values.size());
    }

    public static String lrange(String[] words) {
        String key = words[4];
        int l = Integer.parseInt(words[6]);
        int r = Integer.parseInt(words[8]);
        String output = "";
        synchronized (lock) {
            if (map.containsKey(key)) {
                if (map.get(key).getValueType() == ValueType.LIST) {
                    List<String> elements = (List<String>) map.get(key).getValue();
                    l = normalizeIndex(l, elements.size());
                    r = normalizeIndex(r, elements.size());
                    output = getRespArray(elements.subList(l, Math.min(r + 1, elements.size())));
                } else {
                    output = getErrorMessage("Value is not list");
                }
            } else {
                output = getRespArray(Collections.emptyList());
            }
        }

        return output;
    }

    public static String lpush(String[] words) {
        String key = words[4];
        List<String> elements = new ArrayList<>();
        for (int i = 6; i < words.length - 1; i = i + 2) {
            elements.add(words[i]);
        }
        Collections.reverse(elements);
        List<String> values = new ArrayList<>();
        synchronized (lock) {
            if (map.containsKey(key)) {
                if (map.get(key).getValueType() == ValueType.LIST) {
                    values = (List<String>) map.get(key).getValue();
                } else {
                    return getErrorMessage("Value is not a list");
                }
            }
            values.addAll(0, elements);

            map.put(key, new MyPair<>(ValueType.LIST, values, -1L));
            lock.notifyAll();
        }

        return getRespInteger(values.size());
    }

    public static String llen(String[] words) {
        String key = words[4];
        int res = 0;
        synchronized (lock) {
            if (map.containsKey(key)) {
                if (map.get(key).getValueType() == ValueType.LIST) {
                    List<String> values = (List<String>) map.get(key).getValue();
                    res = values.size();
                } else {
                    return getErrorMessage("Value is not a list");
                }
            }
        }

        return getRespInteger(res);
    }

    public static String lpop(String[] words) {
        String key = words[4];
        List<String> res = new ArrayList<>();
        int cnt = 1;
        if (words.length > 6) cnt = Integer.parseInt(words[6]);
        synchronized (lock) {
            if (map.containsKey(key)) {
                if (map.get(key).getValueType() == ValueType.LIST) {
                    List<String> values = (List<String>) map.get(key).getValue();
                    int originalSize = values.size();

                    for (int i = 0; i < cnt && i < originalSize; ++i) {
                        res.add(values.get(0));
                        values.remove(0);
                    }

                    map.put(key, new MyPair<>(ValueType.LIST, values, -1L));
                } else {
                    return getErrorMessage("Value is not a list");
                }
            }
        }

        return cnt == 1 ? getBulkString(res.get(0)) : getRespArray(res);
    }

    public static String blpop(String[] words) throws InterruptedException {
        String key = words[4];
        double timeout = Double.parseDouble(words[6]);
        timeout *= 1000;
        String res = null;

        synchronized (lock) {
            long beforeTimeInMillis = System.currentTimeMillis();
            long remainingTime = (long)timeout;
            List<String> values;
            if (timeout > 0) {
                while (!map.containsKey(key) && remainingTime > 0) {
                    lock.wait((long) timeout);
                    remainingTime = Math.min(0, beforeTimeInMillis + (long)timeout - System.currentTimeMillis());
                }
            } else {
                while (!map.containsKey(key) ) {
                    lock.wait();
                }
            }

            if (timeout > 0) {
                if (remainingTime > 0) {
                    values = (List<String>) map.get(key).getValue();
                    while (values.size() == 0) {
                        lock.wait(remainingTime);
                        remainingTime = Math.min(0, beforeTimeInMillis + (long)timeout - System.currentTimeMillis());
                    }
                }
            } else {
                values = (List<String>) map.get(key).getValue();
                while (values.size() == 0) {
                    lock.wait();
                }
            }

            if (map.containsKey(key)) {
                if (map.get(key).getValueType() == ValueType.LIST) {
                    values = (List<String>) map.get(key).getValue();
                    if (values.size() > 0) {
                        res = values.get(0);
                        values.remove(0);
                        map.put(key, new MyPair<>(ValueType.LIST, values, -1L));
                    } else {
                        return getRespArray(Collections.emptyList());
                    }
                } else {
                    return getErrorMessage("Value is not a list");
                }
            } else {
                return getNullArray();
            }
        }

        return getRespArray(Arrays.asList(key, res));
    }

    public static int normalizeIndex(int i, int l) {
        if (i >= 0) return i;
        return Math.max(0, l + i);
    }

    public static String getBulkString(String s) {
        if (s == null) return "$-1\r\n";
        return "$" + s.length() + "\r\n" + s + "\r\n";
    }

    public static String getSimpleString(String s) {
        return "+" + s + "\r\n";
    }

    public static String getRespInteger(int i) {
      return ":" + i + "\r\n";
    }

    public static String getErrorMessage(String error) {
      return "-" + error + "\r\n";
    }

    public static String getRespArray(List<String> elements) {
        StringBuilder res = new StringBuilder("*" + elements.size());
        for (String e : elements) res.append("\r\n$").append(e.length()).append("\r\n").append(e);
        res.append("\r\n");

        return res.toString();
    }

    public static String getNullArray() {
        return "*-1\r\n";
    }

    public static class MyPair<V> {
        private final ValueType type;
        private final V value;
        private final Long expiry;

        public MyPair(ValueType type, V value, Long expiry) {
            this.type = type;
            this.value = value;
            this.expiry = expiry;
        }

        public ValueType getValueType() {
            return type;
        }

        public V getValue() {
            return value;
        }

        public Long getExpiry() {
            return expiry;
        }

        @Override
        public String toString() {
            return "(" + type + ", " + value + ", " + expiry + ")";
        }
    }

    public enum ValueType {
        STRING,
        LIST,
        DEFAULT
    }
}

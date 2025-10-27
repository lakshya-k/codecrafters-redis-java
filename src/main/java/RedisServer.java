import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class RedisServer {
    // In-memory hashmap to store key:value,expiration
    static HashMap<String, Value<?>> map;

    public RedisServer() {
        map = new HashMap<>();
    }

    public void start() {
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

            while (true) {
                // Wait for connection from client.
                Socket clientSocket = serverSocket.accept();

                Runnable task = () -> {
                    try {
                        while (true) {
                            byte[] input = new byte[1024];
                            clientSocket.getInputStream().read(input);
                            String output = parseCommand(new String(input));
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

    private String parseCommand(String input) throws InterruptedException {
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
            case "type" -> type(words);
            case "xadd" -> xadd(words);
            case "xrange" -> xrange(words);
            case "xread" -> xread(words);
            default -> "";
        };

        return output;
    }

    private static String ping() {
        return RespResponseUtility.getSimpleString("PONG");
    }

    private static String echo(String[] words) {
        return RespResponseUtility.getBulkString(words[4]);
    }

    private String set(String[] words) {
        String key = words[4];
        Value<String> value;
        if (words.length > 8) {
            if (words[8].equals("PX")) {
                value = new Value<>(ValueType.STRING, words[6],
                        Long.parseLong(words[10]) + System.currentTimeMillis());
            } else {
                value = new Value<>(ValueType.STRING, words[6],
                        Long.parseLong(words[10]) * 1000 + System.currentTimeMillis());
            }
        } else {
            value = new Value<>(ValueType.STRING, words[6], -1L);
        }
        synchronized (this) {
            map.put(key, value);
        }

        return RespResponseUtility.getSimpleString("OK");
    }

    private String get(String[] words) {
        String output = null;
        synchronized (this) {
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

        return RespResponseUtility.getBulkString(output);
    }

    private String rpush(String[] words) {
        String key = words[4];
        List<String> elements = new ArrayList<>();
        for (int i = 6; i < words.length - 1; i = i + 2) {
            elements.add(words[i]);
        }
        List<String> values = new ArrayList<>();
        synchronized (this) {
            if (map.containsKey(key)) {
                if (map.get(key).getValueType() == ValueType.LIST) {
                    values = (List<String>) map.get(key).getValue();
                } else {
                    return RespResponseUtility.getErrorMessage("Value is not a list");
                }
            }
            values.addAll(elements);

            map.put(key, new Value<>(ValueType.LIST, values, -1L));
            this.notifyAll();
        }
        return RespResponseUtility.getRespInteger(values.size());
    }

    private String lrange(String[] words) {
        String key = words[4];
        int l = Integer.parseInt(words[6]);
        int r = Integer.parseInt(words[8]);
        String output = "";
        synchronized (this) {
            if (map.containsKey(key)) {
                if (map.get(key).getValueType() == ValueType.LIST) {
                    List<String> elements = (List<String>) map.get(key).getValue();
                    l = RespResponseUtility.normalizeIndex(l, elements.size());
                    r = RespResponseUtility.normalizeIndex(r, elements.size());
                    output = RespResponseUtility.getRespArray(elements.subList(l, Math.min(r + 1, elements.size())));
                } else {
                    output = RespResponseUtility.getErrorMessage("Value is not list");
                }
            } else {
                output = RespResponseUtility.getRespArray(Collections.emptyList());
            }
        }

        return output;
    }

    private String lpush(String[] words) {
        String key = words[4];
        List<String> elements = new ArrayList<>();
        for (int i = 6; i < words.length - 1; i = i + 2) {
            elements.add(words[i]);
        }
        Collections.reverse(elements);
        List<String> values = new ArrayList<>();
        synchronized (this) {
            if (map.containsKey(key)) {
                if (map.get(key).getValueType() == ValueType.LIST) {
                    values = (List<String>) map.get(key).getValue();
                } else {
                    return RespResponseUtility.getErrorMessage("Value is not a list");
                }
            }
            values.addAll(0, elements);

            map.put(key, new Value<>(ValueType.LIST, values, -1L));
            this.notifyAll();
        }

        return RespResponseUtility.getRespInteger(values.size());
    }

    private String llen(String[] words) {
        String key = words[4];
        int res = 0;
        synchronized (this) {
            if (map.containsKey(key)) {
                if (map.get(key).getValueType() == ValueType.LIST) {
                    List<String> values = (List<String>) map.get(key).getValue();
                    res = values.size();
                } else {
                    return RespResponseUtility.getErrorMessage("Value is not a list");
                }
            }
        }

        return RespResponseUtility.getRespInteger(res);
    }

    private String lpop(String[] words) {
        String key = words[4];
        List<String> res = new ArrayList<>();
        int cnt = 1;
        if (words.length > 6) cnt = Integer.parseInt(words[6]);
        synchronized (this) {
            if (map.containsKey(key)) {
                if (map.get(key).getValueType() == ValueType.LIST) {
                    List<String> values = (List<String>) map.get(key).getValue();
                    int originalSize = values.size();

                    for (int i = 0; i < cnt && i < originalSize; ++i) {
                        res.add(values.get(0));
                        values.remove(0);
                    }

                    map.put(key, new Value<>(ValueType.LIST, values, -1L));
                } else {
                    return RespResponseUtility.getErrorMessage("Value is not a list");
                }
            }
        }

        return cnt == 1 ? RespResponseUtility.getBulkString(res.get(0)) : RespResponseUtility.getRespArray(res);
    }

    private String blpop(String[] words) throws InterruptedException {
        String key = words[4];
        double timeout = Double.parseDouble(words[6]);
        timeout *= 1000;
        String res = null;

        synchronized (this) {
            long beforeTimeInMillis = System.currentTimeMillis();
            long remainingTime = (long) timeout;
            List<String> values;
            if (timeout > 0) {
                while (!map.containsKey(key) && remainingTime > 0) {
                    this.wait((long) timeout);
                    remainingTime = Math.min(0, beforeTimeInMillis + (long) timeout - System.currentTimeMillis());
                }
            } else {
                while (!map.containsKey(key)) {
                    this.wait();
                }
            }

            if (timeout > 0) {
                if (remainingTime > 0) {
                    values = (List<String>) map.get(key).getValue();
                    while (values.size() == 0) {
                        this.wait(remainingTime);
                        remainingTime = Math.min(0, beforeTimeInMillis + (long) timeout - System.currentTimeMillis());
                    }
                }
            } else {
                values = (List<String>) map.get(key).getValue();
                while (values.size() == 0) {
                    this.wait();
                }
            }

            if (map.containsKey(key)) {
                if (map.get(key).getValueType() == ValueType.LIST) {
                    values = (List<String>) map.get(key).getValue();
                    if (values.size() > 0) {
                        res = values.get(0);
                        values.remove(0);
                        map.put(key, new Value<>(ValueType.LIST, values, -1L));
                    } else {
                        return RespResponseUtility.getRespArray(Collections.emptyList());
                    }
                } else {
                    return RespResponseUtility.getErrorMessage("Value is not a list");
                }
            } else {
                return RespResponseUtility.getNullArray();
            }
        }

        return RespResponseUtility.getRespArray(Arrays.asList(key, res));
    }

    private String type(String[] words) {
        String key = words[4];
        String res = "none";
        synchronized (this) {
            if (map.containsKey(key)) {
                res = map.get(key).getValueType().toString().toLowerCase();
            }
        }
        return RespResponseUtility.getSimpleString(res);
    }

    private String xadd(String[] words) {
        String key = words[4];
        String id = words[6];
        String output = "";
        HashMap<String, String> input = new HashMap<>();
        for (int i = 8; i < words.length - 1; i = i + 4) input.put(words[i], words[i + 2]);

        Stream stream;

        synchronized (this) {
            if (map.containsKey(key)) {
                stream = (Stream) map.get(key).getValue();
            } else {
                stream = new Stream();
            }
            output = stream.add(id, input);
            map.put(key, new Value<>(ValueType.STREAM, stream, -1L));
            this.notifyAll();
        }

        return output;
    }

    private String xrange(String[] words) {
        String key = words[4];
        String lowerLimit = "";
        String upperLimit = "";

        if (words[6].equals("-")) lowerLimit = "0-0";
        else lowerLimit = words[6];
        if (words[8].equals("+")) upperLimit = Long.MAX_VALUE + "-" + Long.MAX_VALUE;
        else upperLimit = words[8];
        String output = "";

        synchronized (this) {
            if (map.containsKey(key)) {
                Stream stream = (Stream) map.get(key).getValue();
                output = stream.xrange(lowerLimit, upperLimit);
            } else {
                output = RespResponseUtility.getRespArray(Collections.emptyList());
            }
        }

        return output;
    }

    private String xread(String[] words) throws InterruptedException {
        boolean block = words[4].equals("block");
        long timeout = block ? Long.parseLong(words[6]) : -1L;
        List<String> keyRange = new ArrayList<>();

        for (int j = block ? 10 : 6; j < words.length - 1; j += 2) {
            keyRange.add(words[j]);
        }

        List<String> keys = new ArrayList<>(keyRange.subList(0, keyRange.size()/2));
        List<String> range = new ArrayList<>(keyRange.subList(keyRange.size()/2, keyRange.size()));
        String output = "";

        synchronized (this) {
            if (timeout > 0) {
                long beforeTimeInMillis = System.currentTimeMillis();
                long remainingTime = timeout;
                output = xreadUtility(keys, range);
                while(remainingTime > 0) {
                    int count = 0;
                    int lastIndex = 0;

                    while ((lastIndex = output.indexOf("*", lastIndex)) != -1) {
                        count++;
                        lastIndex += "*".length(); // Move past the found substring to find the next one
                    }
                    if (count > 3) break;

                    this.wait(remainingTime);
                    remainingTime = Math.max(0, beforeTimeInMillis + remainingTime - System.currentTimeMillis());
                    output = xreadUtility(keys, range);
                }
            } else if (timeout == 0){
                while(true) {
                    output = xreadUtility(keys, range);
                    int count = 0;
                    int lastIndex = 0;

                    while ((lastIndex = output.indexOf("*", lastIndex)) != -1) {
                        count++;
                        lastIndex += "*".length(); // Move past the found substring to find the next one
                    }
                    if (count > 3) break;
                    this.wait();
                }
            } else {
                output = xreadUtility(keys, range);
            }
        }

        int count = 0;
        int lastIndex = 0;

        while ((lastIndex = output.indexOf("*", lastIndex)) != -1) {
            count++;
            lastIndex += "*".length(); // Move past the found substring to find the next one
        }

        if (count > 3) {
            output = "*" + keys.size() + "\r\n" + output;
        } else {
            output = RespResponseUtility.getNullArray();
        }

        return output;
    }

    public String xreadUtility(List<String> keys, List<String> range) {
        String output = "";

        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i);
            String lowerLimit = range.get(i);

            if (map.containsKey(key)) {
                Stream stream = (Stream) map.get(key).getValue();
                String curr = stream.xread(lowerLimit);
                output += "*2\r\n" + RespResponseUtility.getBulkString(key) + curr;
            } else {
                String curr = RespResponseUtility.getRespArray(Collections.emptyList());
                output += "*2\r\n" + RespResponseUtility.getBulkString(key) + curr;
            }
        }

        return output;
    }
}

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class CommandHandler {
    private static final String RDB =
            "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    private final RedisServer server;
    private final HashMap<String, Value<?>> map;

    public CommandHandler(RedisServer server, HashMap<String, Value<?>> map) {
        this.server = server;
        this.map = map;
    }

    public String processInput(String input, Client client) throws InterruptedException, IOException {
        String result = "";
        String[] args = input.split("\r\n");
        if (args.length > 2) {
            String command = args[2].toLowerCase();
            switch(command) {
                case "multi": {
                    if (client.isInTransaction()) {
                        result = RespResponseUtility.getErrorMessage("ERR nested transactions not allowed");
                    } else {
                        client.beginTransaction();
                        result = RespResponseUtility.getSimpleString("OK");
                    }
                    break;
                }
                case "discard": {
                    if (client.isInTransaction()) {
                        client.endTransaction();
                        result = RespResponseUtility.getSimpleString("OK");
                    } else {
                        result = RespResponseUtility.getErrorMessage("ERR DISCARD without MULTI");
                    }
                    break;
                }
                case "exec": {
                    if (client.isInTransaction()) {
                        result = processEnqueuedCommands(client);
                    } else {
                        result = RespResponseUtility.getErrorMessage("ERR EXEC without MULTI");
                    }
                    break;
                }
                default: {
                    if (client.isInTransaction()) {
                        client.enqueueCommand(args);
                        result = RespResponseUtility.getSimpleString("QUEUED");
                    } else {
                        result =  parseCommand(args, true, client);
                    }
                }
            }
        }
        return result;
    }

    public String processEnqueuedCommands(Client client) throws InterruptedException, IOException {
        List<String> outputs = new ArrayList<>();
        List<String> operations = new ArrayList<>();

        synchronized (server) {
            for (String[] enqueuedCommand : client.getEnqueuedCommands()) {
                String[] args = enqueuedCommand;
                operations.add(enqueuedCommand[2].toLowerCase());
                String output = parseCommand(args, false, client);
                outputs.add(output);
            }
        }
        client.endTransaction();

        return RespResponseUtility.getRespArray(operations, outputs);
    }

    public String parseCommand(String[] args, boolean format, Client client) throws InterruptedException, IOException {
        String command = "";
        String output = "";
        if (args.length > 2) {
            command = args[2].toLowerCase();

            output = switch (command) {
                case "ping" -> ping();
                case "echo" -> echo(args);
                case "set" -> set(args);
                case "get" -> get(args);
                case "rpush" -> rpush(args);
                case "lrange" -> lrange(args);
                case "lpush" -> lpush(args);
                case "llen" -> llen(args);
                case "lpop" -> lpop(args);
                case "blpop" -> blpop(args);
                case "type" -> type(args);
                case "xadd" -> xadd(args);
                case "xrange" -> xrange(args);
                case "xread" -> xread(args);
                case "incr" -> incr(args);
                case "info" -> info(args);
                case "replconf" -> replconf(args, client);
                case "psync" -> psync(args, client);
                default -> "";
            };
        }
        if (format) output = RespResponseUtility.getRespOutput(command, output);
        return output;
    }

    private static String ping() {
        return "PONG";
    }

    private static String echo(String[] words) {
        return words[4];
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
        synchronized (server) {
            map.put(key, value);
        }
        return "OK";
    }

    private String get(String[] words) {
        String output = null;
        String key = words[4];
        synchronized (server) {
            if (map.containsKey(key)) {
                String value = (String) map.get(key).getValue();
                Long expiry = map.get(key).getExpiry();
                if (expiry == -1 || expiry > System.currentTimeMillis()) {
                    output = value;
                } else {
                    map.remove(key);
                }
            }
        }
        return output;
    }

    private String rpush(String[] words) {
        String key = words[4];
        List<String> elements = new ArrayList<>();
        for (int i = 6; i < words.length; i = i + 2) {
            elements.add(words[i]);
        }
        List<String> values = new ArrayList<>();
        synchronized (server) {
            if (map.containsKey(key)) {
                if (map.get(key).getValueType() == ValueType.LIST) {
                    values = (List<String>) map.get(key).getValue();
                } else {
                    return RespResponseUtility.getErrorMessage("ERR Value is not a list");
                }
            }
            values.addAll(elements);

            map.put(key, new Value<>(ValueType.LIST, values, -1L));
            server.notifyAll();
        }
        return String.valueOf(values.size());
    }

    private String lrange(String[] words) {
        String key = words[4];
        int l = Integer.parseInt(words[6]);
        int r = Integer.parseInt(words[8]);
        String output = "";
        synchronized (server) {
            if (map.containsKey(key)) {
                if (map.get(key).getValueType() == ValueType.LIST) {
                    List<String> elements = (List<String>) map.get(key).getValue();
                    l = RespResponseUtility.normalizeIndex(l, elements.size());
                    r = RespResponseUtility.normalizeIndex(r, elements.size());
                    output = RespResponseUtility.getRespArray(elements.subList(l, Math.min(r + 1, elements.size())));
                } else {
                    output = RespResponseUtility.getErrorMessage("ERR Value is not list");
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
        for (int i = 6; i < words.length; i = i + 2) {
            elements.add(words[i]);
        }
        Collections.reverse(elements);
        List<String> values = new ArrayList<>();
        synchronized (server) {
            if (map.containsKey(key)) {
                if (map.get(key).getValueType() == ValueType.LIST) {
                    values = (List<String>) map.get(key).getValue();
                } else {
                    return RespResponseUtility.getErrorMessage("ERR Value is not a list");
                }
            }
            values.addAll(0, elements);

            map.put(key, new Value<>(ValueType.LIST, values, -1L));
            server.notifyAll();
        }

        return String.valueOf(values.size());
    }

    private String llen(String[] words) {
        String key = words[4];
        int res = 0;
        synchronized (server) {
            if (map.containsKey(key)) {
                if (map.get(key).getValueType() == ValueType.LIST) {
                    List<String> values = (List<String>) map.get(key).getValue();
                    res = values.size();
                } else {
                    return RespResponseUtility.getErrorMessage("ERR Value is not a list");
                }
            }
        }

        return String.valueOf(res);
    }

    private String lpop(String[] words) {
        String key = words[4];
        List<String> res = new ArrayList<>();
        int cnt = 1;
        if (words.length > 6) cnt = Integer.parseInt(words[6]);
        synchronized (server) {
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
                    return RespResponseUtility.getErrorMessage("ERR Value is not a list");
                }
            }
        }

        return cnt == 1 ? res.get(0) : RespResponseUtility.getRespArray(res);
    }

    private String blpop(String[] words) throws InterruptedException {
        String key = words[4];
        double timeout = Double.parseDouble(words[6]);
        timeout *= 1000;
        String res = null;

        synchronized (server) {
            long beforeTimeInMillis = System.currentTimeMillis();
            long remainingTime = (long) timeout;
            List<String> values;
            if (timeout > 0) {
                while (!map.containsKey(key) && remainingTime > 0) {
                    server.wait((long) timeout);
                    remainingTime = Math.min(0, beforeTimeInMillis + (long) timeout - System.currentTimeMillis());
                }
            } else {
                while (!map.containsKey(key)) {
                    server.wait();
                }
            }

            if (timeout > 0) {
                if (remainingTime > 0) {
                    values = (List<String>) map.get(key).getValue();
                    while (values.size() == 0) {
                        server.wait(remainingTime);
                        remainingTime = Math.min(0, beforeTimeInMillis + (long) timeout - System.currentTimeMillis());
                    }
                }
            } else {
                values = (List<String>) map.get(key).getValue();
                while (values.size() == 0) {
                    server.wait();
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
                    return RespResponseUtility.getErrorMessage("ERR Value is not a list");
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
        synchronized (server) {
            if (map.containsKey(key)) {
                res = map.get(key).getValueType().toString().toLowerCase();
            }
        }
        return res;
    }

    private String xadd(String[] words) {
        String key = words[4];
        String id = words[6];
        String output = "";
        HashMap<String, String> input = new HashMap<>();
        for (int i = 8; i < words.length; i = i + 4) input.put(words[i], words[i + 2]);

        Stream stream;

        synchronized (server) {
            if (map.containsKey(key)) {
                stream = (Stream) map.get(key).getValue();
            } else {
                stream = new Stream();
            }
            output = stream.add(id, input);
            map.put(key, new Value<>(ValueType.STREAM, stream, -1L));
            server.notifyAll();
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

        synchronized (server) {
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


        for (int j = 0; j < words.length; ++j){
        }
        for (int j = block ? 10 : 6; j < words.length; j += 2) {
            if (keyRange.equals("$")) {

            } else {
                keyRange.add(words[j]);
            }
        }

        List<String> keys = new ArrayList<>(keyRange.subList(0, keyRange.size()/2));
        List<String> range = new ArrayList<>(keyRange.subList(keyRange.size()/2, keyRange.size()));
        List<String> updatedRange = new ArrayList<>();

        String output = "";
        synchronized (server) {
            for (int i = 0; i < keys.size(); ++i) {
                if (range.get(i).equals("$")) {
                    if (map.containsKey(keys.get(i))) {
                        Stream stream = (Stream) map.get(keys.get(i)).getValue();
                        updatedRange.add(stream.getLastId());
                    } else {
                        updatedRange.add("0-0");
                    }
                } else {
                    updatedRange.add(range.get(i));
                }
            }

            range = updatedRange;

            if (timeout > 0) {
                long beforeTimeInMillis = System.currentTimeMillis();
                long remainingTime = timeout;
                output = xreadUtility(keys, range);
                while(remainingTime > 0) {
                    if (subStringCount(output, "*") > 3) break;

                    server.wait(remainingTime);
                    remainingTime = Math.max(0, beforeTimeInMillis + remainingTime - System.currentTimeMillis());
                    output = xreadUtility(keys, range);
                }
            } else if (timeout == 0){
                while(true) {
                    output = xreadUtility(keys, range);
                    if (subStringCount(output, "*") > 3) break;
                    server.wait();
                }
            } else {
                output = xreadUtility(keys, range);
            }
        }

        if (subStringCount(output, "*") > 3) {
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

    public static int subStringCount(String str, String substr) {
        int count = 0;
        int lastIndex = 0;

        while ((lastIndex = str.indexOf(substr, lastIndex)) != -1) {
            count++;
            lastIndex += substr.length(); // Move past the found substring to find the next one
        }

        return count;
    }

    private String incr(String[] words) {
        String key = words[4];
        String output = "";

        synchronized (server) {
            if (map.containsKey(key)) {
                String value = (String) map.get(key).getValue();
                try {
                    int v = Integer.parseInt(value);
                    map.put(key, new Value<>(ValueType.STRING, String.valueOf(v + 1), -1L));
                    output = String.valueOf(v + 1);
                } catch (Exception e) {
                    output = RespResponseUtility.getErrorMessage("ERR value is not an integer or out of range");
                }
            } else {
                map.put(key, new Value<>(ValueType.STRING, String.valueOf(1), -1L));
                output = String.valueOf(1);
            }
        }

        return output;
    }

    private String info(String[] args) {
        StringBuilder output = new StringBuilder("# Server\r\n");
        output.append("redis_version:7.2.4\r\n");
        output.append("# Client\r\n");
        output.append("connected_clients:" + server.getClientCount() + "\r\n");
        if (server.getReplicaOf() == null) {
            output.append("# Replication\r\nrole:master\r\n");
            output.append("master_replid:" + server.getReplicationId() + "\r\n");
            output.append("master_repl_offset:" + server.getOffset() + "\r\n");
        }
        else output.append("# Replication\r\nrole:slave\r\n");

        return RespResponseUtility.getBulkString(output.toString());
    }

    private String replconf(String[] args, Client client) {
        if (args[4].equals("GETACK")) return RespResponseUtility.getRespArray(Arrays.asList("REPLCONF", "ACK", "0"));

        return "OK";
    }

    private String psync(String[] args, Client client) throws IOException {
        String output = RespResponseUtility.getSimpleString("FULLRESYNC " + server.getReplicationId() + " 0");
        byte[] rdbBytes = hexStringToByteArray(RDB);
        output += "$" + rdbBytes.length + "\r\n";
        client.send(output.getBytes());
        client.send(rdbBytes);
        //TODO: Return output from this method instead of sending response to client directly
        server.registerReplica(client);

        return null;
    }

    public static byte[] hexStringToByteArray(String hexString) {
        // The length of the hex string should be even.
        int len = hexString.length();
        if (len % 2 != 0) {
            throw new IllegalArgumentException("Hex string must have an even number of characters.");
        }

        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                    + Character.digit(hexString.charAt(i + 1), 16));
        }
        return data;
    }
}

import java.util.List;

public final class RespResponseUtility {

    private RespResponseUtility() {}
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

    public static String getRespInteger(String i) {
        return ":" + i + "\r\n";
    }

    public static String getErrorMessage(String error) {
        return "-" + error + "\r\n";
    }

    public static String getRespArray(List<String> outputs) {
        StringBuilder res = new StringBuilder("*" + outputs.size());
        for (String e : outputs) res.append("\r\n$").append(e.length()).append("\r\n").append(e);
        res.append("\r\n");

        return res.toString();
    }

    // Used to create output for multi-exec
    public static String getRespArray(List<String> commands, List<String> outputs) {
        StringBuilder res = new StringBuilder("*" + outputs.size()).append("\r\n");

        for (int i = 0; i < commands.size(); ++i) {
            res.append(getRespOutput(commands.get(i), outputs.get(i)));
        }

        return res.toString();
    }

    public static String getNullArray() {
        return "*-1\r\n";
    }

    public static String getRespOutput(String command, String output) {
        if (output != null && output.startsWith("-ERR")) return output;

        switch (command) {
            case "ping", "set", "type" -> {
                output = getSimpleString(output);
            }
            case "echo", "bulk", "get", "xadd" -> {
                output = getBulkString(output);
            }
            case "rpush", "lpush", "llen", "incr" -> {
                output = getRespInteger(output);
            }
            case "lpop" -> {
                if (!output.startsWith("*")) output = getBulkString(output);
            }
            default -> {
            }
        }


        return output;
    }
}

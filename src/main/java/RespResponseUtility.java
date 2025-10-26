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
}

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Stream {
    private final static String ERR = "ERR The ID specified in XADD is equal or smaller than the target stream top " +
            "item";
    private final static String ERR_0 = "ERR The ID specified in XADD must be greater than 0-0";
    private LinkedHashMap<String, HashMap<String, String>> linkedHashMap = new LinkedHashMap<>();

    public Stream() {}

    public Stream(String id, HashMap<String, String> input) {
        linkedHashMap.put(id, input);
    }

    public String add(String id, HashMap<String, String> input) {
        int milliSecondsTime = getMilliSecondsTime(id);
        int sequenceNumber = getSequenceNumber(id);

        if (milliSecondsTime == 0 && sequenceNumber == 0) {
            return RespResponseUtility.getErrorMessage(ERR_0);
        }
        String[] lKeys = linkedHashMap.keySet().toArray(new String[0]);
        if (lKeys.length > 0) {
            String lastKey = lKeys[lKeys.length - 1];
            int lastMilliSecondsTime = getMilliSecondsTime(lastKey);
            int lastSequenceNumber = getSequenceNumber(lastKey);

            if (milliSecondsTime < lastMilliSecondsTime || (milliSecondsTime == lastMilliSecondsTime && sequenceNumber <= lastSequenceNumber)) {
                return RespResponseUtility.getErrorMessage(ERR);
            }
        }

        HashMap<String, String> existingMap = new HashMap<>();
        if (linkedHashMap.containsKey(id)) existingMap = linkedHashMap.get(id);
        existingMap.putAll(input);
        linkedHashMap.put(id, existingMap);
        return RespResponseUtility.getBulkString(id);
    }

    public int getMilliSecondsTime(String id) {
        String[] strings = id.split("-");

        return Integer.parseInt(strings[0]);
    }

    public int getSequenceNumber(String id) {
        String[] strings = id.split("-");

        return Integer.parseInt(strings[1]);
    }
}

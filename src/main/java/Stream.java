import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Stream {
    private final static String ERR = "ERR The ID specified in XADD is equal or smaller than the target stream top " +
            "item";
    private final static String ERR_0 = "ERR The ID specified in XADD must be greater than 0-0";
    private LinkedHashMap<String, HashMap<String, String>> linkedHashMap;

    public Stream() {
        linkedHashMap = new LinkedHashMap<>();
    }

    public String add(String id, HashMap<String, String> input) {
        id = getNextId(id);
        if (!validateZeroId(id)) return RespResponseUtility.getErrorMessage(ERR_0);
        if (!validateId(id)) return RespResponseUtility.getErrorMessage(ERR);

        HashMap<String, String> existingMap = new HashMap<>();
        if (linkedHashMap.containsKey(id)) existingMap = linkedHashMap.get(id);
        existingMap.putAll(input);
        linkedHashMap.put(id, existingMap);
        return RespResponseUtility.getBulkString(id);
    }

    public static String getMilliSecondsTime(String id) {
        if (id.equals("*")) return id;
        String[] strings = id.split("-");

        return strings[0];
    }

    public static String getSequenceNumber(String id) {
        if (id.equals("*")) return id;
        String[] strings = id.split("-");

        return strings[1];
    }

    private static boolean validateZeroId(String id) {
        long milliSecondsTime = Long.parseLong(getMilliSecondsTime(id));
        long sequenceNumber = Long.parseLong(getSequenceNumber(id));

        if (milliSecondsTime == 0 && sequenceNumber == 0) {
            return false;
        }
        return true;
    }

    private boolean validateId(String id) {
        long milliSecondsTime = Long.parseLong(getMilliSecondsTime(id));
        long sequenceNumber = Long.parseLong(getSequenceNumber(id));

        String[] lKeys = linkedHashMap.keySet().toArray(new String[0]);
        if (lKeys.length > 0) {
            String lastKey = lKeys[lKeys.length - 1];
            long lastMilliSecondsTime = Long.parseLong(getMilliSecondsTime(lastKey));
            long lastSequenceNumber = Long.parseLong(getSequenceNumber(lastKey));

            if (milliSecondsTime < lastMilliSecondsTime || (milliSecondsTime == lastMilliSecondsTime && sequenceNumber <= lastSequenceNumber)) {
                return false;
            }
        }
        return true;
    }

    private String getNextId(String id) {
        String milliSecondsTime = getMilliSecondsTime(id);
        String sequenceNumber = getSequenceNumber(id);

        String[] lKeys = linkedHashMap.keySet().toArray(new String[0]);

        if (lKeys.length > 0) {
            String lastKey = lKeys[lKeys.length - 1];
            String lastMilliSecondsTime = getMilliSecondsTime(lastKey);
            String lastSequenceNumber = getSequenceNumber(lastKey);

            if (sequenceNumber.equals("*") && milliSecondsTime.equals("*")) {
                long currentTimeInMillis = System.currentTimeMillis();

                if (currentTimeInMillis == Long.parseLong(lastMilliSecondsTime)) {
                    if (Long.parseLong(lastSequenceNumber) == Long.MAX_VALUE) {
                        lastMilliSecondsTime = String.valueOf(Long.parseLong(lastMilliSecondsTime) + 1);
                        lastSequenceNumber = lastMilliSecondsTime.equals("0") ? "1" : "0";

                    } else {
                        lastSequenceNumber = String.valueOf(Long.parseLong(lastSequenceNumber) + 1);
                    }
                } else {
                    lastMilliSecondsTime = String.valueOf(currentTimeInMillis);
                    lastSequenceNumber = "0";
                }

                milliSecondsTime = lastMilliSecondsTime;
                sequenceNumber = lastSequenceNumber;
            } else if (!milliSecondsTime.equals("*") && sequenceNumber.equals("*")) {
                if (milliSecondsTime.equals(String.valueOf(lastMilliSecondsTime))) {
                    sequenceNumber = String.valueOf(Long.parseLong(lastSequenceNumber) + 1);
                } else {
                    sequenceNumber = milliSecondsTime.equals("0") ? "1" : "0";
                }
            }
        } else {
            if (sequenceNumber.equals("*") && milliSecondsTime.equals("*")) {
                milliSecondsTime = String.valueOf(System.currentTimeMillis());
                sequenceNumber = "0";
            } else if (!milliSecondsTime.equals("*") && sequenceNumber.equals("*")) {
                sequenceNumber = milliSecondsTime.equals("0") ? "1" : "0";
            }
        }

        return milliSecondsTime + "-" + sequenceNumber;
    }

    // Returns true if id2 >= id1 else false
    private static boolean compare(String id1, String id2) {
        String milliSecondsTime1 = getMilliSecondsTime(id1);
        String sequenceNumber1 = getSequenceNumber(id1);
        String milliSecondsTime2 = getMilliSecondsTime(id2);
        String sequenceNumber2 = getSequenceNumber(id2);

        if (Long.parseLong(milliSecondsTime2) > Long.parseLong(milliSecondsTime1)) return true;
        if (milliSecondsTime2.equals(milliSecondsTime1)) {
            if (Long.parseLong(sequenceNumber2) >= Long.parseLong(sequenceNumber1)) return true;
        }

        return false;
    }

    public String xrange(String lowerLimit, String upperLimit) {
        if (!lowerLimit.contains("-")) lowerLimit = lowerLimit + "-0";
        if (!upperLimit.contains("-")) upperLimit = upperLimit + "-" + Long.MAX_VALUE;

        StringBuilder res = new StringBuilder();
        int count = 0;

        for (Map.Entry<String, HashMap<String, String>> entry : linkedHashMap.entrySet()) {
            if (compare(lowerLimit, entry.getKey()) && compare(entry.getKey(), upperLimit)) {
                ++count;
                List<String> keyValue = new ArrayList<>();
                for (Map.Entry<String, String> e : entry.getValue().entrySet()) {
                    keyValue.add(e.getKey());
                    keyValue.add(e.getValue());
                }
                res.append("*2\r\n");
                res.append(RespResponseUtility.getBulkString(entry.getKey()));
                res.append(RespResponseUtility.getRespArray(keyValue));
            }
        }
        res.insert(0, "*" + count + "\r\n");

        return res.toString();
    }
}

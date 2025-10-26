import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Stream {
    private LinkedHashMap<String, HashMap<String, String>> linkedHashMap = new LinkedHashMap<>();

    public Stream() {}

    public Stream(String id, HashMap<String, String> input) {
        linkedHashMap.put(id, input);
    }

    public String add(String id, HashMap<String, String> input) {
        HashMap<String, String> existingMap = new HashMap<>();
        if (linkedHashMap.containsKey(id)) existingMap = linkedHashMap.get(id);
        existingMap.putAll(input);
        linkedHashMap.put(id, existingMap);
        return id;
    }
}

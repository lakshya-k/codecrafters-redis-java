import java.util.HashMap;
import java.util.UUID;

public class Main {
    public static void main(String[] args) {
        HashMap<String, String> arguments = new HashMap<>();
        for (int i = 0; i < args.length - 1; ++i) {
            arguments.put(args[i], args[i + 1]);
        }
        UUID id = UUID.randomUUID();
        RedisServer server = new RedisServer(id);

        if (arguments.containsKey("--port")) server.setPort(Integer.parseInt(arguments.get("--port")));
        if (arguments.containsKey("--replicaof")) server.setReplicaOf(arguments.get("--replicaof"));

        server.start();
    }
}

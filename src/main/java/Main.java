import java.util.HashMap;
import java.util.UUID;

public class Main {
    public static void main(String[] args) {
        HashMap<String, String> arguments = new HashMap<>();
        for (int i = 0; i < args.length - 1; ++i) {
            arguments.put(args[i], args[i + 1]);
        }
        UUID id = UUID.randomUUID();
        String dir = ".";
        String dbfilename = "/tmp/redis-data";

        if (arguments.containsKey("--dir")) dir = arguments.get("--dir");
        if (arguments.containsKey("--dbfilename")) dbfilename = arguments.get("--dbfilename");

        RedisConfig config = new RedisConfig(dir, dbfilename);
        RedisServer server = new RedisServer(id, config);

        if (arguments.containsKey("--port")) server.setPort(Integer.parseInt(arguments.get("--port")));
        if (arguments.containsKey("--replicaof")) server.setReplicaOf(arguments.get("--replicaof"));

        server.start();
    }
}

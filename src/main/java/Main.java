import java.util.UUID;

public class Main {
    public static void main(String[] args) {
        RedisServer server;
        UUID uuid = UUID.randomUUID();

        if (args.length > 1) {
            int port = Integer.parseInt(args[1]);
            server = new RedisServer(uuid, port);
        } else {
            server = new RedisServer(uuid);
        }

        server.start();
    }
}

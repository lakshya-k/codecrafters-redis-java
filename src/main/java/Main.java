public class Main {
    public static void main(String[] args) {
        RedisServer server;
        if (args.length > 1) {
            int port = Integer.parseInt(args[1]);
            server = new RedisServer(port);
        } else {
            server = new RedisServer();
        }

        server.start();
    }
}

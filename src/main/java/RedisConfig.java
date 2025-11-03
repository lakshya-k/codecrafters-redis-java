public class RedisConfig {
    private String dir;
    private String dbFilename;
    public String getDir() {
        return this.dir;
    }

    public String getDbFilename() {
        return this.dbFilename;
    }

    public RedisConfig(String dir, String dbFilename) {
        this.dir = dir;
        this.dbFilename = dbFilename;
    }
}

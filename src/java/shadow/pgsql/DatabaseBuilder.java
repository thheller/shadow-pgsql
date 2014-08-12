package shadow.pgsql;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
* Created by zilence on 12.08.14.
*/
public class DatabaseBuilder {
    private String host;
    private int port;

    Map<String, String> connectParams = new HashMap<>();

    private AuthHandler authHandler = null;

    public DatabaseBuilder(String host, int port) {
        this.host = host;
        this.port = port;

        this.setConnectParam("application_name", "shadow.pgsql");

        // FIXME: undecided if I want to allow changes or not
        this.setConnectParam("client_encoding", "UTF-8");
    }

    public void setConnectParam(String key, String value) {
        this.connectParams.put(key, value);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public DatabaseBuilder setUser(String user) {
        this.setConnectParam("user", user);
        return this;
    }

    public DatabaseBuilder setDatabase(String database) {
        this.setConnectParam("database", database);
        return this;
    }

    public AuthHandler getAuthHandler() {
        return authHandler;
    }

    public DatabaseBuilder setAuthHandler(AuthHandler authHandler) {
        this.authHandler = authHandler;
        return this;
    }

    public Database build() throws IOException {
        Database db = new Database(host, port, Collections.unmodifiableMap(connectParams), authHandler);
        db.prepare();
        return db;
    }
}

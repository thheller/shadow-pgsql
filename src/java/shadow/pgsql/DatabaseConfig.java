package shadow.pgsql;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zilence on 24.08.14.
 */
public class DatabaseConfig {
    final String host;
    final int port;
    final Map<String, String> connectParams = new HashMap<>();

    AuthHandler authHandler = null;

    boolean ssl = false;
    SSLContext sslContext = null;
    boolean fetchSchema = true;

    public DatabaseConfig(String host, int port) {
        this.host = host;
        this.port = port;
        this.setConnectParam("application_name", "shadow.pgsql");
        // FIXME: undecided if I want to allow changes or not
        this.setConnectParam("client_encoding", "UTF-8");
    }

    public DatabaseConfig setConnectParam(String key, String value) {
        this.connectParams.put(key, value);
        return this;
    }

    public DatabaseConfig setUser(String name) {
        return setConnectParam("user", name);
    }

    public DatabaseConfig setDatabase(String db) {
        return setConnectParam("database", db);
    }

    public String getConnectParam(String key) {
        return connectParams.get(key);
    }

    public DatabaseConfig setAuthHandler(AuthHandler authHandler) {
        this.authHandler = authHandler;
        return this;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    public AuthHandler getAuthHandler() {
        return authHandler;
    }

    public DatabaseConfig noSchema() {
        this.fetchSchema = false;
        return this;
    }

    public DatabaseConfig useSSL() throws Exception {
        return useSSL(SSLContext.getDefault());
    }

    public DatabaseConfig useSSL(SSLContext context) {
        this.ssl = true;
        this.sslContext = context;
        return this;
    }

    /**
     * this should be considered immutable after get() is called!
     *
     * @return database instance with this config
     */
    public Database get() throws IOException {
        Database db = new Database(this);

        if (fetchSchema) {
            db.fetchSchemaInfo();
        }

        return db;
    }
}

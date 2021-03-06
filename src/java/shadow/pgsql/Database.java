package shadow.pgsql;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import shadow.pgsql.utils.RowProcessor;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes a remote Postgresql Database backend which you can
 * connect to.
 * <p/>
 * Can and should be shared between threads once constructed, use DatabaseBuilder or setup shortcut
 *
 * @author Thomas Heller
 */
public class Database {
    final Map<Integer, String> oid2name = new HashMap<>();
    private final Map<String, Integer> name2oid = new HashMap<>();

    private final Map<ColumnByTableIndex, String> columnNames = new HashMap<>();

    private final DatabaseConfig config;

    final MetricRegistry metricRegistry;
    final Timer connectTimer;
    final Counter preparedCounter;

    final Timer unnamedPrepareTimer;
    final Timer unnamedExecuteTimer;

    final MetricCollector metricCollector;

    public Database(DatabaseConfig config) {
        if (config.getHost() == null) {
            throw new IllegalArgumentException("host cannot be null");
        }
        if (config.getConnectParam("user") == null) {
            throw new IllegalArgumentException("user cannot be null");
        }
        this.config = config;
        this.metricRegistry = config.getMetricRegistry();
        this.metricCollector = config.metricCollector;

        this.connectTimer = metricRegistry.timer(MetricRegistry.name("shadow-pgsql", "connect"));
        this.preparedCounter = metricRegistry.counter(MetricRegistry.name("shadow-pgsql", "prepared"));
        this.unnamedPrepareTimer = metricRegistry.timer(MetricRegistry.name("shadow-pgsql", "query", "unnamed", "prepare"));
        this.unnamedExecuteTimer = metricRegistry.timer(MetricRegistry.name("shadow-pgsql", "query", "unnamed", "execute"));
    }

    public static Database setup(String host, int port, String user, String databaseName) throws IOException {
        DatabaseConfig db = new DatabaseConfig(host, port);

        db.setUser(user);
        db.setDatabase(databaseName);

        return db.get();
    }

    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    public Connection connect() throws IOException {
        Connection pg = null;
        IO io = null;

        Timer.Context timerContext = connectTimer.time();

        if (config.ssl) {
            SocketChannel channel = SocketChannel.open(new InetSocketAddress(config.host, config.port));
            channel.configureBlocking(true);

            // http://www.postgresql.org/docs/9.0/static/protocol-flow.html#AEN84692

            ByteBuffer buf = ByteBuffer.allocate(8);

            buf.putInt(8);
            buf.putInt(80877103);
            buf.flip();
            channel.write(buf);
            buf.clear();

            buf.limit(1);

            if (channel.read(buf) != 1) {
                throw new EOFException();
            }

            buf.flip();

            if (buf.get() != 'S') {
                throw new IllegalStateException("ssl not accepted");
            }

            io = SSLSocketIO.start(channel, config.sslContext, config.host, config.port);
        } else {
            SocketChannel channel = SocketChannel.open(new InetSocketAddress(config.host, config.port));
            channel.configureBlocking(true);
            io = new SocketIO(channel);

            //io = new StreamIO(new Socket(config.host, config.port));
        }

        pg = new Connection(this, io);
        pg.startup(config.connectParams, config.authHandler);

        timerContext.stop();

        return pg;
    }

    void fetchSchemaInfo() throws IOException {
        // fetch schema related things
        try (Connection con = connect()) {

            SQL pg_types = SQL.query("SELECT oid, typname FROM pg_type")
                    .withName("schema.types")
                    .buildRowsWith(Helpers.ROW_AS_LIST)
                    .buildResultsWith(
                            new RowProcessor<List>() {
                                @Override
                                public void process(List row) {
                                    int oid = (int) row.get(0);
                                    String name = (String) row.get(1);
                                    oid2name.put(oid, name);
                                    name2oid.put(name, oid);
                                }
                            })
                    .create();

            int results = (int) con.queryWith(pg_types);

            if (results == 0) {
                throw new IllegalStateException("no types?");
            }

            SQL schema = SQL.query("SELECT" +
                    "  a.attname," + // column name
                    "  a.attnum," + // column index
                    "  b.oid," + // table oid
                    "  b.relname" + // table name
                    " FROM pg_class b" +
                    " JOIN pg_attribute a" +
                    " ON a.attrelid = b.oid" +
                    " WHERE b.relkind = 'r'" +
                    " AND a.attnum > 0" +
                    " AND b.relname NOT LIKE 'pg_%'" +
                    " AND b.relname NOT LIKE 'sql_%'")
                    .withName("schema.names")
                    .buildRowsWith(Helpers.ROW_AS_LIST)
                    .buildResultsWith(
                            new RowProcessor<List>() {
                                @Override
                                public void process(List row) {
                                    String colName = (String) row.get(0);
                                    short colIndex = (short) row.get(1);
                                    int tableOid = (int) row.get(2);
                                    String tableName = (String) row.get(3);

                                    columnNames.put(new ColumnByTableIndex(tableName, colIndex), colName);

                                    if (!oid2name.containsKey(tableOid)) {
                                        oid2name.put(tableOid, tableName);
                                    }
                                }
                            })
                    .create();

            con.queryWith(schema);
        }
    }

    public String getNameForColumn(String tableName, int positionInTable) {
        return this.columnNames.get(new ColumnByTableIndex(tableName, positionInTable));
    }

    public String getNameForOid(int oid) {
        return this.oid2name.get(oid);
    }

    public int getOidForName(String typeName) {
        Integer i = this.name2oid.get(typeName);
        if (i == null) {
            throw new IllegalArgumentException(String.format("unknown type: %s", typeName));
        }

        return i;
    }

    private static class ColumnByTableIndex {
        final String table;
        final int index;

        private ColumnByTableIndex(String table, int index) {
            this.table = table;
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ColumnByTableIndex columnByTableIndex = (ColumnByTableIndex) o;

            if (index != columnByTableIndex.index) return false;
            if (!table.equals(columnByTableIndex.table)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = table.hashCode();
            result = 31 * result + index;
            return result;
        }

        @Override
        public String toString() {
            return "ColumnKey{" +
                    "table='" + table + '\'' +
                    ", index=" + index +
                    '}';
        }
    }
}

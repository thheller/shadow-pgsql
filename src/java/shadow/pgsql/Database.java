package shadow.pgsql;

import shadow.pgsql.types.*;
import shadow.pgsql.utils.RowProcessor;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes a remote Postgresql Database backend which you can
 * connect to.
 *
 * Can and should be shared between threads once constructed, use DatabaseBuilder or setup shortcut
 *
 * @author Thomas Heller
 */
public class Database {
    private final Map<Integer, String> oid2name = new HashMap<>();

    private final Map<ColumnByTableIndex, String> columnNames = new HashMap<>();

    private final String host;
    private final int port;

    private final Map<String, String> connectParams;
    private final AuthHandler authHandler;

    Database(String host, int port, Map<String, String> connectParams, AuthHandler authHandler) {
        this.host = host;
        this.port = port;
        this.connectParams = connectParams;
        this.authHandler = authHandler;
    }

    public static Database setup(String host, int port, String user, String databaseName) throws IOException {
        DatabaseBuilder db = new DatabaseBuilder(host, port);

        db.setConnectParam("user", user);
        db.setConnectParam("database", databaseName);

        return db.build();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Map<String, String> getConnectParams() {
        return connectParams;
    }

    void prepare() throws IOException {
        try (Connection con = connect()) {
            fetchSchemaInfo(con);
        }
    }

    public Connection connect() throws IOException {
        Connection pg = new Connection(this, new Socket(host, port));
        pg.startup(connectParams, authHandler);
        return pg;
    }

    void fetchSchemaInfo(Connection con) throws IOException {
        // fetch schema related things

        SimpleQuery pg_types = new SimpleQuery("SELECT oid, typname FROM pg_type");
        pg_types.setRowBuilder(Handlers.ROW_AS_LIST);
        pg_types.setResultBuilder(
                new RowProcessor<List>() {
                    @Override
                    public void process(List row) {
                        int oid = (int) row.get(0);
                        String name = (String) row.get(1);
                        oid2name.put(oid, name);
                    }
                });

        int results = (int) con.executeQuery(pg_types);

        if (results == 0) {
            throw new IllegalStateException("no types?");
        }

        SimpleQuery schema = new SimpleQuery("SELECT" +
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
                        " AND b.relname NOT LIKE 'sql_%'"
        );

        schema.setRowBuilder(Handlers.ROW_AS_LIST);
        schema.setResultBuilder(
                new RowProcessor<List>() {
                    @Override
                    public void process(List row) {
                        String colName = (String) row.get(0);
                        int colIndex = (int) row.get(1);
                        int tableOid = (int) row.get(2);
                        String tableName = (String) row.get(3);

                        if (!oid2name.containsKey(tableOid)) {
                            oid2name.put(tableOid, tableName);
                        }

                        columnNames.put(new ColumnByTableIndex(tableName, colIndex), colName);
                    }
                });

        con.executeQuery(schema);
    }

    public String getNameForColumn(String tableName, int positionInTable) {
        return this.columnNames.get(new ColumnByTableIndex(tableName, positionInTable));
    }

    public String getNameForOid(int oid) {
        return this.oid2name.get(oid);
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

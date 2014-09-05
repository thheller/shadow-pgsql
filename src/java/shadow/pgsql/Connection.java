package shadow.pgsql;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Primary Inteface to talk to the backend, usually obtained via Database
 * <p/>
 * NOT THREAD-SAFE! Should be closed after use.
 *
 * @author Thomas Heller
 */
public class Connection implements AutoCloseable {
    private int queryId = 0;
    private int savepointId = 0;

    int openStatements = 0;

    final Database db;
    private final IO io;

    public ProtocolOutput output;
    public ProtocolInput input;

    final Map<String, String> parameters = new HashMap<>();

    ConnectionState state;
    TransactionStatus txState;

    Connection(Database db, IO io) throws IOException {
        this.db = db;
        this.io = io;

        this.input = new ProtocolInput(this, io);
        this.output = new ProtocolOutput(io);

        this.state = ConnectionState.CONNECTED;
    }


    public String getParameterValue(String key) {
        return this.parameters.get(key);
    }

    void handleNotice(Map<String, String> notice) {
        // FIXME: delegate to database or some interface
        System.out.format("NOTICE: %s", notice.toString());
    }

    void handleNotify(int processId, String channel, String payload) {
        // FIXME: delegate to database or some interface
        System.out.format("NOTIFY: %d %s -> %s", processId, channel, payload);
    }

    void checkReady() {
        if (state != ConnectionState.READY) {
            throw new IllegalStateException(String.format("Connection not READY (%s)", state));
        }
    }

    void startup(Map<String, String> opts, AuthHandler authHandler) throws IOException {
        output.checkReset();
        output.begin();
        output.int32(196608); // 3.0

        for (String k : opts.keySet()) {
            output.string(k);
            output.string(opts.get(k));
        }
        output.string(); // empty string (aka null byte) means end

        output.complete();
        output.flushAndReset();

        this.state = ConnectionState.START_UP;

        Map<String, String> errorData = null;

        // read until ReadyForQuery
        STARTUP_LOOP:
        while (true) {
            final char type = input.readNextCommand();
            switch (type) {
                case 'R': // AuthenticationOk
                {
                    final int code = input.getInt();
                    if (code != 0 && authHandler == null) {
                        throw new IllegalStateException("authentication requires AuthHandler");
                    }

                    final int dataLen = input.getCurrentSize() - 8;
                    if (dataLen > 0) {
                        this.state = ConnectionState.AUTHENTICATING;
                        authHandler.doAuth(this, code, input.current);
                    }

                    this.state = ConnectionState.START_UP;
                    break;
                }
                case 'S': // ParameterStatus
                {
                    final String parameterName = input.readString();
                    final String parameterValue = input.readString();
                    this.parameters.put(parameterName, parameterValue);

                    break;
                }
                case 'K': // BackendKeyData
                {
                    final int processId = input.getInt();
                    final int secretKey = input.getInt();

                    // FIXME: store this somewhere, needed for query cancel
                    break;
                }
                case 'E': {
                    errorData = input.readMessages();
                    break;
                }
                case 'Z': {
                    input.readReadyForQuery();
                    break STARTUP_LOOP;
                }
                default:
                    throw new IllegalStateException(String.format("illegal protocol message during startup: %s\n", (char) type));
            }
        }

        if (errorData != null) {
            throw new CommandException("Error during startup", errorData);
        }

        // startup complete, ready for query
    }

    public boolean isInTransaction() {
        return this.txState == TransactionStatus.TRANSACTION;
    }

    // FIXME: make public? should only be used for simple commands
    // simple only supports string encoding for types
    StatementResult simpleStatement(String query) throws IOException {
        checkReady();

        output.checkReset();
        output.beginCommand('Q');
        output.string(query);
        output.complete();

        output.flushAndReset();

        return input.readStatementResult();
    }

    // FIXME: transaction mode
    // http://www.postgresql.org/docs/9.3/static/sql-begin.html
    public void begin() throws IOException {
        checkReady();

        switch (txState) {
            case IDLE:
                this.simpleStatement("BEGIN");
                break;
            case TRANSACTION:
                break;
            default:
                throw new IllegalStateException(String.format("can't start transaction while in %s state", txState));
        }
    }

    public Savepoint savepoint() throws IOException {
        final String name = String.format("P%d", savepointId++);
        simpleStatement(String.format("SAVEPOINT %s", name));
        return new Savepoint(this, name);
    }

    public void commit() throws IOException {
        checkReady();

        if (txState != TransactionStatus.TRANSACTION) {
            throw new IllegalStateException(String.format("not in a transaction, in %s", txState));
        }

        this.simpleStatement("COMMIT");
        this.savepointId = 0;
    }

    public void rollback() throws IOException {
        this.simpleStatement("ROLLBACK");
    }

    public Object queryWith(String query, Object... params) throws IOException {
        return query(new SimpleQuery(query), Arrays.asList(params));
    }

    public Object queryWith(Query query, Object... params) throws IOException {
        return query(query, Arrays.asList(params));
    }

    public Object query(Query query, List<Object> params) throws IOException {
        try (PreparedQuery stmt = prepareQuery(query)) {
            return stmt.execute(params);
        }
    }

    public StatementResult executeWith(String statement, Object... params) throws IOException {
        return execute(new SimpleStatement(statement), Arrays.asList(params));
    }

    public StatementResult execute(Statement statement, List params) throws IOException {
        try (PreparedStatement stmt = prepare(statement)) {
            return stmt.execute(params);
        }
    }

    public PreparedStatement prepare(String statement) throws IOException {
        return prepare(new SimpleStatement(statement));
    }

    public PreparedStatement prepare(Statement statement) throws IOException {
        final List<TypeHandler> typeHints = statement.getParameterTypes();

        final String statementId = String.format("s%d", queryId++);

        writeParseDescribeSync(statement.getSQLString(), typeHints, statementId);

        int[] paramInfo = null;
        boolean parsed = false;
        boolean rowDescription = false;

        // success flow usually is 1/t/n/Z

        Map<String, String> errorData = null;

        PREPARE_LOOP:
        while (true) {
            final char type = input.readNextCommand();

            switch (type) {
                case '1': // ParseComplete
                {
                    input.checkSize("ParseComplete", 0);
                    parsed = true;
                    break;
                }
                case 't': // ParameterDescription
                {
                    paramInfo = input.readParameterDescription();
                    break;
                }
                case 'T': // RowDescription
                {
                    input.readRowDescription();
                    rowDescription = true;
                    break;
                }
                case 'n': // NoData
                {
                    break;
                }
                case 'Z': // ReadyForQuery
                {
                    input.readReadyForQuery();
                    break PREPARE_LOOP;
                }
                case 'E': // Error
                {
                    errorData = input.readMessages();
                    break;
                }
                default:
                    throw new IllegalStateException(String.format("protocol violation, received '%s' after Parse", type));
            }
        }

        openStatements += 1;

        if (errorData != null) {
            if (parsed) {
                throw new IllegalStateException("Error but Parsed!");
            }
            throw new CommandException(String.format("Failed to prepare Statement\nSQL: %s", statement.getSQLString()), errorData);
        }

        try {
            // FIXME: check if we got NoData?
            if (!parsed || paramInfo == null) {
                throw new IllegalStateException("backend did not send ParseComplete, ParameterDescription");
            }

            if (rowDescription) {
                throw new CommandException(String.format("%s returns rows, use a Query", statement.getSQLString()));
            }

            final TypeHandler[] encoders = getParamTypes(paramInfo, typeHints, statement.getTypeRegistry());

            return new PreparedStatement(this, statementId, encoders, statement);
        } catch (Exception e) {
            // FIXME: this might also throw!
            closeStatement(statementId);
            throw e;
        }
    }

    private TypeHandler[] getParamTypes(int[] paramInfo, List<TypeHandler> typeHints, TypeRegistry typeRegistry) {
        final TypeHandler[] encoders = new TypeHandler[paramInfo.length];
        for (int i = 0; i < encoders.length; i++) {
            TypeHandler encoder = null;

            if (typeHints.size() > i) {
                encoder = typeHints.get(i);
            }

            if (encoder == null) {
                encoder = typeRegistry.getTypeHandlerForOid(db, paramInfo[i]);
            }

            encoders[i] = encoder;
        }

        return encoders;
    }

    public PreparedQuery prepareQuery(Query query) throws IOException {
        final List<TypeHandler> typeHints = query.getParameterTypes();

        final String statementId = String.format("s%d", queryId++);
        writeParseDescribeSync(query.getSQLString(), typeHints, statementId);

        int[] paramInfo = null;
        ColumnInfo[] columnInfos = null;
        boolean parsed = false;

        // success flow usually is 1/t/T/Z
        Map<String, String> errorData = null;
        boolean noData = false;

        PREPARE_LOOP:
        while (true) {
            final char type = input.readNextCommand();

            switch (type) {
                case '1': // ParseComplete
                {
                    input.checkSize("ParseComplete", 0);
                    parsed = true;
                    break;
                }
                case 't': // ParameterDescription
                {
                    paramInfo = input.readParameterDescription();
                    break;
                }
                case 'T': // RowDescription
                {
                    columnInfos = input.readRowDescription();
                    break;
                }
                case 'Z': // ReadyForQuery
                {
                    input.readReadyForQuery();
                    break PREPARE_LOOP;
                }
                case 'E': // Error
                {
                    errorData = input.readMessages();
                    break;
                }
                case 'n': // NoData
                {
                    noData = true;
                    break;
                }
                default:
                    throw new IllegalStateException(String.format("protocol violation, received '%s' after Parse", type));
            }
        }

        if (errorData != null) {
            // FIXME: I don't assume this will happen.
            if (parsed) {
                throw new IllegalStateException("Error but Parsed!");
            }

            throw new CommandException(String.format("Failed to prepare Query\nSQL: %s", query.getSQLString()), errorData);
        }

        try {
            if (noData) {
                throw new CommandException("Query does not return data, use a Statement");
            }

            if (!parsed || paramInfo == null || columnInfos == null) {
                throw new IllegalStateException("backend did not send ParseComplete, ParameterDescription and RowDescription");
            }

            final TypeHandler[] encoders = getParamTypes(paramInfo, typeHints, query.getTypeRegistry());

            RowBuilder rowBuilder = query.createRowBuilder(columnInfos);
            ResultBuilder resultBuilder = query.createResultBuilder(columnInfos);

            TypeHandler[] decoders = new TypeHandler[columnInfos.length];

            for (int i = 0; i < columnInfos.length; i++) {
                ColumnInfo f = columnInfos[i];
                decoders[i] = query.getTypeRegistry().getTypeHandlerForField(db, f);
            }

            return new PreparedQuery(this, statementId, encoders, query, columnInfos, decoders, resultBuilder, rowBuilder);
        } catch (Exception e) {
            try {
                closeStatement(statementId);
            } catch (Exception e2) {
                // FIXME: what is correct here?
            }
            throw e;
        }
    }

    private void writeParseDescribeSync(String query, List<TypeHandler> typeHints, String statementId) throws IOException {
        checkReady();
        output.checkReset();

        try {
            // Parse
            output.beginCommand('P');
            output.string(statementId);
            output.string(query);
            output.int16((short) typeHints.size());
            for (TypeHandler t : typeHints) {
                if (t == null) {
                    output.int32(0);
                } else {
                    int oid = t.getTypeOid();
                    if (oid == -1) {
                        oid = db.getOidForName(t.getTypeName());
                    }
                    output.int32(oid);
                }
            }
            output.complete();

            // Describe - want ParameterDescription + RowDescription
            output.beginCommand('D');
            output.int8((byte) 'S');
            output.string(statementId);
            output.complete();

            // Sync
            output.simpleCommand('S');
            output.flushAndReset();

            this.state = ConnectionState.QUERY_OPEN;
        } catch (Exception e) {
            output.reset();
            throw e;
        }
    }

    public void close() throws IOException {
        output.checkReset();
        output.simpleCommand('X');
        output.flushAndReset();

        this.state = ConnectionState.CLOSED;

        io.close();
    }

    void closeStatement(String statementId) throws IOException {
        output.checkReset();

        // Close
        output.beginCommand('C');
        output.int8((byte) 'S');
        output.string(statementId);
        output.complete();

        // Sync
        output.simpleCommand('S');
        output.flushAndReset();

        state = ConnectionState.QUERY_CLOSE;

        Map<String, String> errorData = null;
        boolean closed = false;

        // CloseComplete + Ready
        CLOSE_LOOP:
        while (true) {
            final char type = input.readNextCommand();
            switch (type) {
                case '3': // CloseComplete
                {
                    input.checkSize("CloseComplete", 0);
                    closed = true;
                    break;
                }
                case 'E': {
                    errorData = input.readMessages();
                    break;
                }
                case 'Z': // ReadyForQuery
                {
                    input.readReadyForQuery();
                    break CLOSE_LOOP;
                }
                default: {
                    throw new IllegalStateException(String.format("protocol violation while closing query, did not expect '%s'", type));
                }
            }
        }

        openStatements -= 1;

        if (errorData != null) {
            throw new CommandException("Failed to close Statement", errorData);
        }

        if (!closed) {
            throw new IllegalStateException("Close didn't Close!");
        }
    }

    public boolean isReady() {
        return state == ConnectionState.READY && txState == TransactionStatus.IDLE && openStatements == 0;
    }
}

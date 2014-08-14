package shadow.pgsql;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Primary Inteface to talk to the backend, usually obtained via Database
 *
 * NOT THREAD-SAFE! Should be closed after use.
 *
 * @author Thomas Heller
 */
public class Connection implements AutoCloseable {
    private int queryId = 0;
    private int savepointId = 0;

    private final Database db;
    private final Socket socket;

    public ProtocolOutput output;
    public ProtocolInput input;

    final Map<String, String> parameters = new HashMap<>();

    ConnectionState state;
    TransactionStatus txState;

    Connection(Database db, Socket socket) throws IOException {
        this.db = db;
        this.socket = socket;

        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        this.input = new ProtocolInput(this, in);
        this.output = new ProtocolOutput(out);

        this.state = ConnectionState.CONNECTED;
    }

    public void handleNotice(Map<String, String> notice) {
        // FIXME: delegate to database or some interface
        System.out.format("NOTICE: %s", notice.toString());
    }

    public void handleNotify(int processId, String channel, String payload) {
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
                    final int size = input.readInt32();
                    final int code = input.readInt32();
                    if (code != 0 && authHandler == null) {
                        throw new IllegalStateException("authentication requires AuthHandler");
                    }

                    final int dataLen = size - 8;
                    if (dataLen > 0) {
                        byte[] data = new byte[dataLen];
                        input.read(data);

                        this.state = ConnectionState.AUTHENTICATING;
                        authHandler.doAuth(this, code, data);
                    }

                    this.state = ConnectionState.START_UP;
                    break;
                }
                case 'S': // ParameterStatus
                {
                    final int size = input.readInt32();
                    final String parameterName = input.readString();
                    final String parameterValue = input.readString();
                    this.parameters.put(parameterName, parameterValue);

                    break;
                }
                case 'K': // BackendKeyData
                {
                    final int size = input.readInt32();
                    final int processId = input.readInt32();
                    final int secretKey = input.readInt32();

                    // FIXME: store this somewhere, needed for query cancel
                    break;
                }
                case 'E': {
                    errorData = input.readErrorData();
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

        switch(txState) {
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

    public Object executeQuery(String query, Object... params) throws IOException {
        return executeQuery(new SimpleQuery(query), Arrays.asList(params));
    }

    public Object executeQuery(Query query, Object... params) throws IOException {
        return executeQuery(query, Arrays.asList(params));
    }

    public Object executeQuery(Query query, List<Object> params) throws IOException {
        try (PreparedQuery stmt = prepareQuery(query)) {
            return stmt.execute(params);
        }
    }

    public StatementResult execute(String statement, Object... params) throws IOException {
        try (PreparedStatement stmt = prepare(new SimpleStatement(statement))) {
            return stmt.execute(params);
        }
    }

    public PreparedStatement prepare(String query) throws IOException {
        return prepare(new SimpleStatement(query));
    }

    public PreparedStatement prepare(Statement query) throws IOException {
        final List<TypeHandler> typeHints = query.getParameterTypes();

        final String statementId = String.format("s%d", queryId++);

        writeParseDescribeSync(query.getSQLString(), typeHints, statementId);

        int[] paramInfo = null;
        boolean parsed = false;

        // success flow usually is 1/t/n/Z

        Map<String, String> errorData = null;

        PREPARE_LOOP:
        while (true) {
            final char type = input.readNextCommand();

            switch (type) {
                case '1': // ParseComplete
                {
                    final int size = input.readInt32();
                    if (size != 4) {
                        throw new IllegalStateException(String.format("ParseComplete was not size 4 (was %d)", size));
                    }
                    parsed = true;
                    break;
                }
                case 't': // ParameterDescription
                {
                    paramInfo = input.readParameterDescription();
                    break;
                }
                case 'n': // NoData
                {
                    final int size = input.readInt32();
                    break;
                }
                case 'Z': // ReadyForQuery
                {
                    input.readReadyForQuery();
                    break PREPARE_LOOP;
                }
                case 'E': // Error
                {
                    errorData = input.readErrorData();
                    break;
                }
                default:
                    throw new IllegalStateException(String.format("protocol violation, received '%s' after Parse", type));
            }
        }

        if (errorData != null) {
            if (parsed) {
                throw new IllegalStateException("Error but Parsed!");
            }
            throw new CommandException(String.format("Failed to prepare Statement: %s", query.getSQLString()), errorData);
        }

        try {
            // FIXME: check if we got NoData?
            if (!parsed || paramInfo == null) {
                throw new IllegalStateException("backend did not send ParseComplete, ParameterDescription");
            }

            final TypeHandler[] encoders = new TypeHandler[paramInfo.length];
            for (int i = 0; i < encoders.length; i++) {
                if (typeHints.size() > i) {
                    encoders[i] = typeHints.get(i);
                } else {
                    encoders[i] = query.getTypeRegistry().getTypeHandlerForOid(db, paramInfo[i]);
                }
            }

            return new PreparedStatement(this, statementId, encoders, query);
        } catch (Exception e) {
            // FIXME: this might also throw!
            closeQuery(statementId);
            throw e;
        }
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
                    final int size = input.readInt32();
                    if (size != 4) {
                        throw new IllegalStateException(String.format("ParseComplete was not size 4 (was %d)", size));
                    }
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
                    errorData = input.readErrorData();
                    break;
                }
                case 'n': // NoData
                {
                    final int size = input.readInt32();
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

            throw new CommandException(String.format("Failed to prepare Query: %s", query.getSQLString()), errorData);
        }

        try {
            if (noData) {
                throw new CommandException("Query does not return data, use a Statement");
            }

            if (!parsed || paramInfo == null || columnInfos == null) {
                throw new IllegalStateException("backend did not send ParseComplete, ParameterDescription and RowDescription");
            }

            final TypeHandler[] encoders = new TypeHandler[paramInfo.length];
            for (int i = 0; i < encoders.length; i++) {
                if (typeHints.size() > i) {
                    encoders[i] = typeHints.get(i);
                } else {
                    encoders[i] = query.getTypeRegistry().getTypeHandlerForOid(db, paramInfo[i]);
                }
            }

            RowBuilder rowBuilder = query.createRowBuilder(columnInfos);
            ResultBuilder resultBuilder = query.createResultBuilder(columnInfos);

            TypeHandler[] decoders = new TypeHandler[columnInfos.length];

            for (int i = 0; i < columnInfos.length; i++) {
                ColumnInfo f = columnInfos[i];
                decoders[i] = query.getTypeRegistry().getTypeHandlerForField(db, f);
            }

            return new PreparedQuery(this, statementId, encoders, query, columnInfos, decoders, resultBuilder, rowBuilder);
        } catch (Exception e) {
            // FIXME: this might also throw
            closeQuery(statementId);
            throw e;
        }
    }

    private void writeParseDescribeSync(String query, List<TypeHandler> typeHints, String statementId) throws IOException {
        checkReady();

        output.checkReset();
        this.state = ConnectionState.QUERY_OPEN;

        // Parse
        output.beginCommand('P');
        output.string(statementId);
        output.string(query);
        output.int16((short) typeHints.size());
        for (TypeHandler t : typeHints) {
            output.int32(t.getTypeOid());
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
    }

    public void close() throws IOException {
        checkReady();

        output.checkReset();
        output.simpleCommand('X');
        output.flushAndReset();

        this.state = ConnectionState.CLOSED;

        output.close();
        input.close();
        socket.close();

    }

    public void closeQuery(String statementId) throws IOException {
        output.checkReset();
        state = ConnectionState.QUERY_CLOSE;

        // Close
        output.beginCommand('C');
        output.int8((byte) 'S');
        output.string(statementId);
        output.complete();

        // Sync
        output.simpleCommand('S');
        output.flushAndReset();

        Map<String, String> errorData = null;
        boolean closed = false;

        // CloseComplete + Ready
        CLOSE_LOOP:
        while (true) {
            final char type = input.readNextCommand();
            switch (type) {
                case '3': // CloseComplete
                {
                    final int size = input.readInt32();
                    if (size != 4) {
                        throw new IllegalStateException(String.format("CloseComplete should be size 4 (was %d)", size));
                    }
                    closed = true;
                    break;
                }
                case 'E':
                {
                    errorData = input.readErrorData();
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

        if (errorData != null) {
            throw new CommandException("Failed to close Statement", errorData);
        }

        if (!closed) {
            throw new IllegalStateException("Close didn't Close!");
        }
    }
}

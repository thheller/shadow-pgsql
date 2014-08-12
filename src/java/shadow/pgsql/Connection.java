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

                    break;
                }
                case 'E': {
                    throw input.readErrorAndMakeException("STARTUP");
                }
                case 'Z': {
                    input.readReadyForQuery();
                    break STARTUP_LOOP;
                }
                default:
                    throw new IllegalStateException(String.format("illegal protocol message during startup: %s\n", (char) type));
            }
        }

        // startup complete, ready for query
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

    public int execute(String statement, Object... params) throws IOException {
        try (PreparedStatement stmt = prepare(new SimpleStatement(statement))) {
            return stmt.execute(params);
        }
    }

    public PreparedStatement prepare(Statement query) throws IOException {
        final List<TypeHandler> typeHints = query.getParameterTypes();

        final String statementId = String.format("s%d", queryId++);

        writeParseDescribeSync(query, typeHints, statementId);

        int[] paramInfo = null;
        boolean parsed = false;

        // success flow usually is 1/t/n/Z

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
                    throw input.readErrorAndMakeException(query.getStatement());
                }
                default:
                    throw new IllegalStateException(String.format("protocol violation, received '%s' after Parse", type));
            }
        }

        // FIXME: check if we got NoData?
        if (!parsed || paramInfo == null) {
            throw new IllegalStateException("backend did not send ParseComplete, ParameterDescription");
        }

        final TypeHandler[] encoders = new TypeHandler[paramInfo.length];
        for (int i = 0; i < encoders.length; i++) {
            if (typeHints.size() > i) {
                encoders[i] = typeHints.get(i);
            } else {
                encoders[i] = db.getTypeHandlerForOid(paramInfo[i]);
            }
        }

        return new PreparedStatement(this, statementId, encoders, query);
    }

    public PreparedQuery prepareQuery(Query query) throws IOException {
        final List<TypeHandler> typeHints = query.getParameterTypes();

        final String statementId = String.format("s%d", queryId++);
        writeParseDescribeSync(query, typeHints, statementId);

        int[] paramInfo = null;
        ColumnInfo[] columnInfos = null;
        boolean parsed = false;

        // success flow usually is 1/t/T/Z

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
                    throw input.readErrorAndMakeException(query.getStatement());
                }
                case 'n': // NoData
                {
                    // FIXME: cleanup!
                    throw new IllegalStateException("Query does not return result, did you mean prepare?");
                }
                default:
                    throw new IllegalStateException(String.format("protocol violation, received '%s' after Parse", type));
            }
        }

        if (!parsed || paramInfo == null || columnInfos == null) {
            throw new IllegalStateException("backend did not send ParseComplete, ParameterDescription and RowDescription");
        }

        final TypeHandler[] encoders = new TypeHandler[paramInfo.length];
        for (int i = 0; i < encoders.length; i++) {
            if (typeHints.size() > i) {
                encoders[i] = typeHints.get(i);
            } else {
                encoders[i] = db.getTypeHandlerForOid(paramInfo[i]);
            }
        }

        RowBuilder rowBuilder = query.createRowBuilder(columnInfos);
        ResultBuilder resultBuilder = query.createResultBuilder(columnInfos);

        TypeHandler[] decoders = new TypeHandler[columnInfos.length];

        for (int i = 0; i < columnInfos.length; i++) {
            ColumnInfo f = columnInfos[i];
            decoders[i] = db.getTypeHandlerForField(f);
        }

        return new PreparedQuery(this, statementId, encoders, query, columnInfos, decoders, resultBuilder, rowBuilder);
    }

    private void writeParseDescribeSync(Statement query, List<TypeHandler> typeHints, String statementId) throws IOException {
        checkReady();

        output.checkReset();
        this.state = ConnectionState.QUERY_OPEN;

        // Parse
        output.beginCommand('P');
        output.string(statementId);
        output.string(query.getStatement());
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
}

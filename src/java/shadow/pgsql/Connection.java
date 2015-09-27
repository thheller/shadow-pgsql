package shadow.pgsql;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.io.IOException;
import java.util.*;

/**
 * Primary Inteface to talk to the backend, usually obtained via Database
 * <p/>
 * NOT THREAD-SAFE! Should be closed after use.
 *
 * @author Thomas Heller
 */
public class Connection implements AutoCloseable {
    final static List EMPTY_LIST = Collections.unmodifiableList(new ArrayList());

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
        this.output = new ProtocolOutput(this, io);

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
        output.writeStartup(opts);
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
        output.writeSimpleQuery(query);
        output.flushAndReset();

        return input.readStatementResult(query);
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

    public Object queryWith(SQL sql, Object... params) throws IOException {
        return query(sql, Arrays.asList(params));
    }

    public Object query(SQL sql) throws IOException {
        return query(sql, EMPTY_LIST);
    }

    /**
     * execute a query once
     *
     * uses the optimized path of P/B/D/E/S
     *
     * sacrifices some verification for performance
     * since we don't get a ParameterDescription 't' we cannot know the type of parameters
     * so they must be provided
     *
     * saves one network roundtrip since the prepare flow is
     * P/D/S -> network -> B/E/S -> network (repeat-able)
     *
     * @param sql
     * @param params
     * @return result constructed via sql row/result builders
     * @throws IOException
     */
    public Object query(SQL sql, List<Object> params) throws IOException {
        final List<TypeHandler> paramEncoders = sql.getParameterTypes();
        if (paramEncoders.size() != sql.getParamCount()) {
            throw new IllegalArgumentException(String.format("SQL expects %d parameters, must specify their types. Only got %d types", sql.getParamCount(), paramEncoders.size()));
        }

        checkReady();
        output.checkReset();

        output.writeParse(sql.getSQLString(), paramEncoders, null);
        output.writeBind(paramEncoders.toArray(new TypeHandler[paramEncoders.size()]), params, sql, null, null, new short[]{1}); // all binary
        output.writeDescribePortal(null);
        output.writeExecute(null, 0);
        output.writeSync();

        output.flushAndReset();

        ColumnInfo[] columnInfos = null; // created by 'T'
        TypeHandler[] columnDecoders = null;

        boolean parsed = false; // set by '1'
        boolean noData = false; // set by 'n'
        boolean complete = false; // set by 'C'

        // success flow usually is 1/2/(n|T)/D*/C/Z

        Map<String, String> errorData = null;

        ResultBuilder resultBuilder = null;
        RowBuilder rowBuilder = null;

        Object queryResult = null;

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
                case '2': // BindComplete
                {
                    input.checkSize("BindComplete", 0);
                    break;
                }
                case 'T': // RowDescription
                {
                    columnInfos = input.readRowDescription();

                    columnDecoders = new TypeHandler[columnInfos.length];
                    for (int i = 0; i < columnInfos.length; i++) {
                        ColumnInfo f = columnInfos[i];
                        columnDecoders[i] = sql.getTypeRegistry().getTypeHandlerForField(db, f);
                    }

                    resultBuilder = sql.getResultBuilder().create(columnInfos);
                    rowBuilder = sql.getRowBuilder().create(columnInfos);

                    queryResult = resultBuilder.init();
                    break;
                }
                case 'n': // NoData
                {
                    noData = true;
                    break;
                }
                case 'D': // DataRow
                {
                    queryResult = resultBuilder.add(queryResult, input.readRow(columnDecoders, columnInfos, rowBuilder));
                    break;
                }
                case 'C': { // CommandComplete
                    final String tag = input.readString();
                    complete = true;

                    // FIXME: losing information (tag)
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

        if (errorData != null) {
            if (parsed) {
                throw new IllegalStateException("Error but Parsed!");
            }
            throw new CommandException(String.format("Failed to prepare Statement\nsql: %s", sql.getSQLString()), errorData);
        } else {
            if (!complete) {
                throw new IllegalStateException("not complete");
            }
            if (noData) {
                throw new IllegalStateException("backend did not send data, use statement instead of query when defining your SQL");
            } else {
                return resultBuilder.complete(queryResult);
            }
        }
    }

    public Object executeWith(SQL sql, Object... params) throws IOException {
        return execute(sql, Arrays.asList(params));
    }

    public StatementResult execute(SQL sql, List params) throws IOException {
        try (PreparedSQL stmt = prepare(sql)) {
            return stmt.execute(params);
        }
    }

    public PreparedSQL prepare(SQL sql) throws IOException {
        Timer.Context timerContext = startPrepareTimer(sql.getName());

        final List<TypeHandler> typeHints = sql.getParameterTypes();

        final String statementId = String.format("s%d", queryId++);

        writeParseDescribeSync(sql.getSQLString(), typeHints, statementId);

        int[] paramInfo = null; // created by 't'
        ColumnInfo[] columnInfos = null; // created by 'T'
        boolean parsed = false; // set by '1'
        boolean noData = false; // set by 'n'

        // success flow usually is 1/t/(n|T)/Z

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
                    columnInfos = input.readRowDescription();
                    break;
                }
                case 'n': // NoData
                {
                    noData = true;
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
            throw new CommandException(String.format("Failed to prepare Statement\nsql: %s", sql.getSQLString()), errorData);
        }


        try {
            final TypeHandler[] encoders = getParamTypes(paramInfo, typeHints, sql.getTypeRegistry());

            db.metricCollector.collectPrepareTime(sql.getName(), sql.getSQLString(), timerContext.stop());

            if (noData) {
                if (!parsed || paramInfo == null) {
                    throw new IllegalStateException("backend did not send ParseComplete, ParameterDescription");
                }

                if (sql.expectsData()) {
                    throw new IllegalStateException("backend will not send data, use statement instead of query when defining your SQL");
                }

                return new PreparedSQL(this, statementId, encoders, sql);
            } else {
                if (!parsed || paramInfo == null || columnInfos == null) {
                    throw new IllegalStateException("backend did not send ParseComplete, ParameterDescription and RowDescription");
                }

                if (!sql.expectsData()) {
                    throw new IllegalStateException("backend will send data, use query instead of statement when defining your SQL");
                }

                TypeHandler[] decoders = new TypeHandler[columnInfos.length];

                for (int i = 0; i < columnInfos.length; i++) {
                    ColumnInfo f = columnInfos[i];
                    decoders[i] = sql.getTypeRegistry().getTypeHandlerForField(db, f);
                }

                ResultBuilder resultBuilder = sql.getResultBuilder().create(columnInfos);
                RowBuilder rowBuilder = sql.getRowBuilder().create(columnInfos);

                return new PreparedSQL(this, statementId, encoders, sql, columnInfos, decoders, resultBuilder, rowBuilder);
            }
        } catch (Exception e) {
            // FIXME: this might also throw and e will be lost
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

    private Timer.Context startPrepareTimer(String name) {
        Timer prepareTimer = null;

        if (name != null) {
            prepareTimer = this.db.metricRegistry.timer(MetricRegistry.name("shadow-pgsql", "query", name, "prepare"));
        } else {
            prepareTimer = this.db.unnamedPrepareTimer;
        }

        return prepareTimer.time();
    }


    private void writeParseDescribeSync(String query, List<TypeHandler> typeHints, String statementId) throws IOException {
        checkReady();
        output.checkReset();

        try {
            // Parse
            output.writeParse(query, typeHints, statementId);

            // Describe - want ParameterDescription + RowDescription
            output.writeDescribeStatement(statementId);

            // Sync
            output.writeSync();
            output.flushAndReset();

            this.state = ConnectionState.QUERY_OPEN;
        } catch (Exception e) {
            output.reset();
            throw e;
        }
    }

    public void close() throws IOException {
        output.checkReset();
        output.writeCloseConnection();
        output.flushAndReset();

        this.state = ConnectionState.CLOSED;

        io.close();
    }

    void closeStatement(String statementId) throws IOException {
        output.checkReset();

        // Close
        output.writeCloseStatement(statementId);

        // Sync
        output.writeSync();
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

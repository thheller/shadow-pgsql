package shadow.pgsql;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by zilence on 12.08.14.
 */
public class PreparedSQL implements AutoCloseable {
    private final static TypeHandler[] NO_COLUMNS = new TypeHandler[0];

    protected final Connection pg;
    protected final String statementId;

    protected final SQL sql;
    protected final ColumnInfo[] columnInfos;
    protected final TypeHandler[] typeDecoders;

    private final ResultBuilder resultBuilder;
    private final RowBuilder rowBuilder;

    protected final TypeHandler[] typeEncoders;

    final Timer executeTimer;

    // statement
    PreparedSQL(Connection pg, String statementId, TypeHandler[] typeEncoders, SQL sql) {
        this(pg, statementId, typeEncoders, sql, null, null, null, null);
    }

    // query
    PreparedSQL(Connection pg, String statementId, TypeHandler[] typeEncoders, SQL sql, ColumnInfo[] columnInfos, TypeHandler[] typeDecoders, ResultBuilder resultBuilder, RowBuilder rowBuilder) {
        this.pg = pg;
        this.statementId = statementId;
        this.typeEncoders = typeEncoders;
        this.executeTimer = getExecuteTimer(pg, sql.getName());
        this.sql = sql;

        this.columnInfos = columnInfos;
        this.typeDecoders = typeDecoders;
        this.resultBuilder = resultBuilder;
        this.rowBuilder = rowBuilder;

        pg.db.preparedCounter.inc();
    }

    public SQL getSQL() {
        return sql;
    }

    public StatementResult executeWith(Object... queryParams) throws IOException {
        return execute(Arrays.asList(queryParams));
    }

    public StatementResult execute(List queryParams) throws IOException {
        if (sql.expectsData()) {
            throw new IllegalStateException("SQL expects data, use query");
        }
        Timer.Context timerContext = executeTimer.time();

        // flow -> B/E/S
        executeWithParams(NO_COLUMNS, queryParams);

        // flow <- 2/C/Z
        final StatementResult result = pg.input.readStatementResult(sql.getSQLString());

        pg.db.metricCollector.collectExecuteTime(sql.getName(), sql.getSQLString(), timerContext.stop());

        return result;
    }

    public Object queryWith(Object... params) throws IOException {
        return query(Arrays.asList(params));
    }

    public Object query(final List queryParams) throws IOException {
        if (!sql.expectsData()) {
            throw new IllegalStateException("SQL expects no data, use execute");
        }
        final Timer.Context timerContext = executeTimer.time();

        executeWithParams(typeDecoders, queryParams);

        Object queryResult = resultBuilder.init();

        Map<String, String> errorData = null;

        boolean complete = false;

        // flow <- 2/D*/n?/C/Z
        RESULT_LOOP:
        while (true) {
            final char type = pg.input.readNextCommand();

            switch (type) {
                case '2': // BindComplete
                {
                    pg.input.checkSize("BindComplete", 0);
                    break;
                }
                case 'D':  // DataRow
                {
                    queryResult = resultBuilder.add(queryResult, readRow());
                    break;
                }
                case 'C': { // CommandComplete
                    final String tag = pg.input.readString();
                    complete = true;

                    // FIXME: losing information (tag)
                    break;
                }
                case 'Z': // ReadyForQuery
                {
                    pg.input.readReadyForQuery();
                    break RESULT_LOOP;
                }
                case 'E': {
                    errorData = pg.input.readMessages();
                    break;
                }
                default: {
                    throw new IllegalStateException(String.format("invalid protocol action while reading query results: '%s'", type));
                }
            }
        }

        if (errorData != null) {
            throw new CommandException(String.format("Failed to execute Query\nsql: %s\n", sql.getSQLString()), errorData);
        }

        if (!complete) {
            throw new IllegalStateException("Command did not complete");
        }

        final Object result = resultBuilder.complete(queryResult);

        pg.db.metricCollector.collectExecuteTime(sql.getName(), sql.getSQLString(), timerContext.stop());

        return result;
    }

    private Object readRow() throws IOException {
        final int cols = pg.input.getShort();

        if (cols != columnInfos.length) {
            throw new IllegalStateException(
                    String.format("backend said to expect %d columns, but data had %d", columnInfos.length, cols)
            );
        }

        Object row = rowBuilder.init();

        for (int i = 0; i < columnInfos.length; i++) {
            final ColumnInfo field = columnInfos[i];
            final TypeHandler decoder = typeDecoders[i];
            final int colSize = pg.input.getInt();

            Object columnValue = null;

            if (colSize != -1) {
                columnValue = readColumnValue(field, decoder, colSize);
            }

            row = rowBuilder.add(row, field, i, columnValue);
        }

        return rowBuilder.complete(row);
    }

    private Object readColumnValue(ColumnInfo field, TypeHandler decoder, int colSize) throws IOException {
        try {
            Object columnValue;

            if (decoder.supportsBinary()) {
                int mark = pg.input.current.position();

                columnValue = decoder.decodeBinary(pg, field, pg.input.current, colSize);

                if (pg.input.current.position() != mark + colSize) {
                    throw new IllegalStateException(String.format("Field:[%s ,%s] did not consume all bytes", field.name, decoder));
                }
            } else {
                byte[] bytes = new byte[colSize];
                pg.input.getBytes(bytes);

                // FIXME: assumes UTF-8
                final String stringValue = new String(bytes);
                columnValue = decoder.decodeString(pg, field, stringValue);
            }

            return columnValue;
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format("Failed parsing field \"%s\" of table \"%s\"",
                            field.name,
                            field.tableOid > 0 ? pg.db.oid2name.get(field.tableOid) : "--unknown--"
                    ), e);
        }
    }

    protected void writeBind(TypeHandler[] typeDecoders, List<Object> queryParams, String portalId) {
        // Bind
        pg.output.beginCommand('B');
        pg.output.string(portalId); // portal name (might be null)
        pg.output.string(statementId); // statement name (should not be null)

        // format codes for params
        pg.output.int16((short) typeEncoders.length);
        for (TypeHandler t : typeEncoders) {
            pg.output.int16((short) (t.supportsBinary() ? 1 : 0)); // format code 0 = text, 1 = binary
        }

        pg.output.int16((short) typeEncoders.length);
        for (int i = 0; i < typeEncoders.length; i++) {
            TypeHandler encoder = typeEncoders[i];

            Object param = queryParams.get(i);

            try {
                if (param == null) {
                    pg.output.int32(-1);
                } else if (encoder.supportsBinary()) {
                    pg.output.beginExclusive();
                    encoder.encodeBinary(pg, pg.output, param);
                    pg.output.complete();
                } else {
                    String paramString = encoder.encodeToString(pg, param);

                    // FIXME: assumes UTF-8
                    byte[] bytes = paramString.getBytes();

                    pg.output.int32(bytes.length);
                    if (bytes.length > 0) {
                        pg.output.bytea(bytes);
                    }
                }
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("Failed to encode parameter $%d [%s -> \"%s\"]\nsql: %s", i + 1, encoder.getClass().getName(), param, sql.getSQLString()), e);
            }
        }

        pg.output.int16((short) typeDecoders.length);
        for (TypeHandler t : typeDecoders) {
            pg.output.int16((short) (t.supportsBinary() ? 1 : 0));
        }

        pg.output.complete();
    }

    protected void writeExecute(String portalId, int limit) {
        pg.output.beginCommand('E');
        pg.output.string(portalId); // portal name
        pg.output.int32(limit); // max rows, zero = no limit
        pg.output.complete();
    }

    protected void writeSync() {
        // Sync
        pg.output.simpleCommand('S');
    }

    protected void executeWithParams(TypeHandler[] typeDecoders, List queryParams) throws IOException {
        if (queryParams.size() != typeEncoders.length) {
            throw new IllegalArgumentException(String.format("Incorrect params provided to Statement, expected %d got %d", typeEncoders.length, queryParams.size()));
        }

        pg.checkReady();
        pg.output.checkReset();

        // flow -> B/E/H

        try {
            writeBind(typeDecoders, queryParams, null);
            writeExecute(null, 0);
            writeSync();

            pg.output.flushAndReset();
            pg.state = ConnectionState.QUERY_RESULT;
        } catch (Exception e) {
            // nothing on the wire, no harm done
            pg.output.reset();
            pg.state = ConnectionState.READY;
            throw e;
        }
    }

    public void close() throws IOException {
        pg.closeStatement(statementId);

        pg.db.preparedCounter.dec();
    }

    static Timer getExecuteTimer(Connection pg, String metricsName) {
        // FIXME: don't like that this is constructed every time, Query/Statement maybe a better place to keep these?
        if (metricsName != null) {
            return pg.db.metricRegistry.timer(MetricRegistry.name("shadow-pgsql", "query", metricsName, "execute"));
        } else {
            return pg.db.unnamedExecuteTimer;
        }
    }
}

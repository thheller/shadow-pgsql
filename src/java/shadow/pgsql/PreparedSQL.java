package shadow.pgsql;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.io.IOException;
import java.util.*;

/**
 * Created by zilence on 12.08.14.
 */
public class PreparedSQL implements AutoCloseable {
    private final static TypeHandler[] NO_COLUMNS = new TypeHandler[0];

    protected final Connection pg;
    protected final String statementId;

    protected final SQL sql;
    protected final ColumnInfo[] columnInfos;
    protected final TypeHandler[] columnDecoders;

    private final ResultBuilder resultBuilder;
    private final RowBuilder rowBuilder;

    protected final TypeHandler[] paramEncoders;

    final Timer executeTimer;

    // statement
    PreparedSQL(Connection pg, String statementId, TypeHandler[] paramEncoders, SQL sql) {
        this(pg, statementId, paramEncoders, sql, null, null, null, null);
    }

    // query
    PreparedSQL(Connection pg, String statementId, TypeHandler[] paramEncoders, SQL sql, ColumnInfo[] columnInfos, TypeHandler[] columnDecoders, ResultBuilder resultBuilder, RowBuilder rowBuilder) {
        this.pg = pg;
        this.statementId = statementId;
        this.paramEncoders = paramEncoders;
        this.executeTimer = getExecuteTimer(pg, sql.getName());
        this.sql = sql;

        this.columnInfos = columnInfos;
        this.columnDecoders = columnDecoders;
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

    public Object query() throws IOException {
        return query(Connection.EMPTY_LIST);
    }

    public Object query(final List queryParams) throws IOException {
        if (!sql.expectsData()) {
            throw new IllegalStateException("SQL expects no data, use execute");
        }
        final Timer.Context timerContext = executeTimer.time();

        executeWithParams(columnDecoders, queryParams);

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
                    queryResult = resultBuilder.add(queryResult, pg.input.readRow(columnDecoders, columnInfos, rowBuilder));
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


    protected void executeWithParams(TypeHandler[] typeDecoders, List queryParams) throws IOException {
        if (queryParams.size() != paramEncoders.length) {
            throw new IllegalArgumentException(String.format("Incorrect params provided to Statement, expected %d got %d", paramEncoders.length, queryParams.size()));
        }

        pg.checkReady();
        pg.output.checkReset();

        try {
            // flow -> B/E/H
            pg.output.writeBind(paramEncoders, queryParams, sql, statementId, null, typeDecoders);
            pg.output.writeExecute(null, 0);
            pg.output.writeSync();

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

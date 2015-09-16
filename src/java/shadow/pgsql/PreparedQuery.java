package shadow.pgsql;

import com.codahale.metrics.Timer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by zilence on 12.08.14.
 */
public class PreparedQuery extends PreparedBase {
    protected final Query query;
    protected final ColumnInfo[] columnInfos;
    protected final TypeHandler[] typeDecoders;

    private final ResultBuilder resultBuilder;
    private final RowBuilder rowBuilder;

    public PreparedQuery(Connection pg, String statementId, TypeHandler[] typeEncoders, Query query, ColumnInfo[] columnInfos, TypeHandler[] typeDecoders, ResultBuilder resultBuilder, RowBuilder rowBuilder) {
        super(pg, statementId, typeEncoders, query.getName());
        this.query = query;
        this.columnInfos = columnInfos;
        this.typeDecoders = typeDecoders;
        this.resultBuilder = resultBuilder;
        this.rowBuilder = rowBuilder;
    }

    @Override
    public String getSQLString() {
        return query.getSQLString();
    }

    public Object executeWith(Object... params) throws IOException {
        return execute(Arrays.asList(params));
    }

    public Object execute(final List queryParams) throws IOException {
        final Timer.Context timerContext = executeTimer.time();

        executeWithParams(typeDecoders, queryParams);

        Object queryResult = resultBuilder.init();

        Map<String, String> errorData = null;

        boolean complete = false;

        // flow <- 2/D*/n?/C/Z
        RESULT_LOOP:
        while (true) {
            // it might be better performance to batch READ multiple D frames and batch PARSE instead of READ/PARSE/READ/PARSE/...
            // would use more memory but be more efficient on the CPU cache level since we are not jumping to IO so much
            // probably overthinking it since the limiting factor probably is IO
            // FIXME: investigate
            final char type = pg.input.readNextCommand();

            switch (type) {
                case '2': // BindComplete
                {
                    pg.input.checkSize("BindComplete", 0);
                    break;
                }
                case 'D':  // DataRow
                {
                    final int cols = pg.input.getShort();

                    if (cols != columnInfos.length) {
                        throw new IllegalStateException(
                                String.format("backend said to expect %d columns, but data had %d", columnInfos.length, cols)
                        );
                    }

                    Object row = rowBuilder.init();

                    for (int i = 0; i < columnInfos.length; i++) {
                        ColumnInfo field = columnInfos[i];
                        TypeHandler decoder = typeDecoders[i];

                        final int colSize = pg.input.getInt();

                        Object value = null;

                        try {
                            if (colSize != -1) {
                                if (decoder.supportsBinary()) {
                                    int mark = pg.input.current.position();

                                    value = decoder.decodeBinary(pg, field, pg.input.current, colSize);

                                    if (pg.input.current.position() != mark + colSize) {
                                        throw new IllegalStateException(String.format("Field:[%s ,%s] did not consume all bytes", field.name, decoder));
                                    }
                                } else {
                                    byte[] bytes = new byte[colSize];
                                    pg.input.getBytes(bytes);

                                    // FIXME: assumes UTF-8
                                    final String stringValue = new String(bytes);
                                    value = decoder.decodeString(pg, field, stringValue);
                                }
                            }
                        } catch (Exception e) {
                            throw new IllegalStateException(
                                    String.format("Failed parsing field \"%s\" of table \"%s\"",
                                            field.name,
                                            field.tableOid > 0 ? pg.db.oid2name.get(field.tableOid) : "--unknown--"
                                    ), e);
                        }

                        row = rowBuilder.add(row, field, i, value);
                    }

                    queryResult = resultBuilder.add(queryResult, rowBuilder.complete(row));
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
            throw new CommandException(String.format("Failed to execute Query\nSQL: %s\n", query.getSQLString()), errorData);
        }

        if (!complete) {
            throw new IllegalStateException("Command did not complete");
        }

        final Object result = resultBuilder.complete(queryResult);

        timerContext.stop();

        return result;
    }

}

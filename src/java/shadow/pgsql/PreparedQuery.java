package shadow.pgsql;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by zilence on 12.08.14.
 */
public class PreparedQuery extends AbstractStatement {
    private int portalId = 0;

    protected final Query query;
    protected final ColumnInfo[] columnInfos;
    protected final TypeHandler[] typeDecoders;

    private final ResultBuilder resultBuilder;
    private final RowBuilder rowBuilder;

    public PreparedQuery(Connection pg, String statementId, TypeHandler[] typeEncoders, Query query, ColumnInfo[] columnInfos, TypeHandler[] typeDecoders, ResultBuilder resultBuilder, RowBuilder rowBuilder) {
        super(pg, statementId, typeEncoders);
        this.query = query;
        this.columnInfos = columnInfos;
        this.typeDecoders = typeDecoders;
        this.resultBuilder = resultBuilder;
        this.rowBuilder = rowBuilder;
    }

    private int batchSize = 0;

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public Object execute(Object... queryParams) throws IOException {
        return execute(resultBuilder, rowBuilder, Arrays.asList(queryParams));
    }

    public Object execute(final List queryParams) throws IOException {
        final String portalName = String.format("p%d", portalId++);

        if (queryParams.size() != typeEncoders.length) {
            throw new IllegalArgumentException(String.format("Not enough Params provided to Statement, expected %d got %d", typeEncoders.length, queryParams.size()));
        }

        pg.checkReady();
        pg.output.checkReset();
        pg.state = ConnectionState.QUERY_RESULT;

        // flow -> B/E/H

        writeBind(typeDecoders, queryParams, portalName);
        writeExecute(portalName, batchSize);
        writeFlush();

        pg.output.flushAndReset();

        Object queryResult = resultBuilder.init();

        // flow <- 2/D*/s?/C?
        RESULT_LOOP:
        while (true) {
            final char type = pg.input.readNextCommand();

            switch (type) {
                case '2': // BindComplete
                {
                    final int size = pg.input.readInt32();
                    if (size != 4) {
                        throw new IllegalStateException("BindComplete was not size 4 (was %d)");
                    }
                    break;
                }
                case 's': // PortalSuspended
                {
                    final int size = pg.input.readInt32();

                    pg.output.checkReset();
                    this.writeExecute(portalName, batchSize);
                    this.writeFlush();
                    pg.output.flushAndReset();
                    break;
                }
                case 'D':  // DataRow
                {
                    final int size = pg.input.readInt32();
                    final int cols = pg.input.readInt16();

                    if (cols != columnInfos.length) {
                        throw new IllegalStateException(
                                String.format("backend said to expect %d columns, but data had %d", columnInfos.length, cols)
                        );
                    }

                    Object row = rowBuilder.init();

                    for (int i = 0; i < columnInfos.length; i++) {
                        ColumnInfo field = columnInfos[i];
                        TypeHandler decoder = typeDecoders[i];

                        final int colSize = pg.input.readInt32();

                        Object value = null;

                        if (colSize != -1) {
                            if (decoder.supportsBinary()) {
                                value = decoder.decodeBinary(pg, field, colSize);
                            } else {
                                byte[] bytes = new byte[colSize];
                                pg.input.read(bytes);

                                // FIXME: assumes UTF-8
                                final String stringValue = new String(bytes);
                                value = decoder.decodeString(pg, field, stringValue);
                            }
                        }

                        row = rowBuilder.add(row, field, i, value);
                    }

                    queryResult = resultBuilder.add(queryResult, rowBuilder.complete(row));
                    break;
                }
                case 'C': { // CommandComplete
                    final int size = pg.input.readInt32();
                    final String tag = pg.input.readString();

                    break RESULT_LOOP;
                }
                default: {
                    throw new IllegalStateException(String.format("invalid protocol action while reading query results: '%s'", type));
                }
            }
        }

        doSync();

        return resultBuilder.complete(queryResult);
    }


}

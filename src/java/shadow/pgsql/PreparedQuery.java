package shadow.pgsql;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
        super(pg, statementId, typeEncoders);
        this.query = query;
        this.columnInfos = columnInfos;
        this.typeDecoders = typeDecoders;
        this.resultBuilder = resultBuilder;
        this.rowBuilder = rowBuilder;
    }

    public Object executeWith(Object... params) throws IOException {
        return execute(Arrays.asList(params));
    }

    public Object execute(final List queryParams) throws IOException {
        executeWithParams(typeDecoders, queryParams);

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

                    // FIXME: losing information (tag)
                    break;
                }
                case 'Z': // ReadyForQuery
                {
                    pg.input.readReadyForQuery();
                    break RESULT_LOOP;
                }
                default: {
                    throw new IllegalStateException(String.format("invalid protocol action while reading query results: '%s'", type));
                }
            }
        }

        return resultBuilder.complete(queryResult);
    }

}

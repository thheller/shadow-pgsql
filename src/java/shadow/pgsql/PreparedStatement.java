package shadow.pgsql;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by zilence on 12.08.14.
 *
 * @author Thomas Heller
 */
public class PreparedStatement extends AbstractStatement {
    private final static TypeHandler[] NO_COLUMNS = new TypeHandler[0];
    private final Statement statement;

    public PreparedStatement(Connection pg, String statementId, TypeHandler[] typeEncoders, Statement statement) {
        super(pg, statementId, typeEncoders);
        this.statement = statement;
    }

    public Statement getStatement() {
        return statement;
    }

    public int execute(List queryParams) throws IOException {
        // flow -> B/E/S
        writeBindExecuteSync(NO_COLUMNS, queryParams, null, 0);

        // flow <- 2/C/Z

        int rowsAffected = 0;

        RESULT_LOOP:
        while (true) {
            final char type = pg.input.readNextCommand();

            switch (type) {
                case '2': // BindComplete
                {
                    final int size = pg.input.readInt32();
                    if (size != 4) {
                        throw new IllegalStateException(String.format("BindComplete was not size 4 (was %d)", size));
                    }
                    break;
                }
                case 'C': { // CommandComplete
                    final int size = pg.input.readInt32();
                    final String tag = pg.input.readString();

                    // FIXME: is this safe?
                    // FIXME: loses tag info, might be useful? maybe change return type
                    rowsAffected = Integer.parseInt(tag.substring(1 + tag.lastIndexOf(" ")));
                    break;
                }
                case 'Z': {
                    pg.input.readReadyForQuery();
                    break RESULT_LOOP;
                }
                default: {
                    throw new IllegalStateException(String.format("invalid protocol action while reading query results: '%s'", type));
                }
            }
        }

        return rowsAffected;
    }

    public int execute(Object... queryParams) throws IOException {
        return execute(Arrays.asList(queryParams));
    }

}

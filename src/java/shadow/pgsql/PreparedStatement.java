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

    public StatementResult execute(List queryParams) throws IOException {
        // flow -> B/E/S
        executeWithParams(NO_COLUMNS, queryParams);

        // flow <- 2/C/Z
        return pg.input.readStatementResult();
    }

    public StatementResult execute(Object... queryParams) throws IOException {
        return execute(Arrays.asList(queryParams));
    }

}

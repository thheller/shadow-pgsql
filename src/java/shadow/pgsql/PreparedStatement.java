package shadow.pgsql;

import com.codahale.metrics.Timer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by zilence on 12.08.14.
 *
 * @author Thomas Heller
 */
public class PreparedStatement extends PreparedBase {
    private final static TypeHandler[] NO_COLUMNS = new TypeHandler[0];
    private final Statement statement;

    public PreparedStatement(Connection pg, String statementId, TypeHandler[] typeEncoders, Statement statement) {
        super(pg, statementId, typeEncoders, statement.getName());
        this.statement = statement;
    }

    public Statement getStatement() {
        return statement;
    }

    @Override
    public String getSQLString() {
        return statement.getSQLString();
    }

    public StatementResult executeWith(Object... queryParams) throws IOException {
        return execute(Arrays.asList(queryParams));
    }

    public StatementResult execute(List queryParams) throws IOException {
        Timer.Context timerContext = executeTimer.time();

        // flow -> B/E/S
        executeWithParams(NO_COLUMNS, queryParams);

        // flow <- 2/C/Z
        final StatementResult result =  pg.input.readStatementResult(statement.getSQLString());

        pg.db.metricCollector.collectExecuteTime(statement.getName(), statement.getSQLString(), timerContext.stop());

        return result;
    }


}

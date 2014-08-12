package shadow.pgsql;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zilence on 12.08.14.
 */
public class SimpleStatement implements Statement {
    private final String sql;
    private final List<TypeHandler> parameterTypes;

    public SimpleStatement(String sql, List<TypeHandler> parameterTypes) {
        this.sql = sql;
        this.parameterTypes = parameterTypes;
    }

    public SimpleStatement(String sql) {
        this(sql, new ArrayList<>());
    }

    @Override
    public String getStatement() {
        return sql;
    }

    @Override
    public List<TypeHandler> getParameterTypes() {
        return parameterTypes;
    }
}

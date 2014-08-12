package shadow.pgsql;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zilence on 12.08.14.
 */
public class SimpleQuery implements Query {

    private final String statement;
    private final List<TypeHandler> parameterTypes;
    private ResultBuilder resultBuilder;
    private RowBuilder rowBuilder;

    public SimpleQuery(String statement) {
        this(statement, new ArrayList<>());
    }

    public SimpleQuery(String statement, List<TypeHandler> typeHandlers) {
        this(statement, typeHandlers, Handlers.RESULT_AS_LIST, Handlers.ROW_AS_MAP);
    }

    public SimpleQuery(String statement, List<TypeHandler> parameterTypes, ResultBuilder resultBuilder, RowBuilder rowBuilder) {
        this.statement = statement;
        this.parameterTypes = parameterTypes;
        this.resultBuilder = resultBuilder;
        this.rowBuilder = rowBuilder;
    }

    @Override
    public String getStatement() {
        return statement;
    }

    @Override
    public List<TypeHandler> getParameterTypes() {
        return parameterTypes;
    }

    public void setResultBuilder(ResultBuilder resultBuilder) {
        this.resultBuilder = resultBuilder;
    }

    public void setRowBuilder(RowBuilder rowBuilder) {
        this.rowBuilder = rowBuilder;
    }

    @Override
    public ResultBuilder createResultBuilder(ColumnInfo[] columns) {
        return resultBuilder;
    }

    @Override
    public RowBuilder createRowBuilder(ColumnInfo[] columns) {
        return rowBuilder;
    }
}

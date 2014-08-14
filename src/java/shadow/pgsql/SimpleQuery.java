package shadow.pgsql;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zilence on 12.08.14.
 */
public class SimpleQuery implements Query {

    private final String sql;

    private final List<TypeHandler> parameterTypes;
    private ResultBuilder resultBuilder;
    private RowBuilder rowBuilder;
    private TypeRegistry typeRegistry = TypeRegistry.DEFAULT;

    public SimpleQuery(String sql) {
        this(sql, new ArrayList<>());
    }

    public SimpleQuery(String sql, List<TypeHandler> typeHandlers) {
        this(sql, typeHandlers, Handlers.RESULT_AS_LIST, Handlers.ROW_AS_MAP);
    }

    public SimpleQuery(String sql, List<TypeHandler> parameterTypes, ResultBuilder resultBuilder, RowBuilder rowBuilder) {
        this.sql = sql;
        this.parameterTypes = parameterTypes;
        this.resultBuilder = resultBuilder;
        this.rowBuilder = rowBuilder;
    }

    public TypeRegistry getTypeRegistry() {
        return typeRegistry;
    }

    public void setTypeRegistry(TypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Override
    public String getSQLString() {
        return sql;
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

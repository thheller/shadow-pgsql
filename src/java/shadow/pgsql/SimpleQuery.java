package shadow.pgsql;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zilence on 12.08.14.
 */
public class SimpleQuery implements Query {

    private String name;

    private final String sql;

    private final List<TypeHandler> parameterTypes;

    private TypeRegistry typeRegistry = TypeRegistry.DEFAULT;

    private ResultBuilder resultBuilder;
    private RowBuilder rowBuilder;

    public SimpleQuery(String sql) {
        this(sql, new ArrayList<>());
    }

    public SimpleQuery(String sql, List<TypeHandler> typeHandlers) {
        this(sql, typeHandlers, Helpers.RESULT_AS_LIST, Helpers.ROW_AS_MAP);
    }

    public SimpleQuery(String sql, List<TypeHandler> parameterTypes, TypeRegistry typeRegistry, ResultBuilder resultBuilder, RowBuilder rowBuilder) {
        this.sql = sql;
        this.parameterTypes = parameterTypes;
        this.typeRegistry = typeRegistry;
        this.resultBuilder = resultBuilder;
        this.rowBuilder = rowBuilder;
    }

    public SimpleQuery(String sql, List<TypeHandler> parameterTypes, ResultBuilder resultBuilder, RowBuilder rowBuilder) {
        this.sql = sql;
        this.parameterTypes = parameterTypes;
        this.resultBuilder = resultBuilder;
        this.rowBuilder = rowBuilder;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

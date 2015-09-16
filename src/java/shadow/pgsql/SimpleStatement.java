package shadow.pgsql;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zilence on 12.08.14.
 */
public class SimpleStatement implements Statement {
    private String name;

    private final String sql;
    private final List<TypeHandler> parameterTypes;

    private TypeRegistry typeRegistry = TypeRegistry.DEFAULT;

    public SimpleStatement(String sql, List<TypeHandler> parameterTypes) {
        this.sql = sql;
        this.parameterTypes = parameterTypes;
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

    public SimpleStatement(String sql) {
        this(sql, new ArrayList<>());
    }

    @Override
    public String getSQLString() {
        return sql;
    }

    @Override
    public List<TypeHandler> getParameterTypes() {
        return parameterTypes;
    }
}

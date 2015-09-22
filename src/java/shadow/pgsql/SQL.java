package shadow.pgsql;

import shadow.pgsql.types.Int4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Desribes your SQL string and how results should be handled, should then be prepared and executed via Connection.
 * <p/>
 * Contains information about parameter types
 * <p/>
 * Create with SQL.query or SQL.statement
 * <p/>
 * Query returns Data
 * Statement only returns the StatementResult (usually how many rows were affected)
 */
public final class SQL {
    public enum Type {
        QUERY,
        STATEMENT
    }

    private static final Pattern PARAM_PATTERN = Pattern.compile("\\$([0-9]+)", Pattern.MULTILINE | Pattern.DOTALL);

    private final Type type;
    private final String name;
    private final String sql;
    private final int paramCount;
    private final List<TypeHandler> parameterTypes;
    private final TypeRegistry typeRegistry;
    private final ResultBuilder.Factory resultBuilder;
    private final RowBuilder.Factory rowBuilder;

    public SQL(Type type, String name, String sql, int paramCount, List<TypeHandler> parameterTypes, TypeRegistry typeRegistry, ResultBuilder.Factory resultBuilder, RowBuilder.Factory rowBuilder) {
        this.type = type;
        this.name = name;
        this.sql = sql;
        this.paramCount = paramCount;
        this.parameterTypes = parameterTypes;
        this.typeRegistry = typeRegistry;
        this.resultBuilder = resultBuilder;
        this.rowBuilder = rowBuilder;
    }

    public Type getType() {
        return type;
    }

    public int getParamCount() {
        return paramCount;
    }

    public String getName() {
        return name;
    }

    public TypeRegistry getTypeRegistry() {
        return typeRegistry;
    }

    public String getSQLString() {
        return sql;
    }

    public List<TypeHandler> getParameterTypes() {
        return parameterTypes;
    }

    public ResultBuilder.Factory getResultBuilder() {
        return resultBuilder;
    }

    public RowBuilder.Factory getRowBuilder() {
        return rowBuilder;
    }

    public boolean expectsData() {
        return type == Type.QUERY;
    }

    @Override
    public String toString() {
        return "SQL{" +
                "type=" + type +
                ", name='" + name + '\'' +
                ", sql='" + sql + '\'' +
                ", paramCount=" + paramCount +
                '}';
    }

    public static class Builder {
        final Type type;
        final String sql;
        private String name = null;
        private TypeRegistry typeRegistry = TypeRegistry.DEFAULT;
        private ResultBuilder.Factory resultBuilder = null;
        private RowBuilder.Factory rowBuilder = null;
        private List<TypeHandler> paramTypes = new ArrayList<>();

        Builder(Type type, String sql) {
            this.type = type;
            this.sql = sql;
        }

        public Type getType() {
            return type;
        }

        public String getSQLString() {
            return sql;
        }

        public String getName() {
            return name;
        }

        public TypeRegistry getTypeRegistry() {
            return typeRegistry;
        }

        public ResultBuilder.Factory getResultBuilder() {
            return resultBuilder;
        }

        public RowBuilder.Factory getRowBuilder() {
            return rowBuilder;
        }

        public List<TypeHandler> getParamTypes() {
            return paramTypes;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withTypeRegistry(TypeRegistry types) {
            this.typeRegistry = types;
            return this;
        }

        public Builder withParamTypes(List<TypeHandler> paramTypes) {
            this.paramTypes = paramTypes;
            return this;
        }

        public Builder withResultBuilder(ResultBuilder.Factory rbf) {
            if (type != Type.QUERY) {
                throw new IllegalStateException("not building a Query");
            }

            this.resultBuilder = rbf;
            return this;
        }

        public Builder buildResultsWith(final ResultBuilder rb) {
            return withResultBuilder(new ResultBuilder.Factory() {
                @Override
                public ResultBuilder create(ColumnInfo[] columns) {
                    return rb;
                }
            });
        }

        public Builder withRowBuilder(RowBuilder.Factory rbf) {
            if (type != Type.QUERY) {
                throw new IllegalStateException("not building a Query");
            }
            this.rowBuilder = rbf;
            return this;
        }

        public Builder buildRowsWith(final RowBuilder rb) {
            return withRowBuilder(new RowBuilder.Factory() {
                @Override
                public RowBuilder create(ColumnInfo[] columns) {
                    return rb;
                }
            });
        }

        public Builder addParameterType(TypeHandler typeHandler) {
            this.paramTypes.add(typeHandler);
            return this;
        }

        public int getParamCount() {
            Matcher m = PARAM_PATTERN.matcher(sql);
            int paramCount = 0;

            while (m.find()) {
                String d = m.group().substring(1);
                paramCount = Math.max(paramCount, Integer.parseInt(d));
            }
            return paramCount;
        }

        public SQL create() {
            int paramCount = getParamCount();

            if (type == Type.QUERY) {
                return new SQL(type, name, sql, paramCount, paramTypes, typeRegistry, resultBuilder, rowBuilder);
            } else {
                return new SQL(type, name, sql, paramCount, paramTypes, typeRegistry, null, null);
            }
        }

        public PreparedSQL prepare(Connection con) throws IOException {
            return con.prepare(this.create());
        }

    }

    public static Builder query(String sql) {
        return new Builder(Type.QUERY, sql)
                .buildResultsWith(Helpers.RESULT_AS_LIST)
                .buildRowsWith(Helpers.ROW_AS_MAP);
    }

    public static Builder statement(String sql) {
        return new Builder(Type.STATEMENT, sql);
    }
}

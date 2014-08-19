package shadow.pgsql;

import shadow.pgsql.types.Types;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Immutable Type Registry
 * <p/>
 * Allows overriding how types and specific Columns in a Table are handled
 * <p/>
 * Example:
 * <p/>
 * Table T with column C of type text, type text usually encode/decodes Strings.
 * <p/>
 * .registerColumnHandler(T, C, myHandler)
 * <p/>
 * allows you to encode/decode other types into a String column
 * (eg. Clojure Keywords, EDN, etc.)
 * <p/>
 * Want to handle timestamp/timestamptz as JodaTime? No problem.
 * <p/>
 * TypeRegistry is thread-safe and you'll usually just have one per project.
 *
 * @author Thomas Heller
 */
// FIXME: maybe extract interface?
public class TypeRegistry {
    private final Map<Integer, TypeHandler> typeHandlers;
    private final Map<ColumnByName, TypeHandler> customHandlers;

    public static final TypeRegistry DEFAULT = createDefault();

    public static class Builder {
        private final Map<Integer, TypeHandler> typeHandlers;
        private final Map<ColumnByName, TypeHandler> customHandlers;

        Builder() {
            this(new HashMap<>(), new HashMap<>());
        }

        Builder(Map<Integer, TypeHandler> typeHandlers, Map<ColumnByName, TypeHandler> customHandlers) {
            this.typeHandlers = typeHandlers;
            this.customHandlers = customHandlers;
        }

        public Builder registerTypeHandler(TypeHandler handler) {
            this.typeHandlers.put(handler.getTypeOid(), handler);
            return this;
        }

        public Builder registerColumnHandler(String tableName, String columnName, TypeHandler handler) {
            this.customHandlers.put(new ColumnByName(tableName, columnName), handler);
            return this;
        }

        public TypeRegistry build() {
            return new TypeRegistry(typeHandlers, customHandlers);
        }
    }

    private static TypeRegistry createDefault() {
        Builder b = new Builder();

        TypeHandler[] defaults = new TypeHandler[]{
                Types.INT2,
                Types.INT2_ARRAY,
                Types.INT4,
                Types.INT4_ARRAY,
                Types.INT8,
                Types.INT8_ARRAY,
                Types.FLOAT4,
                Types.FLOAT8,
                Types.OID,
                Types.NUMERIC,
                Types.NAME,
                Types.TEXT,
                Types.TEXT_ARRAY,
                Types.VARCHAR,
                Types.VARCHAR_ARRAY,
                Types.TIMESTAMP,
                Types.TIMESTAMPTZ,
                Types.DATE,
                Types.BYTEA,
                Types.BOOL
        };

        for (TypeHandler t : defaults) {
            b.registerTypeHandler(t);
        }

        return b.build();
    }

    TypeRegistry(Map<Integer, TypeHandler> typeHandlers, Map<ColumnByName, TypeHandler> customHandlers) {
        this.typeHandlers = Collections.unmodifiableMap(typeHandlers);
        this.customHandlers = Collections.unmodifiableMap(customHandlers);
    }

    public static Builder copyDefault() {
        return DEFAULT.copy();
    }

    public Builder copy() {
        Map<Integer, TypeHandler> types = new HashMap<>();
        Map<ColumnByName, TypeHandler> handlers = new HashMap<>();

        types.putAll(this.typeHandlers);
        handlers.putAll(this.customHandlers);

        return new Builder(types, handlers);
    }

    public TypeHandler getTypeHandlerForOid(Database pg, int typeOid) {
        final TypeHandler handler = typeHandlers.get(typeOid);
        if (handler == null) {
            throw new IllegalArgumentException(String.format("unsupported type: %d (%s)", typeOid, pg.getNameForOid(typeOid)));
        }
        return handler;
    }

    public TypeHandler getTypeHandlerForField(Database pg, ColumnInfo column) {

        // column that belongs to a table
        if (column.tableOid > 0 || column.positionInTable >= -1) {
            final String tableName = pg.getNameForOid(column.tableOid);

            // we have table name
            if (tableName != null) {
                final String columnName = pg.getNameForColumn(tableName, column.positionInTable);

                // DO NOT USE column.name CAUSE THAT MAY BE AN ALIAS
                // SELECT foo AS bar -- column.name is bar, we want foo

                // the column name is known
                if (columnName != null) {
                    final TypeHandler customHandler = getTypeHandlerForColumn(tableName, columnName);

                    if (customHandler != null) {
                        return customHandler;
                    }
                }
            }
        }

        return getTypeHandlerForOid(pg, column.typeOid);
    }

    public TypeHandler getTypeHandlerForColumn(String tableName, String columnName) {
        return customHandlers.get(new ColumnByName(tableName, columnName));
    }

    private static class ColumnByName {
        final String table;
        final String column;

        private ColumnByName(String table, String column) {
            this.table = table;
            this.column = column;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ColumnByName that = (ColumnByName) o;

            if (!column.equals(that.column)) return false;
            if (!table.equals(that.table)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = table.hashCode();
            result = 31 * result + column.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "ColumnByName{" +
                    "table='" + table + '\'' +
                    ", column='" + column + '\'' +
                    '}';
        }
    }

}

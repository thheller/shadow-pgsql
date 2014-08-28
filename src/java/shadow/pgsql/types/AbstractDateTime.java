package shadow.pgsql.types;

import shadow.pgsql.ColumnInfo;
import shadow.pgsql.Connection;
import shadow.pgsql.TypeHandler;

import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

/**
 * Created by zilence on 14.08.14.
 */
public abstract class AbstractDateTime implements TypeHandler {
    private final int oid;
    private final String name;
    private final DateTimeFormatter format;

    protected AbstractDateTime(int oid, String name, DateTimeFormatter format) {
        this.oid = oid;
        this.name = name;
        this.format = format;
    }

    @Override
    public int getTypeOid() {
        return oid;
    }

    @Override
    public String getTypeName() {
        return name;
    }

    @Override
    public String encodeToString(Connection con, Object param) {
        return format.format((TemporalAccessor) param);
    }

    @Override
    public Object decodeString(Connection con, ColumnInfo field, String value) {
        return convertParsed(con, format.parse(value));
    }

    protected abstract Object convertParsed(Connection con, TemporalAccessor temporal);
}

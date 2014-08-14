package shadow.pgsql.types;

import shadow.pgsql.ColumnInfo;
import shadow.pgsql.Connection;
import shadow.pgsql.ProtocolOutput;
import shadow.pgsql.TypeHandler;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

/**
 * Created by zilence on 14.08.14.
 */
public abstract class DateTime implements TypeHandler {
    private final int oid;
    private final DateTimeFormatter format;

    protected DateTime(int oid, DateTimeFormatter format) {
        this.oid = oid;
        this.format = format;
    }

    @Override
    public int getTypeOid() {
        return oid;
    }

    @Override
    public boolean supportsBinary() {
        return false;
    }

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        throw new UnsupportedOperationException("TBD");
    }

    @Override
    public String encodeToString(Connection con, Object param) {
        return format.format((TemporalAccessor) param);
    }

    // https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/datetime.c
    // https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/timestamp.c
    private static final long USECS_PER_DAY = 86400000000l;
    private static final long POSTGRES_EPOCH_JDATE = 2451545;
    private static final int MONTHS_PER_YEAR = 12;

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, int colSize) throws IOException {
        long jd = con.input.stream.readLong();
        throw new UnsupportedOperationException("TBD");
    }

    protected abstract Object convertParsed(TemporalAccessor temporal);

    @Override
    public Object decodeString(Connection con, ColumnInfo field, String value) {
        return convertParsed(format.parse(value));
    }
}

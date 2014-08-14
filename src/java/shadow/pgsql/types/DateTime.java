package shadow.pgsql.types;

import shadow.pgsql.ColumnInfo;
import shadow.pgsql.Connection;
import shadow.pgsql.ProtocolOutput;
import shadow.pgsql.TypeHandler;

import java.io.IOException;
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
    // how do you define variable length fields in text
    // "yyyy-MM-dd HH:mm:ss.SSS"
    public static final DateTime TIMESTAMP = new DateTime(1114,
            new DateTimeFormatterBuilder()
                    .appendValue(ChronoField.YEAR, 4)
                    .appendLiteral("-")
                    .appendValue(ChronoField.MONTH_OF_YEAR, 2)
                    .appendLiteral("-")
                    .appendValue(ChronoField.DAY_OF_MONTH, 2)
                    .appendLiteral(" ")
                    .appendValue(ChronoField.HOUR_OF_DAY, 2)
                    .appendLiteral(":")
                    .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                    .appendLiteral(":")
                    .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                    .appendFraction(ChronoField.MILLI_OF_SECOND, 0, 3, true)
                    .toFormatter()
    ) {
        @Override
        protected Object convertParsed(TemporalAccessor temporal) {
            return LocalDateTime.from(temporal);
        }
    };

    public static final DateTime TIMESTAMPTZ = new DateTime(1184,
            new DateTimeFormatterBuilder()
                    .appendValue(ChronoField.YEAR, 4)
                    .appendLiteral("-")
                    .appendValue(ChronoField.MONTH_OF_YEAR, 2)
                    .appendLiteral("-")
                    .appendValue(ChronoField.DAY_OF_MONTH, 2)
                    .appendLiteral(" ")
                    .appendValue(ChronoField.HOUR_OF_DAY, 2)
                    .appendLiteral(":")
                    .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                    .appendLiteral(":")
                    .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                    .appendFraction(ChronoField.MILLI_OF_SECOND, 0, 3, true)
                    .appendOffset("+HHmm", "+00")
                    .toFormatter()
    ) {
        @Override
        protected Object convertParsed(TemporalAccessor temporal) {
            return OffsetDateTime.from(temporal);
        }
    };

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

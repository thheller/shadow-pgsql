package shadow.pgsql.types;

import shadow.pgsql.Connection;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

/**
 * Created by zilence on 14.08.14.
 */
public class Types {
    // how do you define variable length fields in text
    // "yyyy-MM-dd HH:mm:ss.SSS"
    public static final Timestamp TIMESTAMP = new Timestamp(1114,
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
                    .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                    .toFormatter()
    ) {
        @Override
        protected Object convertParsed(Connection con, TemporalAccessor temporal) {
            // always return with timezone, just not worth it to use localdatetime
            return LocalDateTime.from(temporal).atZone(ZoneId.of(con.getParameterValue("TimeZone"))).toOffsetDateTime();
        }
    };
    public static final Timestamp TIMESTAMPTZ = new Timestamp(1184,
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
                    .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                    .appendOffset("+HHmm", "+00")
                    .toFormatter()
    ) {
        @Override
        protected Object convertParsed(Connection con, TemporalAccessor temporal) {
            return OffsetDateTime.from(temporal);
        }
    };
    public static final Date DATE = new Date();

    public static final Int2 INT2 = new Int2(21);
    public static final TypedArray INT2_ARRAY = new TypedArray(1005, INT2, TypedArray.makeReader(Short.TYPE));

    public static final Int4 OID = new Int4(26);
    public static final Int4 INT4 = new Int4(23);
    public static final TypedArray INT4_ARRAY = new TypedArray(1007, INT4, TypedArray.makeReader(Integer.TYPE));

    public static final Int8 INT8 = new Int8(20);
    public static final TypedArray INT8_ARRAY = new TypedArray(1016, INT8, TypedArray.makeReader(Long.TYPE));

    public static final Numeric NUMERIC = new Numeric();

    public static final Text NAME = new Text(19);
    public static final Text TEXT = new Text(25);
    public static final TypedArray TEXT_ARRAY = new TypedArray(1009, TEXT, TypedArray.makeReader(String.class));
    public static final Text CHAR = new Text(1042);
    public static final Text VARCHAR = new Text(1043);
    public static final TypedArray VARCHAR_ARRAY = new TypedArray(1015, VARCHAR, TypedArray.makeReader(String.class));


    public static final ByteA BYTEA = new ByteA();

    public static final Bool BOOL = new Bool();
}

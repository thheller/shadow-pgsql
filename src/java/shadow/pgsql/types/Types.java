package shadow.pgsql.types;

import shadow.pgsql.Connection;

import java.math.BigDecimal;
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
    public static final int OID_NAME = 19;
    public static final int OID_INT8 = 20;
    public static final int OID_INT2 = 21;
    public static final int OID_INT4 = 23;
    public static final int OID_TEXT = 25;
    public static final int OID_OID = 26;

    public static final int OID_CHAR = 1042;
    public static final int OID_VARCHAR = 1043;

    public static final int OID_TIMESTAMP = 1114;
    public static final int OID_TIMESTAMPTZ = 1184;

    public static final int OID_NUMERIC = 1700;

    // how do you define variable length fields in text
    // "yyyy-MM-dd HH:mm:ss.SSS"
    public static final Timestamp TIMESTAMP = new Timestamp(OID_TIMESTAMP, "timestamp",
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
    public static final Timestamp TIMESTAMPTZ = new Timestamp(OID_TIMESTAMPTZ, "timestamptz",
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

    public static final Int2 INT2 = new Int2(OID_INT2, "int2");
    public static final TypedArray INT2_ARRAY = new TypedArray(INT2, TypedArray.makeReader(Short.TYPE), false);

    public static final Int4 OID = new Int4(OID_OID, "oid");
    public static final Int4 INT4 = new Int4(OID_INT4, "int4");
    public static final TypedArray INT4_ARRAY = new TypedArray(INT4, TypedArray.makeReader(Integer.TYPE), false);

    public static final Int8 INT8 = new Int8(OID_INT8, "int8");
    public static final TypedArray INT8_ARRAY = new TypedArray(INT8, TypedArray.makeReader(Long.TYPE), false);

    public static final Numeric NUMERIC = new Numeric();
    public static final TypedArray NUMERIC_ARRAY = new TypedArray(NUMERIC, TypedArray.makeReader(BigDecimal.class), false);

    public static final Text NAME = new Text(OID_NAME, "name");
    public static final Text TEXT = new Text(OID_TEXT, "text");
    public static final TypedArray TEXT_ARRAY = new TypedArray(TEXT, TypedArray.makeReader(String.class), true);
    public static final Text CHAR = new Text(OID_CHAR, "char");
    public static final Text VARCHAR = new Text(OID_VARCHAR, "varchar");
    public static final TypedArray VARCHAR_ARRAY = new TypedArray(VARCHAR, TypedArray.makeReader(String.class), true);

    public static final TypedArray TIMESTAMP_ARRAY = new TypedArray(TIMESTAMP, TypedArray.makeReader(OffsetDateTime.class), true);
    public static final TypedArray TIMESTAMPTZ_ARRAY = new TypedArray(TIMESTAMPTZ, TypedArray.makeReader(OffsetDateTime.class), true);


    public static final ByteA BYTEA = new ByteA();

    public static final Bool BOOL = new Bool();
    public static final Float4 FLOAT4 = new Float4();
    public static final Float8 FLOAT8 = new Float8();

    public static final HStore HSTORE = new HStore();
}

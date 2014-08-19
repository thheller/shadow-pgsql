package shadow.pgsql.types;

import shadow.pgsql.ColumnInfo;
import shadow.pgsql.Connection;
import shadow.pgsql.ProtocolOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.*;
import java.time.format.DateTimeFormatter;

/**
 * Created by zilence on 19.08.14.
 */
public abstract class Timestamp extends AbstractDateTime {

    protected Timestamp(int oid, DateTimeFormatter format) {
        super(oid, format);
    }

    @Override
    public boolean supportsBinary() {
        return true;
    }

    // timestamps are number of microseconds from 2000-01-01 00:00:00.000000
    // microseconds between 1970-01-01 -> 2000-01-01
    private static final long PG_EPOCH_OFFSET = 946684800000000l;

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, ByteBuffer buf, int size) throws IOException {
        long jd = buf.getLong() + PG_EPOCH_OFFSET;
        long milli = jd / 1000;
        long micros = jd % 1000;
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(milli).plusNanos(micros * 1000), ZoneOffset.UTC);
    }

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        // this can't be right, should not be this complicated

        Instant i = null;
        if (param instanceof OffsetDateTime) {
            i = ((OffsetDateTime) param)
                    .atZoneSameInstant(ZoneOffset.UTC)
                    .toInstant();
        } else if (param instanceof ZonedDateTime) {
            i = ((ZonedDateTime) param)
                    .withZoneSameInstant(ZoneOffset.UTC)
                    .toInstant();
        } else if (param instanceof LocalDateTime) {
            i = ((LocalDateTime) param)
                    .atZone(ZoneId.systemDefault())
                    .withZoneSameInstant(ZoneOffset.UTC)
                    .toInstant();
        } else if (param instanceof Instant) {
            i = (Instant) param;
        } else {
            throw new IllegalArgumentException(String.format("unsupported timestamp type: %s", param.getClass().getName()));
        }

        long n = i.getNano();
        long milli = i.toEpochMilli();
        long micros = ((n / 1000) % 1000);
        long ts = ((milli * 1000) + micros) - PG_EPOCH_OFFSET;
        output.int64(ts);
    }
}

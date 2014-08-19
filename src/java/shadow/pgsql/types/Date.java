package shadow.pgsql.types;

import shadow.pgsql.ColumnInfo;
import shadow.pgsql.Connection;
import shadow.pgsql.ProtocolOutput;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;

/**
 * Created by zilence on 19.08.14.
 */
public class Date extends AbstractDateTime {

    public Date() {
        super(1082, DateTimeFormatter.ISO_DATE);
    }

    @Override
    public boolean supportsBinary() {
        return true;
    }


    public static final LocalDate PG_DATE_BASE = LocalDate.of(2000, 1, 1);

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        if (!(param instanceof LocalDate)) {
            // FIXME: maybe accept DateTime stuff and just drop the time?
            throw new IllegalArgumentException(String.format("not a localdate: %s", param.getClass().getName()));
        }

        int days = (int) ChronoUnit.DAYS.between(PG_DATE_BASE, (LocalDate)param);
        output.int32(days);
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, int colSize) throws IOException {
        int days = con.input.readInt32();
        return PG_DATE_BASE.plusDays(days);
    }

    @Override
    protected Object convertParsed(Connection con, TemporalAccessor temporal) {
        return LocalDate.from(temporal);
    }
}

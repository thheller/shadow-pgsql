package shadow.pgsql.types;

import shadow.pgsql.Connection;
import shadow.pgsql.ColumnInfo;
import shadow.pgsql.ProtocolOutput;
import shadow.pgsql.TypeHandler;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * Created by zilence on 10.08.14.
 */
public class Numeric implements TypeHandler {
    public static final int OID = 1700;
    public static final Numeric INSTANCE = new Numeric();

    Numeric() {
    }

    @Override
    public int getTypeOid() {
        return OID;
    }

    @Override
    public boolean supportsBinary() {
        return false; // need to figure out encoding first
    }

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        throw new AbstractMethodError("need to figure this out first");
    }

    @Override
    public String encodeToString(Connection con, Object param) {
        return ((BigDecimal)param).toPlainString();
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, int colSize) throws IOException {
        // FIXME: I HAVE NO IDEA WHAT I'M DOING!
        // this can't be right ... do I really need a String?

        final int ndigits = con.input.stream.readShort();
        final int weight = con.input.stream.readShort(); // what does this mean?
        final int sign = con.input.stream.readShort();
        final int rscale = con.input.stream.readShort(); // pos of decimal point from right

        // doubt that this is efficient?

        StringBuilder sb = new StringBuilder();
        int[] digits = new int[ndigits];
        for (int i = 0; i < ndigits; i++) {
            digits[i] = con.input.stream.readUnsignedShort();
        }

        throw new AbstractMethodError("TBD: figure out the rest");
    }

    @Override
    public Object decodeString(Connection con, ColumnInfo field, String value) {
        return new BigDecimal(value);
    }
}

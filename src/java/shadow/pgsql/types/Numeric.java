package shadow.pgsql.types;

import shadow.pgsql.ColumnInfo;
import shadow.pgsql.Connection;
import shadow.pgsql.ProtocolOutput;
import shadow.pgsql.TypeHandler;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * Created by zilence on 10.08.14.
 */
public class Numeric implements TypeHandler {
    public static final int OID = 1700;

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
        return ((BigDecimal) param).toPlainString();
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, ByteBuffer buf, int colSize) throws IOException {
        // FIXME: I HAVE NO IDEA WHAT I'M DOING!

        final int ndigits = buf.getShort();
        final int weight = buf.getShort(); // what does this mean?
        final int sign = buf.getShort();
        final int rscale = buf.getShort(); // pos of decimal point from right

        StringBuilder sb = new StringBuilder();
        int[] digits = new int[ndigits];
        for (int i = 0; i < ndigits; i++) {
            digits[i] = buf.getShort();
        }

        throw new UnsupportedOperationException("TBD: figure out the rest");
    }

    @Override
    public Object decodeString(Connection con, ColumnInfo field, String value) {
        return new BigDecimal(value);
    }
}

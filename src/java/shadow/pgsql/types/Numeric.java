package shadow.pgsql.types;

import shadow.pgsql.ColumnInfo;
import shadow.pgsql.Connection;
import shadow.pgsql.ProtocolOutput;
import shadow.pgsql.TypeHandler;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Created by zilence on 10.08.14.
 */
public class Numeric implements TypeHandler {

    Numeric() {
    }

    @Override
    public int getTypeOid() {
        return Types.OID_NUMERIC;
    }

    @Override
    public String getTypeName() {
        return "numeric";
    }

    @Override
    public boolean supportsBinary() {
        return false; // need to figure out encoding first
    }

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        NBase nbase = NBase.pack((BigDecimal) param);

        output.int16((short) nbase.digits.length);
        output.int16(nbase.weight);
        output.int16(nbase.sign);
        output.int16(nbase.dscale);

        for (short v : nbase.digits) {
            output.int16(v);
        }
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, ByteBuffer buf, int colSize) throws IOException {
        final short ndigits = buf.getShort();
        final short weight = buf.getShort();
        final short sign = buf.getShort();
        final short dscale = buf.getShort();

        short[] digits = new short[ndigits];

        for (int i = 0; i < ndigits; i++) {
            digits[i] = buf.getShort();
        }

        return NBase.unpack(weight, sign, dscale, digits);
    }

    @Override
    public String encodeToString(Connection con, Object param) {
        if (param instanceof BigDecimal) {
            return ((BigDecimal) param).toPlainString();
        } else if (param instanceof BigInteger) {
            return param.toString();
        } else if (param instanceof Double) {
            return BigDecimal.valueOf((Double)param).toPlainString();
        } else if (param instanceof Float) {
            return BigDecimal.valueOf((Float)param).toPlainString();
        } else if (param instanceof Long) {
            return BigDecimal.valueOf((Long) param).toPlainString();
        } else if (param instanceof Integer) {
            return BigDecimal.valueOf((Integer) param).toPlainString();
        } else {
            throw new IllegalArgumentException(String.format("invalid BigDecimal type: %s [%s]", param.getClass().getName(), param));
        }
    }

    @Override
    public Object decodeString(Connection con, ColumnInfo field, String value) {
        return new BigDecimal(value);
    }
}

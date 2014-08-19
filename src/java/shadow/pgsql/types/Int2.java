package shadow.pgsql.types;

import shadow.pgsql.ColumnInfo;
import shadow.pgsql.Connection;
import shadow.pgsql.ProtocolOutput;
import shadow.pgsql.TypeHandler;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by zilence on 10.08.14.
 */
public class Int2 implements TypeHandler {
    private final int oid;

    public Int2(int oid) {
        this.oid = oid;
    }

    @Override
    public int getTypeOid() {
        return oid;
    }

    @Override
    public boolean supportsBinary() {
        return true;
    }

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        if (param instanceof Long) {
            output.int16(((Long) param).shortValue());
        } else if (param instanceof Integer) {
            output.int16(((Integer) param).shortValue());
        } else if (param instanceof Short) {
            output.int16((Short) param);
        } else {
            throw new IllegalArgumentException(String.format("Unsupported int2 type: %s [%s]", param.getClass().getName(), param.toString()));
        }
    }

    @Override
    public String encodeToString(Connection con, Object param) {
        return param.toString();
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, ByteBuffer buf, int colSize) throws IOException {
        return buf.getShort();
    }

    @Override
    public Object decodeString(Connection con, ColumnInfo field, String value) {
        return Integer.valueOf(value);
    }
}

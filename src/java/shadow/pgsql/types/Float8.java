package shadow.pgsql.types;

import shadow.pgsql.ColumnInfo;
import shadow.pgsql.Connection;
import shadow.pgsql.ProtocolOutput;
import shadow.pgsql.TypeHandler;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by zilence on 19.08.14.
 */
public class Float8 implements TypeHandler {

    @Override
    public int getTypeOid() {
        return 701;
    }

    @Override
    public String getTypeName() {
        return "float8";
    }

    @Override
    public boolean supportsBinary() {
        return true;
    }

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        if (param instanceof Double) {
            output.float8(((Double) param));
        } else if (param instanceof Float) {
            output.float8(((Float) param).doubleValue());
        } else if  (param instanceof Long) {
            output.float8(((Long)param).doubleValue());
        } else if  (param instanceof Integer) {
            output.float8(((Integer)param).doubleValue());
        } else {
            throw new IllegalArgumentException(String.format("not a float8: %s", param.getClass().getName()));
        }
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, ByteBuffer buf, int size) throws IOException {
        return buf.getDouble();
    }

    @Override
    public String encodeToString(Connection con, Object param) {
        throw new UnsupportedOperationException("only binary format supported");
    }

    @Override
    public Object decodeString(Connection con, ColumnInfo field, String value) {
        throw new UnsupportedOperationException("only binary format supported");
    }
}


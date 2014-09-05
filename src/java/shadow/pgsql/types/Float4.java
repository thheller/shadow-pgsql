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
public class Float4 implements TypeHandler {

    @Override
    public int getTypeOid() {
        return 700;
    }

    @Override
    public String getTypeName() {
        return "float4";
    }

    @Override
    public boolean supportsBinary() {
        return true;
    }

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        if (param instanceof Float) {
            output.float4((Float) param);
        } else if (param instanceof Double) {
            output.float4(((Double) param).floatValue());
        } else if  (param instanceof Long) {
            output.float4(((Long)param).floatValue());
        } else if  (param instanceof Integer) {
            output.float4(((Integer)param).floatValue());
        } else {
            throw new IllegalArgumentException(String.format("not a float4: %s", param.getClass().getName()));
        }
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, ByteBuffer buf, int size) throws IOException {
        return buf.getFloat();
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

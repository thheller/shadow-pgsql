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
public class Int4 implements TypeHandler {

    private final int oid;
    private final String name;

    public Int4(int oid, String name) {
        this.oid = oid;
        this.name = name;
    }

    @Override
    public int getTypeOid() {
        return oid;
    }

    @Override
    public String getTypeName() {
        return name;
    }

    @Override
    public boolean supportsBinary() {
        return true;
    }

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        if (param instanceof Long) {
            output.int32(((Long) param).intValue());
        } else if (param instanceof Integer) {
            output.int32((Integer) param);
        } else if (param instanceof Short) {
            output.int32(((Short) param).intValue());
        } else {
            throw new IllegalArgumentException(String.format("Unsupported int4 type: %s", param.getClass().getName()));
        }
    }

    @Override
    public String encodeToString(Connection con, Object param) {
        return param.toString();
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, ByteBuffer buf, int colSize) throws IOException {
        return buf.getInt();
    }

    @Override
    public Object decodeString(Connection con, ColumnInfo field, String value) {
        return Integer.valueOf(value);
    }
}

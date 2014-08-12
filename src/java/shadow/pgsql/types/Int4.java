package shadow.pgsql.types;

import shadow.pgsql.Connection;
import shadow.pgsql.ColumnInfo;
import shadow.pgsql.ProtocolOutput;
import shadow.pgsql.TypeHandler;

import java.io.IOException;

/**
 * Created by zilence on 10.08.14.
 */
public class Int4 implements TypeHandler {
    public static final Int4 OID = new Int4(26);
    public static final Int4 INSTANCE = new Int4(23);

    private final int oid;

    Int4(int oid) {
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
            output.int32(((Long) param).intValue());
        } else if (param instanceof Integer) {
            output.int32((Integer) param);
        } else if (param instanceof Short) {
            output.int32(((Short) param).intValue());
        } else {
            throw new IllegalArgumentException(String.format("Unsupported int2 type: %s [%s]", param.getClass().getName(), param.toString()));
        }
    }

    @Override
    public String encodeToString(Connection con, Object param) {
        return param.toString();
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, int colSize) throws IOException {
        return con.input.readInt32();
    }

    @Override
    public Object decodeString(Connection con, ColumnInfo field, String value) {
        return Integer.valueOf(value);
    }
}

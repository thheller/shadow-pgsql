package shadow.pgsql.types;

import shadow.pgsql.Connection;
import shadow.pgsql.ColumnInfo;
import shadow.pgsql.ProtocolOutput;
import shadow.pgsql.TypeHandler;

import java.io.IOException;

/**
 * Created by zilence on 10.08.14.
 */
public class Int8 implements TypeHandler {
    public static final int OID = 20;
    public static final Int8 INSTANCE = new Int8();

    Int8() {
    }

    @Override
    public int getTypeOid() {
        return OID;
    }

    @Override
    public boolean supportsBinary() {
        return true;
    }

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        if (param instanceof Long) {
            output.int64(((Long) param).longValue());
        } else if (param instanceof Integer) {
            output.int64(((Integer) param).longValue());
        } else if (param instanceof Short) {
            output.int64(((Short) param).longValue());
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
        return con.input.stream.readLong();
    }

    @Override
    public Object decodeString(Connection con, ColumnInfo field, String value) {
        return Integer.valueOf(value);
    }
}

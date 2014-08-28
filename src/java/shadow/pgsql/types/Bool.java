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
public class Bool implements TypeHandler {

    @Override
    public int getTypeOid() {
        return 16;
    }


    @Override
    public String getTypeName() {
        return "bool";
    }

    @Override
    public boolean supportsBinary() {
        return true;
    }

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        if (!(param instanceof Boolean)) {
            throw new IllegalArgumentException(String.format("expected boolean, got: %s", param.getClass().getName()));
        }

        boolean b = (boolean) param;
        output.int8(b ? 1 : 0);
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, ByteBuffer buf, int size) throws IOException {
        return buf.get() != 0;
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

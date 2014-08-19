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
public class ByteA implements TypeHandler {

    @Override
    public int getTypeOid() {
        return 17;
    }

    @Override
    public boolean supportsBinary() {
        return true;
    }

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        if (param instanceof byte[]) {
            // FIXME: support ByteBuffer, InputStream?, ...
            output.write((byte[]) param);
        } else if (param instanceof ByteBuffer) {
            output.put((ByteBuffer) param);
        } else {
            throw new IllegalArgumentException(String.format("unsupported binary type: %s", param.getClass().getName()));
        }
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, ByteBuffer buf, int size) throws IOException {
        final byte[] bytes = new byte[size];
        buf.get(bytes);
        return bytes;
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

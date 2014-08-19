package shadow.pgsql.types;

import shadow.pgsql.ColumnInfo;
import shadow.pgsql.Connection;
import shadow.pgsql.ProtocolOutput;
import shadow.pgsql.TypeHandler;

import java.io.IOException;

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
        if (!(param instanceof byte[])) {
            // FIXME: support ByteBuffer, InputStream?, ...
            throw new IllegalArgumentException(String.format("unsupported binary type: %s", param.getClass().getName()));
        }
        output.write((byte[]) param);
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, int colSize) throws IOException {
        final byte[] bytes = new byte[colSize];
        con.input.readFully(bytes);
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

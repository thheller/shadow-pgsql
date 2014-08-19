package shadow.pgsql;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by zilence on 10.08.14.
 */
public interface TypeHandler {
    public int getTypeOid();

    public boolean supportsBinary();

    public void encodeBinary(Connection con, ProtocolOutput output, Object param);

    Object decodeBinary(Connection con, ColumnInfo field, ByteBuffer buf, int size) throws IOException;

    public String encodeToString(Connection con, Object param);

    Object decodeString(Connection con, ColumnInfo field, String value);
}

package shadow.pgsql;

import java.io.IOException;

/**
 * Created by zilence on 10.08.14.
 */
public interface TypeHandler {
    public int getTypeOid();

    public boolean supportsBinary();

    public void encodeBinary(Connection con, ProtocolOutput output, Object param);
    Object decodeBinary(Connection con, ColumnInfo field, int colSize) throws IOException;

    public String encodeToString(Connection con, Object param);
    Object decodeString(Connection con, ColumnInfo field, String value);
}

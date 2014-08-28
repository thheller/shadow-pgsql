package shadow.pgsql;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by zilence on 10.08.14.
 */
public interface TypeHandler {
    /**
     * if the type has a constant OID return it here
     *
     * should be most types, but extensions like hstore or custom types do not have constant OID
     *
     * @return positive int for OID, -1 to use type name
     */
    public int getTypeOid();

    /**
     * used if oid == -1
     *
     * @return
     */
    public String getTypeName();

    public boolean supportsBinary();

    public void encodeBinary(Connection con, ProtocolOutput output, Object param);

    Object decodeBinary(Connection con, ColumnInfo field, ByteBuffer buf, int size) throws IOException;

    public String encodeToString(Connection con, Object param);

    Object decodeString(Connection con, ColumnInfo field, String value);
}

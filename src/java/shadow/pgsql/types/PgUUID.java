package shadow.pgsql.types;

import shadow.pgsql.ColumnInfo;
import shadow.pgsql.Connection;
import shadow.pgsql.ProtocolOutput;
import shadow.pgsql.TypeHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by zilence on 05.09.14.
 */
public class PgUUID implements TypeHandler {

    @Override
    public int getTypeOid() {
        return 2950;
    }

    @Override
    public String getTypeName() {
        return "uuid";
    }

    @Override
    public boolean supportsBinary() {
        return true;
    }

    private UUID checkUUID(Object param) {
        if (param instanceof UUID) {
            return (UUID)param;
        } else {
            throw new IllegalArgumentException(String.format("not a uuid: %s [%s]", param.getClass().getName(), param));
        }
    }

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        final UUID uuid = checkUUID(param);
        output.int64(uuid.getMostSignificantBits());
        output.int64(uuid.getLeastSignificantBits());
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, ByteBuffer buf, int size) throws IOException {
        return new UUID(buf.getLong(), buf.getLong());
    }

    @Override
    public String encodeToString(Connection con, Object param) {
        return checkUUID(param).toString();
    }

    @Override
    public Object decodeString(Connection con, ColumnInfo field, String value) {
        return UUID.fromString(value);
    }
}

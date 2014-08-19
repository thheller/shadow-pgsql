package shadow.pgsql.types;

import shadow.pgsql.ColumnInfo;
import shadow.pgsql.Connection;
import shadow.pgsql.ProtocolOutput;
import shadow.pgsql.TypeHandler;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by zilence on 11.08.14.
 */
public class Text implements TypeHandler {
    public static interface Conversion {
        public String encode(Object param);

        public Object decode(String value);
    }

    public static Conversion AS_IS = new Conversion() {
        @Override
        public String encode(Object param) {
            if (!(param instanceof String)) {
                throw new IllegalArgumentException(String.format("not a string: %s", param.getClass().getName()));
            }
            return (String) param;
        }

        @Override
        public Object decode(String value) {
            return value;
        }
    };

    private final int oid;
    private final Conversion conversion;

    public Text(int oid) {
        this(oid, AS_IS);
    }

    public Text(int oid, Conversion conversion) {
        this.oid = oid;
        this.conversion = conversion;
    }

    public Text(Conversion conversion) {
        this(25, conversion); // 25 = text (default type)
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
        // FIXME: handle encoding properly, I assume everything is UTF-8 for now!
        output.bytea(conversion.encode(param).getBytes());
    }

    @Override
    public String encodeToString(Connection con, Object param) {
        return conversion.encode(param);
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, ByteBuffer buf, int size) throws IOException {
        // FIXME: is this the only way to get a string?
        byte[] bytes = new byte[size];
        buf.get(bytes);

        // FIXME: utf-8
        return conversion.decode(new String(bytes));
    }

    @Override
    public Object decodeString(Connection con, ColumnInfo field, String value) {
        return conversion.decode(value);
    }
}

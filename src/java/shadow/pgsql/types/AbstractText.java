package shadow.pgsql.types;


        import shadow.pgsql.Connection;
        import shadow.pgsql.ColumnInfo;
        import shadow.pgsql.ProtocolOutput;
        import shadow.pgsql.TypeHandler;

        import java.io.IOException;

/**
 * Created by zilence on 11.08.14.
 */
public abstract class AbstractText implements TypeHandler{
    @Override
    public boolean supportsBinary() {
        return true;
    }

    public abstract String asString(Object param);
    public abstract Object fromString(String input);

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        // FIXME: handle encoding properly, I assume everything is UTF-8 for now!
        output.bytea(asString(param).getBytes());
    }

    @Override
    public String encodeToString(Connection con, Object param) {
        return asString(param);
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, int size) throws IOException {
        byte[] bytes = new byte[size];
        con.input.read(bytes);
        return fromString(new String(bytes));
    }

    @Override
    public Object decodeString(Connection con, ColumnInfo field, String value) {
        return fromString(value);
    }
}


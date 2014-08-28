package shadow.pgsql.types;

import shadow.pgsql.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zilence on 28.08.14.
 */
public class HStore implements TypeHandler {

    public static interface Handler<ACC, RESULT> {
        public String keyToString(Object key);
        public String valueToString(Object value);

        public ACC init(int size);
        public ACC add(ACC state, String key, String value);
        public RESULT complete(ACC state);
    }

    private final Handler handler;

    public HStore() {
        this(new Handler<Map<String, String>, Map<String, String>>() {

            @Override
            public String keyToString(Object key) {
                return key.toString();
            }

            @Override
            public String valueToString(Object value) {
                return value.toString();
            }

            @Override
            public Map<String, String> init(int size) {
                return new HashMap<>(size);
            }

            @Override
            public Map<String, String> add(Map<String, String> state, String key, String value) {
                state.put(key, value);
                return state;
            }

            @Override
            public Map<String, String> complete(Map<String, String> state) {
                return state;
            }
        });
    }

    public HStore(Handler handler) {
        this.handler = handler;
    }

    @Override
    public int getTypeOid() {
        return -1;
    }

    @Override
    public String getTypeName() {
        return "hstore";
    }

    @Override
    public boolean supportsBinary() {
        return true;
    }

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        if (!(param instanceof Map)) {
            throw new IllegalArgumentException(String.format("need a map instance: %s", param.getClass().getName()));
        }

        Map m = (Map) param;
        output.int32(m.size());

        for (Object key : m.keySet()) {
            String skey = handler.keyToString(key);

            // FIXME: assumes utf-8
            output.byteaWithLength(skey.getBytes());

            Object val = m.get(key);

            if (val != null) {
                String sval = handler.valueToString(val);

                // FIXME: assumes utf-8
                output.byteaWithLength(sval.getBytes());
            } else {
               output.int32(-1);
            }
        }
    }

    private String string(ByteBuffer buf, int len) {
        byte[] bytes = new byte[len];
        buf.get(bytes);

        // FIXME: assumes utf-8
        return new String(bytes);
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, ByteBuffer buf, int size) throws IOException {
        int count = buf.getInt();

        Object state = handler.init(count);

        for (int i = 0; i < count; i++) {
            int keylen = buf.getInt();
            String key = string(buf, keylen);

            int valuelen = buf.getInt();
            String value;

            if (valuelen < 0) {
                value = null;
            } else {
                value = string(buf, valuelen);
            }

            state = handler.add(state, key, value);
        }

        return handler.complete(state);
    }

    @Override
    public String encodeToString(Connection con, Object param) {
        throw new UnsupportedOperationException("only binary supported");
    }

    @Override
    public Object decodeString(Connection con, ColumnInfo field, String value) {
        throw new UnsupportedOperationException("only binary supported");
    }
}

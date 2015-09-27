package shadow.pgsql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * Created by thheller on 09.08.14.
 */
public class ProtocolOutput {
    // FIXME: whats a good default size?
    // don't want too much resizing, its fine to reserve some memory
    public static final int BLOCK_SIZE = 16384;
    private ByteBuffer out = ByteBuffer.allocateDirect(BLOCK_SIZE);

    static class Mark {
        final int position;
        final int size;
        final boolean includeSize;

        Mark(int position, int size, boolean includeSize) {
            this.position = position;
            this.size = size;
            this.includeSize = includeSize;
        }
    }

    private final Stack<Mark> marks = new Stack<>();

    private final Connection pg;
    private final IO io;

    public ProtocolOutput(Connection pg, IO io) {
        this.pg = pg;
        this.io = io;
    }

    private void maybeGrow(int bytesComing) {
        if (this.out.remaining() < bytesComing) {
            ByteBuffer larger = ByteBuffer.allocateDirect(this.out.capacity() + Math.max(BLOCK_SIZE, bytesComing));

            out.flip();
            larger.put(out);

            this.out = larger;
        }
    }

    private void beginCommand(char type) {
        int8((byte) type);
        begin();
    }

    private void simpleCommand(char type) {
        int8((byte) type);
        int32(4);
    }

    /**
     * the protocol often requires the size of bytes to follow as an in32
     * mark the position and insert placeholder, complete will then rewind
     * and insert the accumulated size including the size of the placeholder itself
     */
    public void begin() {
        begin(4, true);
    }

    public void beginInclusive() {
        begin(4, true);
    }

    /**
     * same as begin, but the accumulated size will not include the size of
     * the placeholder itself
     */
    public void beginExclusive() {
        begin(4, false);
    }

    void begin(int size, boolean includeSize) {
        marks.push(new Mark(out.position(), size, includeSize));
        // write placeholder size, 4 bytes
        maybeGrow(size);
        out.putInt(0);
    }

    public void complete() {
        if (marks.empty()) {
            throw new IllegalStateException("no marks");
        }

        final int pos = out.position();
        final Mark mark = marks.pop();
        out.position(mark.position);
        final int size = pos - mark.position;
        int32(mark.includeSize ? size : (size - mark.size));
        out.position(pos);
    }

    // not public, only connection or query should call this
    void flushAndReset() throws IOException {
        if (!marks.empty()) {
            throw new IllegalStateException("marks not empty!");
        }

        out.flip();
        io.send(out);
        reset();
    }

    void reset() {
        this.marks.clear();
        this.out.clear();
    }

    public void int64(long val) {
        maybeGrow(8);
        out.putLong(val);
    }

    public void int32(int val) {
        maybeGrow(4);
        out.putInt(val);
    }

    public void int16(short val) {
        maybeGrow(2);
        out.putShort(val);
    }

    public void int8(int b) {
        maybeGrow(1);
        out.put((byte) b);
    }

    public void float4(float value) {
        maybeGrow(4);
        out.putFloat(value);
    }

    public void float8(double value) {
        maybeGrow(8);
        out.putDouble(value);
    }

    // aka empty string
    public void string() {
        nullTerminate();
    }

    public void string(String s) {
        if (s != null) {
            write(s.getBytes());
        }
        nullTerminate();
    }

    public void byteaWithLength(byte[] b) {
        int32(b.length);
        write(b);
    }

    public void bytea(byte[] b) {
        write(b);
    }

    public void nullTerminate() {
        int8((byte) 0);
    }

    public void write(byte[] in) {
        maybeGrow(in.length);
        out.put(in);
    }

    public void put(ByteBuffer in) {
        maybeGrow(in.remaining());
        out.put(in);
    }

    /**
     * sanity check that there is nothing pending
     * someone didn't flush if it is
     */
    void checkReset() {
        if (out.position() != 0) {
            throw new IllegalStateException(String.format("expected buffer position to be at 0 but is at %d", out.position()));
        }
    }

    void writeStartup(Map<String, String> opts) {
        begin();
        int32(196608); // 3.0
        for (String k : opts.keySet()) {
            string(k);
            string(opts.get(k));
        }
        string(); // empty string (aka null byte) means end
        complete();
    }

    void writeBind(TypeHandler[] paramEncoders, List<Object> queryParams, SQL sql, String statementId, String portalId, TypeHandler[] columnDecoders) {
        short[] formatCodes = new short[columnDecoders.length];
        for (int i = 0; i < formatCodes.length; i++) {
            formatCodes[i] = (short) (columnDecoders[i].supportsBinary() ? 1 : 0);
        }

        writeBind(paramEncoders, queryParams, sql, statementId, portalId, formatCodes);
    }

    void writeBind(TypeHandler[] paramEncoders, List<Object> queryParams, SQL sql, String statementId, String portalId, short[] formatCodes) {
        // Bind
        beginCommand('B');
        string(portalId); // portal name (might be null)
        string(statementId); // statement name (should not be null)

        // format codes for params
        int16((short) paramEncoders.length);
        for (TypeHandler t : paramEncoders) {
            int16((short) (t.supportsBinary() ? 1 : 0)); // format code 0 = text, 1 = binary
        }

        int16((short) paramEncoders.length);
        for (int i = 0; i < paramEncoders.length; i++) {
            TypeHandler encoder = paramEncoders[i];

            Object param = queryParams.get(i);

            try {
                if (param == null) {
                    int32(-1);
                } else if (encoder.supportsBinary()) {
                    beginExclusive();
                    encoder.encodeBinary(pg, this, param);
                    complete();
                } else {
                    String paramString = encoder.encodeToString(pg, param);

                    // FIXME: assumes UTF-8
                    byte[] bytes = paramString.getBytes();

                    int32(bytes.length);
                    if (bytes.length > 0) {
                        bytea(bytes);
                    }
                }
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Failed to encode parameter $%d%nvalue: \"%s\"%ntype: %s%nusing: %s%nsql: %s",
                                i + 1,
                                param,
                                param.getClass().getName(),
                                encoder.getClass().getName(),
                                sql.getSQLString()),
                        e);
            }
        }

        int16((short) formatCodes.length);
        for (short formatCode : formatCodes) {
            int16(formatCode);
        }

        complete();
    }

    void writeExecute(String portalId, int limit) {
        beginCommand('E');
        string(portalId); // portal name
        int32(limit); // max rows, zero = no limit
        complete();
    }

    void writeParse(String query, List<TypeHandler> typeHints, String statementId) {
        beginCommand('P');
        string(statementId);
        string(query);
        int16((short) typeHints.size());
        for (TypeHandler t : typeHints) {
            if (t == null) {
                int32(0);
            } else {
                int oid = t.getTypeOid();
                if (oid == -1) {
                    oid = pg.db.getOidForName(t.getTypeName());
                }
                int32(oid);
            }
        }
        complete();
    }

    void writeDescribePortal(String portal) {
        beginCommand('D');
        int8((byte) 'P');
        string(portal);
        complete();
    }

    void writeDescribeStatement(String statementId) {
        beginCommand('D');
        int8((byte) 'S');
        string(statementId);
        complete();
    }

    void writeSync() {
        simpleCommand('S');
    }

    void writeCloseStatement(String statementId) {
        beginCommand('C');
        int8((byte) 'S');
        string(statementId);
        complete();
    }

    void writeCloseConnection() {
        simpleCommand('X');
    }

    void writeSimpleQuery(String query) {
        beginCommand('Q');
        string(query);
        complete();
    }

}


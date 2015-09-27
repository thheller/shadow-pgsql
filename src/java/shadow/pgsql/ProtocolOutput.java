package shadow.pgsql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Created by thheller on 09.08.14.
 */
public class ProtocolOutput {
    // FIXME: whats a good default size?
    // don't want too much resizing, its fine to reserve some memory
    public static final int BLOCK_SIZE = 8192;
    private ByteBuffer out = ByteBuffer.allocateDirect(BLOCK_SIZE);

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

    private ProtocolMarker beginCommand(char type) {
        int8((byte) type);
        return begin();
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
    public ProtocolMarker begin() {
        return begin(4, true);
    }

    public ProtocolMarker beginInclusive() {
        return begin(4, true);
    }

    /**
     * same as begin, but the accumulated size will not include the size of
     * the placeholder itself
     */
    public ProtocolMarker beginExclusive() {
        return begin(4, false);
    }

    ProtocolMarker begin(int size, boolean includeSize) {
        final ProtocolMarker mark = new ProtocolMarker(this, out.position(), size, includeSize);
        // write placeholder size, 4 bytes
        maybeGrow(size);
        out.putInt(0);

        return mark;
    }

    void completeCommand(ProtocolMarker mark) {
        final int pos = out.position();
        out.position(mark.position);
        final int size = pos - mark.position;
        int32(mark.includeSize ? size : (size - mark.size));
        out.position(pos);
    }

    // not public, only connection or query should call this
    void flushAndReset() throws IOException {
        out.flip();
        io.send(out);
        reset();
    }

    void reset() {
        out.clear();
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
    public void cstring() {
        nullTerminate();
    }

    public void cstring(String s) {
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
        final ProtocolMarker mark = begin();
        int32(196608); // 3.0
        for (String k : opts.keySet()) {
            cstring(k);
            cstring(opts.get(k));
        }
        cstring(); // empty string (aka null byte) means end
        mark.complete();
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
        final ProtocolMarker mark = beginCommand('B');
        cstring(portalId); // portal name (might be null)
        cstring(statementId); // statement name (should not be null)

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
                    final ProtocolMarker pMark = beginExclusive();
                    encoder.encodeBinary(pg, this, param);
                    pMark.complete();
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

        mark.complete();
    }

    void writeExecute(String portalId, int limit) {
        final ProtocolMarker mark = beginCommand('E');
        cstring(portalId); // portal name
        int32(limit); // max rows, zero = no limit
        mark.complete();
    }

    void writeParse(String query, List<TypeHandler> typeHints, String statementId) {
        final ProtocolMarker mark = beginCommand('P');
        cstring(statementId);
        cstring(query);
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
        mark.complete();
    }

    void writeDescribePortal(String portal) {
        final ProtocolMarker mark = beginCommand('D');
        int8((byte) 'P');
        cstring(portal);
        mark.complete();
    }

    void writeDescribeStatement(String statementId) {
        final ProtocolMarker mark = beginCommand('D');
        int8((byte) 'S');
        cstring(statementId);
        mark.complete();
    }

    void writeSync() {
        simpleCommand('S');
    }

    void writeCloseStatement(String statementId) {
        final ProtocolMarker mark = beginCommand('C');
        int8((byte) 'S');
        cstring(statementId);
        mark.complete();
    }

    void writeCloseConnection() {
        simpleCommand('X');
    }

    void writeSimpleQuery(String query) {
        final ProtocolMarker mark = beginCommand('Q');
        cstring(query);
        mark.complete();
    }

}


package shadow.pgsql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
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

    private final Connection connection;
    private final SocketChannel channel;

    public ProtocolOutput(Connection con, SocketChannel channel) {
        this.connection = con;
        this.channel = channel;
    }

    private void maybeGrow(int bytesComing) {
        if (this.out.remaining() < bytesComing) {
            ByteBuffer larger = ByteBuffer.allocate(this.out.capacity() + BLOCK_SIZE);

            out.flip();
            larger.put(out);

            this.out = larger;
        }
    }

    void beginCommand(char type) {
        int8((byte) type);
        begin();
    }

    void simpleCommand(char type) {
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
        int written = channel.write(out);
        if (written != out.limit()) {
            // FIXME: loop
            throw new IllegalStateException("partial write");
        }
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
}


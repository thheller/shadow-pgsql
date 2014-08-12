package shadow.pgsql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.io.OutputStream;
import java.util.Stack;

/**
 * Created by thheller on 09.08.14.
 */
public class ProtocolOutput {
    // FIXME: whats a good default size?
    // don't want too much resizing, its fine to reserve some memory
    public static final int BLOCK_SIZE = 16384;
    private byte bytes[];
    private int position;

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
    private final OutputStream target;

    public ProtocolOutput(OutputStream target) {
        this.target = target;
        bytes = new byte[BLOCK_SIZE];
    }

    private void maybeGrow(int bytesComing) {
        if (this.position + bytesComing > bytes.length) {
            // FIXME: grows at least by BLOCK_SIZE, should maybe be more dynamic?
            bytes = Arrays.copyOf(bytes, bytes.length + Math.max(bytesComing, BLOCK_SIZE));
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
        marks.push(new Mark(position, size, includeSize));
        // write placeholder size, 4 bytes
        maybeGrow(size);
        this.position += size;
    }

    void complete() {
        if (marks.empty()) {
            throw new IllegalStateException("no marks");
        }

        final int pos = position;
        final Mark mark = marks.pop();
        this.position = mark.position;
        final int size = pos - position;
        int32(mark.includeSize ? size : (size - mark.size));
        this.position = pos;
    }

    // not public, only connection or query should call this
    void flushAndReset() throws IOException {
        if (!marks.empty()) {
            throw new IllegalStateException("marks not empty!");
        }

        target.write(bytes, 0, position);
        target.flush();

        reset();
    }

    void reset() {
        this.position = 0;
    }

    public void int64(long val) {
        maybeGrow(8);
        bytes[position] = (byte) (val >>> 52);
        bytes[position + 1] = (byte) (val >>> 48);
        bytes[position + 2] = (byte) (val >>> 40);
        bytes[position + 3] = (byte) (val >>> 32);
        bytes[position + 4] = (byte) (val >>> 24);
        bytes[position + 5] = (byte) (val >>> 16);
        bytes[position + 6] = (byte) (val >>> 8);
        bytes[position + 7] = (byte) (val);

        this.position += 8;
    }

    public void int32(int val) {
        maybeGrow(4);
        bytes[position] = (byte) (val >>> 24);
        bytes[position + 1] = (byte) (val >>> 16);
        bytes[position + 2] = (byte) (val >>> 8);
        bytes[position + 3] = (byte) (val);

        this.position += 4;
    }

    public void int16(short val) {
        maybeGrow(2);
        bytes[position] = (byte) (val >>> 8);
        bytes[position + 1] = (byte) val;

        this.position += 2;
    }

    public void int8(int b) {
        maybeGrow(1);
        bytes[position] = (byte) b;
        position += 1;
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
        write(in, 0, in.length);
    }

    public void write(byte[] in, int offset, int length) {
        // not sure I need this check
        if (offset < 0 || offset > in.length || length < 0 || (offset + length) - in.length > 0) {
            throw new IndexOutOfBoundsException();
        }

        maybeGrow(length);
        System.arraycopy(in, offset, bytes, position, length);
        position += length;
    }


    /**
     * sanity check that there is nothing pending
     * someone didn't flush if it is
     */
    void checkReset() {
        if (position != 0) {
            throw new IllegalStateException(String.format("expected buffer position to be at 0 but is at %d", position));
        }
    }

    void close() throws IOException {
        target.close();
    }
}


package shadow.pgsql;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Created by zilence on 19.09.15.
 */
public class StreamIO implements IO {

    private final Socket socket;
    private BufferedOutputStream out;
    private BufferedInputStream in;

    private final Frame frame = new Frame();
    private final ByteBuffer recvBuffer = ByteBuffer.allocate(65536);

    public StreamIO(Socket socket) throws IOException {
        this.socket = socket;
        this.out = new BufferedOutputStream(socket.getOutputStream());
        this.in = new BufferedInputStream(socket.getInputStream());
    }


    static class Frame implements ProtocolFrame {
        char type;
        int size;
        ByteBuffer buffer;

        @Override
        public char getType() {
            return type;
        }

        @Override
        public int getSize() {
            return size;
        }

        @Override
        public ByteBuffer getBuffer() {
            return buffer;
        }
    }

    @Override
    public void send(ByteBuffer buf) throws IOException {
        // will usually use direct bytebuffers which may not have arrays
        if (buf.hasArray()) {
            out.write(buf.array(), buf.position(), buf.remaining());
        } else {
            while (buf.hasRemaining()) {
                // BufferedOutputStream.write is synchronized, might not be best to call it for every byte
                out.write(buf.get());
            }
        }
        out.flush();
    }

    private char readChar() throws IOException {
        int c = in.read();
        if (c < 0) {
            throw new EOFException();
        }

        return (char) c;
    }

    private final byte[] int4buf = new byte[4];

    private int readInt() throws IOException {
        if (in.read(int4buf) != 4) {
            throw new EOFException();
        }

        return (int4buf[0] & 0xFF) << 24 |
                (int4buf[1] & 0xFF) << 16 |
                (int4buf[2] & 0xFF) << 8 |
                (int4buf[3] & 0xFF);
    }

    @Override
    public ProtocolFrame nextFrame() throws IOException {
        frame.type = readChar();
        frame.size = readInt() - 4;

        ByteBuffer buf;
        if (frame.size > recvBuffer.capacity()) {
            buf = ByteBuffer.allocate(frame.size);
        } else {
            buf = recvBuffer;
        }

        buf.position(0);
        buf.limit(frame.size);

        int remaining = frame.size;
        int pos = 0;

        while (remaining > 0) {
            int read = in.read(buf.array(), pos, remaining);
            if (read < 0) {
                throw new EOFException();
            }

            pos += read;
            remaining -= read;
        }

        frame.buffer = buf;

        return frame;
    }

    @Override
    public void close() throws IOException {
        this.out.close();
        this.in.close();
        this.socket.close();
    }
}

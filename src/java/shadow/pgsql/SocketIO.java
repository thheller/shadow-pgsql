package shadow.pgsql;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Created by zilence on 23.08.14.
 */
public class SocketIO implements IO {
    private final SocketChannel channel;

    private final ByteBuffer recvBuffer;

    // 8192 is default buffer size of TCP/BufferedInputStream
    // making it bigger does not seem to provide any benefit
    // private static final int BUFFER_SIZE = 8192;
    private static final int BUFFER_SIZE = 65536;

    private char previousFrame;
    private int nextPosition;
    private int nextLimit;

    public SocketIO(SocketChannel channel) {
        this.channel = channel;

        this.recvBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        this.recvBuffer.flip();
        this.previousFrame = 0;
        this.nextPosition = 0;
        this.nextLimit = 0;
    }

    @Override
    public void send(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            if (channel.write(buf) < 0) {
                throw new EOFException();
            }
        }
    }

    public Frame nextFrame() throws IOException {
        if (recvBuffer.hasRemaining()) {
            throw new IllegalStateException("previous frame did not consume all bytes");
        }

        recvBuffer.position(nextPosition);
        recvBuffer.limit(nextLimit);

        if (recvBuffer.remaining() < 5)  {
            recvBuffer.compact();
            if (channel.read(recvBuffer) < 5) {
                throw new IllegalStateException("didnt refill enough for frame header");
            }
            recvBuffer.flip();
        }

        char type = previousFrame = (char) recvBuffer.get();
        final int size = recvBuffer.getInt() - 4;
        ByteBuffer buf;

        if (recvBuffer.remaining() >= size) {
            // next frame already completely available
            final int pos = recvBuffer.position();
            nextPosition = pos + size;
            nextLimit = recvBuffer.limit();

            recvBuffer.limit(pos + size);
            buf = recvBuffer;

        } else if (size > recvBuffer.capacity()) {
            // next frame too big to fit default buffer
            // FIXME: could also resize recvBuffer

            buf = ByteBuffer.allocateDirect(size);
            buf.put(recvBuffer);
            while (buf.hasRemaining()) {
                int read = channel.read(buf);
                if (read < 0){
                    throw new EOFException();
                }
            }
            buf.flip();

            nextPosition = 0;
            nextLimit = 0;
        } else {
            // not enough stuff available, read more

            recvBuffer.compact();
            while (recvBuffer.position() < size) {
                int read = channel.read(recvBuffer);
                if (read < 0){
                    throw new EOFException();
                }
            }

            int pos = recvBuffer.position();

            recvBuffer.position(0);
            recvBuffer.limit(size);

            buf = recvBuffer;

            nextPosition = size;
            nextLimit = pos;
        }

        if (buf.remaining() != size) {
            throw new IllegalStateException("protocol error, did not properly set up buf");
        }

        return new IO.Frame(type, size, buf);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}

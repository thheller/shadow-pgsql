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

    public SocketIO(SocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void send(ByteBuffer buf) throws IOException {
        int written = channel.write(buf);
        if (written != buf.limit()) {
            // FIXME: loop
            throw new IllegalStateException("partial write");
        }
    }

    @Override
    public void recv(ByteBuffer buf) throws IOException {
        int size = buf.limit();

        int read = 0;
        do {
            int x = channel.read(buf);
            if (x == -1) {
                throw new EOFException();
            }
            read += x;
        } while (read < size);

        if (read != size) {
            throw new IllegalStateException("partial buffer");
        }

        buf.flip();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}

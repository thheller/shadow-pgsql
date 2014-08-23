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
        while (buf.hasRemaining()) {
            if (channel.write(buf) < 0) {
                throw new EOFException();
            }
        }
    }

    @Override
    public void recv(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            if (channel.read(buf) < 0) {
                throw new EOFException();
            }
        }
        buf.flip();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}

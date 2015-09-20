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
    private static final int BUFFER_SIZE = 8192;

    public SocketIO(SocketChannel channel) {
        this.channel = channel;

        this.recvBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        this.recvBuffer.flip();
    }

    @Override
    public void send(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            if (channel.write(buf) < 0) {
                throw new EOFException();
            }
        }
    }

    private void refill() throws IOException {
        if (recvBuffer.hasRemaining()) {
            throw new IllegalStateException("recvBuffer not empty");
        }
        recvBuffer.clear();
        int read = channel.read(recvBuffer);
        if (read < 0) {
            throw new EOFException();
        }
        recvBuffer.flip();
    }

    private void transfer(ByteBuffer dst) {
        int n = Math.min(recvBuffer.remaining(), dst.remaining());
        for (int i = 0; i < n; i++) {
            dst.put(recvBuffer.get());
        }
    }

    @Override
    public void recv(ByteBuffer buf) throws IOException {

        while (true) {
            transfer(buf);

            int remaining = buf.remaining();
            if (remaining > 0) {
                // read directly into target if we need more than recvBuffer can take
                if (remaining > recvBuffer.capacity()) {
                    if (channel.read(buf) < 0) {
                        throw new EOFException();
                    }
                } else {
                    refill();
                }
            } else {
                break;
            }
        }

        buf.flip();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}

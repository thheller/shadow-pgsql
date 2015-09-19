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

    public StreamIO(Socket socket) throws IOException {
        this.socket = socket;
        this.out = new BufferedOutputStream(socket.getOutputStream());
        this.in = new BufferedInputStream(socket.getInputStream());
    }

    @Override
    public void send(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            out.write(buf.get());
        }
        out.flush();
    }

    @Override
    public void recv(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int read = in.read();
            if (read == -1) {
                throw new EOFException();
            }
            buf.put((byte) read);
        }

        buf.flip();
    }

    @Override
    public void close() throws IOException {
        this.out.close();
        this.in.close();
        this.socket.close();
    }
}

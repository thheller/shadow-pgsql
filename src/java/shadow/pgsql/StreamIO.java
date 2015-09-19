package shadow.pgsql;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Created by zilence on 19.09.15.
 */
public class StreamIO implements IO {

    private final Socket socket;
    private BufferedOutputStream out;
    private InputStream in;

    private static final int CHUNK_SIZE = 8192;
    private final byte[] chunk = new byte[CHUNK_SIZE];

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
            // reading in chunks is about twice as fast as read/put each byte
            // still requires BufferedInputStream, about 5 times slower without
            // don't know why BufferedInputStream is so much faster since we are basically doing the same thing again?
            // FIXME: WTF?
            int bytesToRead = Math.min(buf.remaining(), CHUNK_SIZE);
            int bytesRead = in.read(chunk, 0, bytesToRead);

            if (bytesRead == -1) {
                throw new EOFException();
            }
            buf.put(chunk, 0, bytesRead);
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

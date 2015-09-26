package shadow.pgsql;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLSession;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Created by zilence on 23.08.14.
 */
public class SSLSocketIO implements IO {
    private final SocketChannel channel;
    private final SSLEngine ssl;

    private ByteBuffer sslIn;
    private ByteBuffer in;

    private ByteBuffer sslOut;
    private ByteBuffer out;

    public SSLSocketIO(SocketChannel channel, SSLEngine ssl) {
        this.channel = channel;
        this.ssl = ssl;

        SSLSession session = ssl.getSession();

        this.sslIn = ByteBuffer.allocateDirect(session.getPacketBufferSize());
        this.in = ByteBuffer.allocateDirect(session.getApplicationBufferSize());

        this.sslOut = ByteBuffer.allocateDirect(session.getPacketBufferSize());
        this.out = ByteBuffer.allocateDirect(session.getApplicationBufferSize());
    }


    public void handshake() throws IOException {

        ssl.beginHandshake();

        SSLEngineResult res;

        boolean firstUnwrap = true;

        HANDSHAKE:
        while (true) {
            SSLEngineResult.HandshakeStatus hs = ssl.getHandshakeStatus();

            switch (hs) {
                case FINISHED:
                    break HANDSHAKE;

                case NOT_HANDSHAKING:
                    break HANDSHAKE;

                case NEED_UNWRAP: {
                    // FIXME: the handshake is clearly not made for blocking operation
                    // what to do if sslIn has remaining bytes but not enough to unwrap?
                    // probably try ssl.unwrap first, on BUFFER_UNDERFLOW, read more?
                    // FIXME: this is pretty unstable code! assumes we received everything we need in one go
                    // which is what postgres does most of the time I guess
                    if (firstUnwrap || !sslIn.hasRemaining()) {
                        sslIn.clear();
                        if (channel.read(sslIn) < 0) {
                            throw new IllegalStateException("eof while handshaking");
                        }

                        sslIn.flip();

                        firstUnwrap = false;
                    }

                    res = ssl.unwrap(sslIn, in);
                    if (res.getStatus() != SSLEngineResult.Status.OK) {
                        throw new IllegalStateException("not ok?");
                    }

                    break;
                }
                case NEED_WRAP: {
                    res = ssl.wrap(out, sslOut);

                    if (res.getStatus() != SSLEngineResult.Status.OK) {
                        throw new IllegalStateException("not ok?");
                    }

                    sslOut.flip();

                    while (sslOut.hasRemaining()) {
                        if (channel.write(sslOut) < 0) {
                            throw new IllegalStateException("eof while handshaking");
                        }
                    }

                    sslOut.clear();
                    break;
                }
                case NEED_TASK: {
                    ssl.getDelegatedTask().run();
                    break;
                }
                default:
                    throw new IllegalStateException(String.format("handshake: %s", hs));

            }
        }

        in.clear();
        in.flip();

        out.clear();

        sslIn.clear();
        sslIn.flip();

        sslOut.clear();
    }

    void putLimit(ByteBuffer src, ByteBuffer dest) {
        int available = src.remaining();
        int needed = dest.remaining();
        int limit = src.limit();

        if (available > needed) {
            src.limit(src.position() + needed);
        }

        dest.put(src);

        src.limit(limit);
    }

    @Override
    public void send(ByteBuffer buf) throws IOException {

        while (buf.hasRemaining()) {
            SSLEngineResult result = ssl.wrap(buf, sslOut);

            if (result.getStatus() != SSLEngineResult.Status.OK) {
                throw new IllegalStateException("ssl not ok?");
            }

            sslOut.flip();

            while (sslOut.hasRemaining()) {
                if (channel.write(sslOut) < 0) {
                    throw new EOFException();
                }
            }

            sslOut.clear();
        }
    }


    public ProtocolFrame nextFrame() {
        throw new IllegalStateException("ssl not up-to-date");
    }

    public void recv(ByteBuffer buf) throws IOException {
        // FIXME: this is so ugly, seems to work but is no way ideal
        // buffer underflow makes this weird
        // sometimes sslIn contain enough for 2 unwraps, yet calling unwrap does not unwrap all bytes
        // sometimes we just don't have enough bytes
        // impossible to tell before calling unwrap
        // there should only be one place we call channel.read

        while (buf.hasRemaining()) {
            // we need more bytes

            if (in.hasRemaining()) {
                // have some
                putLimit(in, buf);
            } else if (sslIn.hasRemaining()) {
                in.clear();

                SSLEngineResult result = ssl.unwrap(sslIn, in);

                switch (result.getStatus()) {
                    case OK:
                        sslIn.compact();
                        sslIn.flip();
                        break;
                    case BUFFER_UNDERFLOW:
                        sslIn.position(sslIn.limit());
                        sslIn.limit(sslIn.capacity());

                        if (channel.read(sslIn) < 0) {
                            throw new EOFException();
                        }
                        sslIn.flip();
                        break;
                    default:
                        throw new IllegalStateException("how do you get into a buffer overflow?");
                }

                in.flip();
            } else {
                in.clear();

                sslIn.clear();
                channel.read(sslIn);
                sslIn.flip();

                SSLEngineResult result = ssl.unwrap(sslIn, in);

                if (result.getStatus() != SSLEngineResult.Status.OK) {
                    throw new IllegalStateException("ssl not ok?");
                }

                in.flip();
            }
        }

        buf.flip();
    }

    @Override
    public void close() throws IOException {
        // FIXME: proper ssl shutdown
        channel.close();
    }

    public static SSLSocketIO start(SocketChannel channel, SSLContext context, String host, int port) throws IOException {
        SSLEngine engine = context.createSSLEngine(host, port);

        engine.setUseClientMode(true);

        SSLSocketIO io = new SSLSocketIO(channel, engine);
        io.handshake();
        return io;
    }
}

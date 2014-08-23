package shadow.pgsql;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLSession;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;

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

        HANDSHAKE:
        while (true) {
            SSLEngineResult.HandshakeStatus hs = ssl.getHandshakeStatus();

            switch (hs) {
                case FINISHED:
                    break HANDSHAKE;

                case NOT_HANDSHAKING:
                    break HANDSHAKE;

                case NEED_UNWRAP: {
                    // FIXME: blocking here after ServerHello, can't figure out why, server is supposed to send something
                    if (channel.read(sslIn) < 0) {
                        throw new IllegalStateException("eof while handshaking");
                    }

                    sslIn.flip();

                    res = ssl.unwrap(sslIn, in);
                    sslIn.compact();

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

        System.out.println("yo");
    }

    @Override
    public void send(ByteBuffer buf) throws IOException {

        while (buf.hasRemaining()) {
            out.put(buf);
            out.flip();

            ssl.wrap(out, sslOut);

            while (sslOut.hasRemaining()) {
                if (channel.write(sslOut) < 0) {
                    throw new EOFException();
                }
            }
        }

        System.out.println("yo");
    }

    @Override
    public void recv(ByteBuffer buf) throws IOException {
        throw new UnsupportedOperationException("recv");
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("close");
    }

    public static SSLSocketIO start(SocketChannel channel, String host, int port) throws IOException, NoSuchAlgorithmException {
        SSLContext context = SSLContext.getDefault();

        SSLEngine engine = context.createSSLEngine(host, port);

        engine.setUseClientMode(true);
        engine.setWantClientAuth(false);
        engine.setNeedClientAuth(false);

        SSLSocketIO io = new SSLSocketIO(channel, engine);
        io.handshake();
        return io;
    }
}

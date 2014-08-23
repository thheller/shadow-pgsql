package shadow.pgsql;

import org.junit.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.security.*;
import java.security.cert.CertificateException;

/**
 * Created by zilence on 19.08.14.
 */
public class StressTest {

    @Test
    public void testConnect() throws IOException {
        for (int i = 0; i < 500; i++) {
            // Database.setup("localhost", 5432, "zilence", "cms");
        }
    }
}

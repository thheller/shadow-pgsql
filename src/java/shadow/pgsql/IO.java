package shadow.pgsql;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by zilence on 23.08.14.
 */
public interface IO extends Closeable {
    void send(ByteBuffer buf) throws IOException;

    void recv(ByteBuffer buf) throws IOException;
}

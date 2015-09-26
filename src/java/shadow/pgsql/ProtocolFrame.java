package shadow.pgsql;

import java.nio.ByteBuffer;

/**
 * Created by zilence on 26.09.15.
 */
public interface ProtocolFrame {
    char getType();
    int getSize();
    ByteBuffer getBuffer();
}

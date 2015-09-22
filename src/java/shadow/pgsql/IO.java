package shadow.pgsql;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by zilence on 23.08.14.
 */
public interface IO extends Closeable {
    Frame nextFrame() throws IOException;

    void send(ByteBuffer buf) throws IOException;

    class Frame {
        public final char type;
        public final int size;
        public final ByteBuffer buffer;

        public Frame(char type, int size, ByteBuffer buffer) {
            this.type = type;
            this.size = size;
            this.buffer = buffer;
        }
    }
}

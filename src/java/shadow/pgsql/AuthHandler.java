package shadow.pgsql;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by zilence on 10.08.14.
 */
public interface AuthHandler {
    /**
     * should handle the Authentication method requested by the backend
     * <p/>
     * may require some network traffic, should return once the negotiation is complete
     * and the backend is ready to proceed normally
     * <p/>
     * should throw when authentication is not supported or possible
     *
     * @param con
     * @param type
     * @param buf extra bytes send by the backend, each type contains it own set of bytes
     *            handle accordingly
     * @link http://www.postgresql.org/docs/9.2/static/protocol-message-formats.html
     */
    public void doAuth(Connection con, int type, ByteBuffer buf) throws IOException;
}

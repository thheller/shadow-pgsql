package shadow.pgsql;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zilence on 10.08.14.
 */
public class ProtocolInput {
    // FIXME: find a good default buffer size, this may be too much but shouldn't hurt.
    // FIXME: make configurable
    private static final int BUFFER_SIZE = 65536;

    private final Connection pg;
    private final IO io;

    private final ByteBuffer frame = ByteBuffer.allocateDirect(5); // 1 byte command, 4 byte size
    private final ByteBuffer defaultBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);

    public ByteBuffer current = defaultBuffer;
    public int currentSize = 0;

    public ProtocolInput(Connection pg, IO io) {
        this.pg = pg;
        this.io = io;
    }

    String readString() throws IOException {
        StringBuffer sbuf = new StringBuffer();
        byte b;
        while (true) {
            b = current.get();
            if (b == 0) {
                return sbuf.toString();
            } else {
                sbuf.append((char) b);
            }
        }
    }

    Map<String, String> readMessages() throws IOException {
        final Map<String, String> errorData = new HashMap<>();

        while (true) {
            final byte fieldType = current.get();
            if (fieldType == 0) {
                return errorData;
            } else {
                errorData.put(String.valueOf((char) fieldType), readString());
            }
        }
    }

    /**
     * reads next command byte
     * <p/>
     * may read "over" NOTICE, NOTIFY, since they may appear at any time but are not protocol relevant
     *
     * @return
     * @throws IOException
     * @link http://www.postgresql.org/docs/9.3/static/protocol-message-formats.html
     */
    char readNextCommand() throws IOException {
        if (pg.state == ConnectionState.ERROR) {
            throw new IllegalStateException("In error state, figure out how to recover properly");
        }

        while (true) {
            frame.clear();

            io.recv(frame);

            final char type = (char) frame.get();
            final int size = currentSize = frame.getInt() - 4; // size includes itself

            // FIXME: worth skipping?
            if (size == 0) {
                this.current = null;
                return type;
            } else {
                if (size > BUFFER_SIZE) {
                    // oversized packet, allocate new buffer
                    // FIXME: is this really the best strategy here? at least make it configurable
                    current = ByteBuffer.allocateDirect(size);
                } else {
                    // otherwise use a default, keeps allocations down to a minimum
                    current = defaultBuffer;
                    current.clear();
                    current.limit(size);
                }

                io.recv(current);

                // FIXME: current.asReadyOnlyBuffer() ?

                switch (type) {
                    case 'N': // NoticeResponse
                    {
                        pg.handleNotice(readMessages());
                        break;
                    }
                    case 'A': // NotificationResponse
                    {
                        final int processId = current.getInt();
                        final String channel = readString();
                        final String payload = readString();
                        pg.handleNotify(processId, channel, payload);
                        break;
                    }
                    default:
                        return type;
                }
            }
        }
    }

    void readReadyForQuery() throws IOException {
        checkSize("ReadyForQuery", 1);

        final char backendState = (char) current.get();
        switch (backendState) {
            case 'I':
                pg.txState = TransactionStatus.IDLE;
                break;
            case 'E':
                pg.txState = TransactionStatus.FAILED;
                break;
            case 'T':
                pg.txState = TransactionStatus.TRANSACTION;
                break;
            default:
                throw new IllegalStateException(String.format("unknown transaction state: %s", backendState));

        }

        pg.state = ConnectionState.READY;
    }

    ColumnInfo[] readRowDescription() throws IOException {
        final int fields = getShort();

        ColumnInfo[] columnInfos = new ColumnInfo[fields];

        for (int i = 0; i < fields; i++) {
            String name = readString();
            int tableOid = current.getInt();
            int tablePos = current.getShort();
            int typeOid = current.getInt();
            int typeSize = current.getShort();
            int typeMod = current.getInt();
            int format = current.getShort(); // unreliable info, we might overwrite it, should not be needed by anyone anyway

            columnInfos[i] = new ColumnInfo(name, tableOid, tablePos, typeOid, typeSize, typeMod);
        }

        return columnInfos;
    }

    int[] readParameterDescription() throws IOException {
        final int count = getShort();

        final int[] oids = new int[count];
        for (int i = 0; i < count; i++) {
            oids[i] = current.getInt();
        }
        return oids;
    }

    public int getInt() throws IOException {
        return current.getInt();
    }

    public int getShort() throws IOException {
        return current.getShort();
    }

    public void getBytes(byte[] data) throws IOException {
        current.get(data);
    }

    public StatementResult readStatementResult(String sql) throws IOException {
        StatementResult result = null;

        Map<String, String> errorData = null;

        RESULT_LOOP:
        while (true) {
            final char type = readNextCommand();

            switch (type) {
                case '2': // BindComplete
                {
                    checkSize("BindComplete", 0);
                    break;
                }
                case 'C': { // CommandComplete
                    final String tag = readString();

                    result = new StatementResult(tag);
                    break;
                }
                case 'Z': {
                    pg.input.readReadyForQuery();
                    break RESULT_LOOP;
                }
                case 'E': {
                    errorData = pg.input.readMessages();
                    break;
                }
                default: {
                    throw new IllegalStateException(String.format("invalid protocol action while reading statement results: '%s'", type));
                }
            }
        }

        if (errorData != null) {
            throw new CommandException(String.format("Failed to bind Statement\n[sql]: %s", sql), errorData);
        }

        return result;
    }

    public void checkSize(String message, int expected) {
        if (currentSize != expected) {
            throw new IllegalStateException(String.format("%s was not size %d (was %d)", message, expected, currentSize));
        }
    }

    public int getCurrentSize() {
        return currentSize;
    }
}

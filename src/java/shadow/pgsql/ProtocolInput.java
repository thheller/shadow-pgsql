package shadow.pgsql;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zilence on 10.08.14.
 */
public class ProtocolInput {
    private final Connection pg;
    public final DataInputStream stream;

    public ProtocolInput(Connection pg, DataInputStream stream) {
        this.pg = pg;
        this.stream = stream;
    }

    public DataInputStream getStream() {
        return stream;
    }

    String readString() throws IOException {
        StringBuffer sbuf = new StringBuffer();
        byte b;
        while (true) {
            b = stream.readByte();
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
            final byte fieldType = stream.readByte();
            if (fieldType == 0) {
                return errorData;
            } else {
                errorData.put(String.valueOf((char) fieldType), readString());
            }
        }
    }

    CommandException readErrorAndMakeException(String causeByQuery) throws IOException {
        final int size = stream.readInt();

        final Map<String, String> errorData = readMessages();

        pg.state = ConnectionState.ERROR;

        return new CommandException(causeByQuery, errorData);
    }

    //

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
            final char type = (char) stream.readByte();

            switch (type) {
                case 'N': // NoticeResponse
                {
                    final int size = stream.readInt();
                    pg.handleNotice(readMessages());
                    break;
                }
                case 'A': // NotificationResponse
                {
                    final int size = stream.readInt();

                    final int processId = stream.readInt();
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

    void readReadyForQuery() throws IOException {
        final int size = readInt32();
        if (size != 5) {
            throw new IllegalStateException(String.format("ReadyForQuery was not size 5 (was %d)", size));
        }

        final char backendState = (char) stream.readByte();
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
        final int size = readInt32();
        final int fields = readInt16();

        ColumnInfo[] columnInfos = new ColumnInfo[fields];

        for (int i = 0; i < fields; i++) {
            String name = readString();
            int tableOid = stream.readInt();
            int tablePos = stream.readShort();
            int typeOid = stream.readInt();
            int typeSize = stream.readShort();
            int typeMod = stream.readInt();
            int format = stream.readShort(); // unreliable info, we might overwrite it, should not be needed by anyone anyway

            columnInfos[i] = new ColumnInfo(name, tableOid, tablePos, typeOid, typeSize, typeMod);
        }

        return columnInfos;
    }

    int[] readParameterDescription() throws IOException {
        final int size = readInt32();
        final int count = readInt16();

        final int[] oids = new int[count];
        for (int i = 0; i < count; i++) {
            oids[i] = stream.readInt();
        }
        return oids;
    }


    public int readInt32() throws IOException {
        return stream.readInt();
    }

    public int readInt16() throws IOException {
        return stream.readShort();
    }

    public void close() throws IOException {
        stream.close();
    }

    public void read(byte[] data) throws IOException {
        stream.read(data);
    }
}

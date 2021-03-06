package shadow.pgsql;

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

    private final Connection pg;
    private final IO io;

    public ByteBuffer current;
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
            final ProtocolFrame frame = io.nextFrame();

            currentSize = frame.getSize();
            current = frame.getBuffer();

            switch (frame.getType()) {
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
                    return frame.getType();
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


    Object readRow(final TypeHandler[] typeDecoders, final ColumnInfo[] columnInfos, final RowBuilder rowBuilder) throws IOException {
        final int cols = getShort();

        if (cols != columnInfos.length) {
            throw new IllegalStateException(
                    String.format("backend said to expect %d columns, but data had %d", columnInfos.length, cols)
            );
        }

        Object row = rowBuilder.init();

        for (int i = 0; i < cols; i++) {
            final ColumnInfo field = columnInfos[i];
            final TypeHandler decoder = typeDecoders[i];
            final int colSize = getInt();

            Object columnValue = null;

            if (colSize != -1) {
                columnValue = readColumnValue(field, decoder, colSize);
            }

            row = rowBuilder.add(row, field, i, columnValue);
        }

        return rowBuilder.complete(row);
    }

    Object readColumnValue(final ColumnInfo field, final TypeHandler decoder, int colSize) throws IOException {
        try {
            Object columnValue;

             if (decoder.supportsBinary()) {
                int mark = current.position();

                columnValue = decoder.decodeBinary(pg, field, current, colSize);

                if (current.position() != mark + colSize) {
                    throw new IllegalStateException(String.format("Field:[%s ,%s] did not consume all bytes", field.name, decoder));
                }
            } else {
                byte[] bytes = new byte[colSize];
                getBytes(bytes);

                // FIXME: assumes UTF-8
                final String stringValue = new String(bytes, 0, colSize, "UTF-8");
                columnValue = decoder.decodeString(pg, field, stringValue);
            }

            return columnValue;
        } catch (Exception e) {
            // FIXME: turns ALL Exceptions into IllegalStateException?
            throw new IllegalStateException(
                    String.format("Failed parsing field \"%s\" of table \"%s\"",
                            field.name,
                            field.tableOid > 0 ? pg.db.oid2name.get(field.tableOid) : "--unknown--"
                    ), e);
        }
    }

    public void skipFrame() {
        current.position(current.limit());
    }
}

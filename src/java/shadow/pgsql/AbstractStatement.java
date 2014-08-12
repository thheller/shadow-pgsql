package shadow.pgsql;

import java.io.IOException;
import java.util.List;

/**
 * Created by zilence on 09.08.14.
 */
public abstract class AbstractStatement implements AutoCloseable {
    protected final Connection pg;
    protected final String statementId;

    protected final TypeHandler[] typeEncoders;

    protected AbstractStatement(Connection pg, String statementId, TypeHandler[] typeEncoders) {
        this.pg = pg;
        this.statementId = statementId;
        this.typeEncoders = typeEncoders;
    }

    protected void writeBind(TypeHandler[] typeDecoders, List<Object> queryParams, String portalId) {
        // Bind
        pg.output.beginCommand('B');
        pg.output.string(portalId); // portal name (might be null)
        pg.output.string(statementId); // statement name (should not be null)

        // format codes for params
        pg.output.int16((short) typeEncoders.length);
        for (TypeHandler t : typeEncoders) {
            pg.output.int16((short) (t.supportsBinary() ? 1 : 0)); // format code 0 = text, 1 = binary
        }

        pg.output.int16((short) typeEncoders.length);
        for (int i = 0; i < typeEncoders.length; i++) {
            TypeHandler encoder = typeEncoders[i];

            Object param = queryParams.get(i);

            if (param == null) {
                pg.output.int32(-1);
            } else if (encoder.supportsBinary()) {
                pg.output.beginExclusive();
                encoder.encodeBinary(pg, pg.output, param);
                pg.output.complete();
            } else {
                String paramString = encoder.encodeToString(pg, param);

                // FIXME: assumes UTF-8
                byte[] bytes = paramString.getBytes();

                pg.output.int32(bytes.length);
                if (bytes.length > 0) {
                    pg.output.bytea(bytes);
                }
            }
        }

        pg.output.int16((short) typeDecoders.length);
        for (TypeHandler t : typeDecoders) {
            pg.output.int16((short) (t.supportsBinary() ? 1 : 0));
        }

        pg.output.complete();
    }

    protected void writeExecute(String portalId, int limit) {
        pg.output.beginCommand('E');
        pg.output.string(portalId); // portal name
        pg.output.int32(limit); // max rows, zero = no limit
        pg.output.complete();
    }

    protected void writeFlush() {
        // Flush
        pg.output.simpleCommand('H');
    }

    protected void writeSync() {
        // Sync
        pg.output.simpleCommand('S');
    }

    protected void doSync() throws IOException {
        writeSync();

        pg.output.flushAndReset();

        READY_LOOP:
        while (true)
        {
            final char type = pg.input.readNextCommand();
            switch (type) {
                case 'Z':  // ReadyForQuery
                {
                    pg.input.readReadyForQuery();
                    break READY_LOOP;
                }
                default: {
                    throw new IllegalStateException(String.format("invalid protocol action while syncing: '%s'", type));
                }
            }
        }
    }

    protected void writeBindExecuteSync(TypeHandler[] typeDecoders, List<Object> queryParams, String portalId, int limit) throws IOException {
        if (queryParams.size() != typeEncoders.length) {
            throw new IllegalArgumentException(String.format("Not enough Params provided to Statement, expected %d got %d", typeEncoders.length, queryParams.size()));
        }

        pg.checkReady();
        pg.output.checkReset();
        pg.state = ConnectionState.QUERY_RESULT;

        writeBind(typeDecoders, queryParams, portalId);
        writeExecute(portalId, limit);
        writeSync();

        // flow -> B/E/S

        pg.output.flushAndReset();
    }

    public void close() throws IOException {
        pg.output.checkReset();
        pg.state = ConnectionState.QUERY_CLOSE;

        // Close
        pg.output.beginCommand('C');
        pg.output.int8((byte) 'S');
        pg.output.string(statementId);
        pg.output.complete();

        // Sync
        pg.output.simpleCommand('S');
        pg.output.flushAndReset();

        // CloseComplete + Ready
        CLOSE_LOOP:
        while (true) {
            final char type = pg.input.readNextCommand();
            switch (type) {
                case '3': // CloseComplete
                {
                    final int size = pg.input.readInt32();
                    if (size != 4) {
                        throw new IllegalStateException(String.format("CloseComplete should be size 4 (was %d)", size));
                    }
                    break;
                }
                case 'Z': // ReadyForQuery
                {
                    pg.input.readReadyForQuery();
                    break CLOSE_LOOP;
                }
                default: {
                    throw new IllegalStateException(String.format("protocol violation while closing query, did not expect '%s'", type));
                }
            }
        }
    }
}

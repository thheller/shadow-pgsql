package shadow.pgsql;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.io.IOException;
import java.util.List;

/**
 * Created by zilence on 09.08.14.
 */
public abstract class PreparedBase implements AutoCloseable {
    protected final Connection pg;
    protected final String statementId;

    protected final TypeHandler[] typeEncoders;

    final Timer executeTimer;

    protected PreparedBase(Connection pg, String statementId, TypeHandler[] typeEncoders, String metricsName) {
        this.pg = pg;
        this.statementId = statementId;
        this.typeEncoders = typeEncoders;

        // FIXME: don't like that this is constructed every time, Query/Statement maybe a better place to keep these?
        if (metricsName != null) {
            this.executeTimer = pg.db.metricRegistry.timer(MetricRegistry.name("shadow-pgsql", "query", metricsName, "execute"));
        } else {
            this.executeTimer = pg.db.unnamedExecuteTimer;
        }

        pg.db.preparedCounter.inc();
    }

    public abstract String getSQLString();

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

            try {
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
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("Failed to encode parameter $%d [%s]\nSQL: %s", i + 1, param, getSQLString()), e);
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

    protected void writeSync() {
        // Sync
        pg.output.simpleCommand('S');
    }

    protected void executeWithParams(TypeHandler[] typeDecoders, List queryParams) throws IOException {
        if (queryParams.size() != typeEncoders.length) {
            throw new IllegalArgumentException(String.format("Incorrect params provided to Statement, expected %d got %d", typeEncoders.length, queryParams.size()));
        }

        pg.checkReady();
        pg.output.checkReset();

        // flow -> B/E/H

        try {
            writeBind(typeDecoders, queryParams, null);
            writeExecute(null, 0);
            writeSync();

            pg.output.flushAndReset();
            pg.state = ConnectionState.QUERY_RESULT;
        } catch (Exception e) {
            // nothing on the wire, no harm done
            pg.output.reset();
            pg.state = ConnectionState.READY;
            throw e;
        }
    }

    public void close() throws IOException {
        pg.closeStatement(statementId);

        pg.db.preparedCounter.dec();
    }
}

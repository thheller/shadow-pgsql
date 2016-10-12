package shadow.pgsql;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zilence on 15.08.14.
 */

public class DatabasePool extends GenericObjectPool<Connection> {
    private static final AtomicInteger poolSeq = new AtomicInteger(0);

    private final Database database;
    private final int poolId;

    public DatabasePool(Database database) {
        super(new PooledObjectFactory<Connection>() {
            @Override
            public PooledObject<Connection> makeObject() throws Exception {
                return new DefaultPooledObject<>(database.connect());
            }

            @Override
            public void destroyObject(PooledObject<Connection> po) throws Exception {
                po.getObject().close();
            }

            @Override
            public boolean validateObject(PooledObject<Connection> po) {
                Connection con = po.getObject();

                // FIXME: any way to set an error message?
                if (con.openStatements > 0) {
                    return false;
                } else if (con.txState != TransactionStatus.IDLE) {
                    return false;
                }

                con.checkReady();
                return true;
            }

            @Override
            public void activateObject(PooledObject<Connection> po) throws Exception {
            }

            @Override
            public void passivateObject(PooledObject<Connection> po) throws Exception {
            }
        });

        this.poolId = poolSeq.incrementAndGet();
        this.database = database;

        this.setDefaultOpts();
    }

    public void setDefaultOpts() {
        this.setMinIdle(3);
        this.setMaxIdle(25);
        this.setMaxTotal(25);
        this.setMaxWaitMillis(1000);
        this.setBlockWhenExhausted(true);
    }

    public int getPoolId() {
        return poolId;
    }

    public Database getDatabase() {
        return database;
    }

    public void doneWithConnection(Connection con) {
        if (con.isReady()) {
            returnObject(con);
        } else {
            try {
                invalidateObject(con);
            } catch (Exception e) {
                // FIXME: logger?
                System.out.format("Exception while invalidating Pool Object: %s\n", e);
            }
        }
    }

    public void checkConnection(Connection con) {
        if (con.isInTransaction()) {
            throw new IllegalStateException("Still in transaction, please commit or rollback!");
        }
        // FIXME: could be less picky and close them?
        if (con.openStatements > 0) {
            throw new IllegalStateException("Open Statement/Query in connection, please .close everything you prepared.");
        }
    }

    public <RESULT> RESULT withConnection(DatabaseTask<RESULT> task) throws Exception {
        Connection con = this.borrowObject();
        try {
            RESULT result = task.withConnection(con);
            checkConnection(con);
            return result;
        } catch (IOException e) {
            try {
                invalidateObject(con);
            } catch (Exception e2) {
                System.out.format("Caught IOException, cannot invalidate con: %s\n", e2);
            }
            throw e;
        } finally {
            doneWithConnection(con);
        }
    }


}

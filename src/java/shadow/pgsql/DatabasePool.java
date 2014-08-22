package shadow.pgsql;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;

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
        this.setMinIdle(0);
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

    public <RESULT> RESULT withConnection(DatabaseTask<RESULT> task) throws Exception {
        Connection con = this.borrowObject();
        try {
            RESULT result = task.withConnection(con);
            this.returnObject(con);
            return result;
        } catch (Exception e) {
            // FIXME: really? probably safe to re-use
            this.invalidateObject(con);
            throw e;
        }
    }


}

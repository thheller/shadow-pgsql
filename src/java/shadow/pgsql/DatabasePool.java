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
                // doesn't do any real validation, just checks if no writes are pending
                po.getObject().checkReady();
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


}

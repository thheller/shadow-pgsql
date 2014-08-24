package shadow.pgsql;

import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

/**
 * Created by zilence on 19.08.14.
 */
public class StressTest {

    @Test
    public void testConnect() throws IOException {
        for (int i = 0; i < 500; i++) {
            // Database.setup("localhost", 5432, "zilence", "cms");
        }
    }

    @Test
    public void testSSL() throws Exception {
        Database db = new DatabaseConfig("localhost", 5432)
                .setUser("zilence")
                .setDatabase("shadow_pgsql")
                .useSSL()
                .noSchema()
                .get();

        Connection pg = db.connect();

        SimpleQuery q = new SimpleQuery("INSERT INTO binary_types (fbytea) VALUES ($1) RETURNING fbytea");
        q.setRowBuilder(Helpers.ONE_COLUMN);
        q.setResultBuilder(Helpers.ONE_ROW);

        try (PreparedQuery pq = pg.prepareQuery(q)) {
            byte[] send = new byte[1000000];
            new Random().nextBytes(send);

            byte[] recv = (byte[]) pq.executeWith(send);

            assertArrayEquals(send, recv);
        }
    }
}

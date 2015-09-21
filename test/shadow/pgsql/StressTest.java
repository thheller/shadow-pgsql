package shadow.pgsql;

import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.*;

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

        SQL q = SQL.query("INSERT INTO binary_types (fbytea) VALUES ($1) RETURNING fbytea")
                .buildRowsWith(Helpers.ONE_COLUMN)
                .buildResultsWith(Helpers.ONE_ROW)
                .create();

        try (PreparedSQL pq = pg.prepare(q)) {
            byte[] send = new byte[1000000];
            new Random().nextBytes(send);

            byte[] recv = (byte[]) pq.queryWith(send);

            assertArrayEquals(send, recv);
        }
    }

    @Test
    public void testSQLParsing() {
        assertEquals(2, SQL.query("SELECT * FROM something WHERE a = $1 AND b = $2").getParamCount());
    }

    @Test
    public void testTemp() throws IOException {
        SQL sql = SQL.query("SELECT fnumeric FROM num_types").create();

        Database db = new DatabaseConfig("localhost", 5432)
                .setUser("zilence")
                .setDatabase("shadow_pgsql")
                .get();
        try (Connection pg = db.connect()) {
            Object result = pg.query(sql);
            System.out.println(result);
        }
    }

}

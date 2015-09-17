package shadow.pgsql;

import org.junit.Test;
import shadow.pgsql.types.NBase;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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

    public void nbaseRoundtrip(BigDecimal bd) {
        System.out.println(bd);
        NBase nb = NBase.pack(bd);
        assertEquals(bd, NBase.unpack(nb.weight, nb.sign, nb.dscale, nb.digits));
    }

    @Test
    public void testNBase() {
        //nbaseRoundtrip(new BigDecimal("1.23456789"));
        //nbaseRoundtrip(new BigDecimal("12.3456789"));
        //nbaseRoundtrip(new BigDecimal("123.456789"));
        //nbaseRoundtrip(new BigDecimal("12345.6789"));
        //nbaseRoundtrip(new BigDecimal("1234567.89"));
        //nbaseRoundtrip(new BigDecimal("12345678.9"));
        //nbaseRoundtrip(new BigDecimal("123456789.0"));
    }
}

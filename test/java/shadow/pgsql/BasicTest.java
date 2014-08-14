package shadow.pgsql;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * alright ... this is a very basic test.
 */
public class BasicTest {
    private Connection pg;
    private Database db;

    @Before
    public void setupConnection() throws Exception {
        this.db = Database.setup("localhost", 5432, "zilence", "shadow_pgsql");
        this.pg = db.connect();

        pg.execute("DELETE FROM num_types");
        pg.execute("DELETE FROM timestamp_types");
        pg.execute("DELETE FROM array_types");
    }

    @After
    public void closeConnection() throws Exception {
        this.pg.close();
    }

    @Test
    public void testTransactionWithSavepoint() throws IOException {
        pg.begin();

        try (PreparedStatement stmt = pg.prepare("INSERT INTO num_types (fint8) VALUES ($1)")) {
            stmt.executeWith(1);
            Savepoint savepoint = pg.savepoint();
            stmt.executeWith(2);
            savepoint.rollback();
            stmt.executeWith(3);
        }

        pg.commit();

        SimpleQuery query = new SimpleQuery("SELECT fint8 FROM num_types");
        query.setRowBuilder(Helpers.SINGLE_COLUMN);

        List numTypes = (List) pg.executeQuery(query);
        assertTrue(numTypes.contains(1l));
        assertFalse(numTypes.contains(2l));
        assertTrue(numTypes.contains(3l));
    }

    @Test
    public void testBasicError() throws IOException {
        try {
            pg.executeQuery("SELECT * FROM unknown_table");

            fail("Table doesn't exist");
        } catch (CommandException e) {
        }

        pg.checkReady();

        try {
            pg.prepareQuery(new SimpleQuery("INSERT INTO num_types VALUES ($1)"));

            fail("Not a Query");
        } catch (CommandException e) {
        }


        try (PreparedQuery q = pg.prepareQuery(new SimpleQuery("SELECT * FROM num_types WHERE id = $1"))) {
            try {
                q.executeWith("test");

                fail("Illegal Argument! Not an int.");
            } catch (IllegalArgumentException e) {
            }

            q.executeWith(1);

            pg.checkReady();
        }

        pg.checkReady();
    }

    public void roundtripOffsetDateTime(PreparedQuery pq, OffsetDateTime obj) throws IOException {
        OffsetDateTime result = (OffsetDateTime) pq.executeWith(obj);
        assertTrue(obj.isEqual(result));
    }

    @Test
    public void testTimestampTz() throws IOException {
        try (PreparedQuery pq = roundtripQuery("timestamp_types", "ftimestamptz")) {
            roundtripOffsetDateTime(pq, OffsetDateTime.now());
            roundtripOffsetDateTime(pq, OffsetDateTime.of(2014, 1, 1, 1, 1, 1, 0, ZoneOffset.of("-06:00")));
            roundtripOffsetDateTime(pq, OffsetDateTime.of(2014, 1, 1, 1, 1, 1, 0, ZoneOffset.of("+06:00")));
            roundtripOffsetDateTime(pq, OffsetDateTime.of(2014, 1, 1, 1, 1, 1, 0, ZoneOffset.of("Z")));
        }
    }


    public void roundtripLocalDateTime(PreparedQuery pq, LocalDateTime obj) throws IOException {
        LocalDateTime result = (LocalDateTime) pq.executeWith(obj);
        assertTrue(obj.isEqual(result));
    }

    @Test
    public void testTimestamp() throws IOException {
        try (PreparedQuery pq = roundtripQuery("timestamp_types", "ftimestamp")) {
            roundtripLocalDateTime(pq, LocalDateTime.now()); // will most likely have a fractional second
            roundtripLocalDateTime(pq, LocalDateTime.of(2014, 1, 1, 1, 1, 1, 0)); // test without fractional second
            roundtripLocalDateTime(pq, LocalDateTime.of(2014, 1, 1, 1, 1, 1, 999000000)); // pg default has 3 digit millis, no nanos
        }
    }

    public void roundtripLocalDate(PreparedQuery pq, LocalDate obj) throws IOException {
        LocalDate result = (LocalDate) pq.executeWith(obj);
        assertTrue(obj.isEqual(result));
    }

    @Test
    public void testDate() throws IOException {
        try (PreparedQuery pq = roundtripQuery("timestamp_types", "fdate")) {
            roundtripLocalDate(pq, LocalDate.now());
        }
    }

    public PreparedQuery roundtripQuery(String table, String field) throws IOException {
        SimpleQuery q = new SimpleQuery(String.format("INSERT INTO %s (%s) VALUES ($1) RETURNING %s", table, field, field));
        q.setRowBuilder(Helpers.SINGLE_COLUMN);
        q.setResultBuilder(Helpers.SINGLE_ROW);

        return pg.prepareQuery(q);
    }

    @Test
    public void testInt2Array() throws IOException {
        try (PreparedQuery pq = roundtripQuery("array_types", "aint2")) {
            short[] send = new short[] {1,2,3};
            short[] recv = (short[]) pq.executeWith(send);
            assertArrayEquals(send, recv);
        }
    }

    @Test
    public void testInt4Array() throws IOException {
        try (PreparedQuery pq = roundtripQuery("array_types", "aint4")) {
            int[] send = new int[] {1,2,3};
            int[] recv = (int[]) pq.executeWith(send);

            assertArrayEquals(send, recv);
        }
    }

    @Test
    public void testInt8Array() throws IOException {
        try (PreparedQuery pq = roundtripQuery("array_types", "aint8")) {
            long[] send = new long[] {1,2,3};
            long[] recv = (long[]) pq.executeWith(send);

            assertArrayEquals(send, recv);
        }
    }

    @Test
    public void testTextArray() throws IOException {
        try (PreparedQuery pq = roundtripQuery("array_types", "atext")) {
            String[] send = new String[] {"1","2","3"};

            // lol varargs ...
            List params = new ArrayList();
            params.add(send);

            String[] recv = (String[]) pq.execute(params);

            assertArrayEquals(send, recv);
        }
    }
}

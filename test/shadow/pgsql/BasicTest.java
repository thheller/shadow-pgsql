package shadow.pgsql;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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

        pg.executeWith("DELETE FROM types");
        pg.executeWith("DELETE FROM num_types");
        pg.executeWith("DELETE FROM timestamp_types");
        pg.executeWith("DELETE FROM array_types");
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
        query.setRowBuilder(Helpers.ONE_COLUMN);

        List numTypes = (List) pg.executeQueryWith(query);
        assertTrue(numTypes.contains(1l));
        assertFalse(numTypes.contains(2l));
        assertTrue(numTypes.contains(3l));
    }

    @Test
    public void testBasicError() throws IOException {
        try {
            pg.executeQueryWith("SELECT * FROM unknown_table");

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
            roundtripOffsetDateTime(pq, OffsetDateTime.of(2014, 1, 1, 0, 0, 0, 123456000, ZoneOffset.of("Z")));
            roundtripOffsetDateTime(pq, OffsetDateTime.of(2014, 1, 1, 1, 1, 1, 0, ZoneOffset.of("+06:00")));
            roundtripOffsetDateTime(pq, OffsetDateTime.of(2014, 1, 1, 1, 1, 1, 0, ZoneOffset.of("-06:00")));
            roundtripOffsetDateTime(pq, OffsetDateTime.now());
        }
    }


    public void roundtripLocalDateTime(PreparedQuery pq, LocalDateTime obj) throws IOException {
        OffsetDateTime result = (OffsetDateTime) pq.executeWith(obj);
        assertTrue(obj.atZone(ZoneId.systemDefault()).toOffsetDateTime().isEqual(result));
    }

    @Test
    public void testTimestamp() throws IOException {
        try (PreparedQuery pq = roundtripQuery("timestamp_types", "ftimestamp")) {
            // pg only has micros
            roundtripLocalDateTime(pq, LocalDateTime.of(2014, 1, 1, 1, 1, 1, 0));
            roundtripLocalDateTime(pq, LocalDateTime.of(2014, 1, 1, 1, 1, 1, 1000));
            roundtripLocalDateTime(pq, LocalDateTime.of(2014, 1, 1, 1, 1, 1, 12000));
            roundtripLocalDateTime(pq, LocalDateTime.of(2014, 1, 1, 1, 1, 1, 123000));
            roundtripLocalDateTime(pq, LocalDateTime.of(2014, 1, 1, 1, 1, 1, 1234000));
            roundtripLocalDateTime(pq, LocalDateTime.of(2014, 1, 1, 1, 1, 1, 12345000));
            roundtripLocalDateTime(pq, LocalDateTime.of(2014, 1, 1, 1, 1, 1, 123456000));
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
            roundtripLocalDate(pq, LocalDate.of(1979, 1, 1));
        }
    }

    public PreparedQuery roundtripQuery(String table, String field) throws IOException {
        SimpleQuery q = new SimpleQuery(String.format("INSERT INTO %s (%s) VALUES ($1) RETURNING %s", table, field, field));
        q.setRowBuilder(Helpers.ONE_COLUMN);
        q.setResultBuilder(Helpers.ONE_ROW);

        return pg.prepareQuery(q);
    }

    @Test
    public void testInt2Array() throws IOException {
        try (PreparedQuery pq = roundtripQuery("array_types", "aint2")) {
            short[] send = new short[]{1, 2, 3};
            short[] recv = (short[]) pq.executeWith(send);
            assertArrayEquals(send, recv);
        }
    }

    @Test
    public void testInt4Array() throws IOException {
        try (PreparedQuery pq = roundtripQuery("array_types", "aint4")) {
            int[] send = new int[]{1, 2, 3};
            int[] recv = (int[]) pq.executeWith(send);

            assertArrayEquals(send, recv);
        }
    }

    @Test
    public void testInt8Array() throws IOException {
        try (PreparedQuery pq = roundtripQuery("array_types", "aint8")) {
            long[] send = new long[]{1, 2, 3};
            long[] recv = (long[]) pq.executeWith(send);

            assertArrayEquals(send, recv);
        }
    }

    @Test
    public void testByteA() throws IOException {
        try (PreparedQuery pq = roundtripQuery("binary_types", "fbytea")) {
            byte[] send = new byte[]{0,1,3,3,7,0};
            byte[] recv = (byte[]) pq.executeWith(send);

            assertArrayEquals(send, recv);

            send = new byte[1000000];
            new Random().nextBytes(send);
            recv = (byte[]) pq.executeWith(send);

            assertArrayEquals(send, recv);
        }
    }

    @Test
    public void testTextArray() throws IOException {
        try (PreparedQuery pq = roundtripQuery("array_types", "atext")) {
            String[] send = new String[]{"clojure", "postgresql", "java"};

            // lol varargs ...
            List params = new ArrayList();
            params.add(send);

            String[] recv = (String[]) pq.execute(params);

            assertArrayEquals(send, recv);

            List sendList = new ArrayList();
            sendList.add("4");
            sendList.add("5");
            sendList.add("6");

            recv = (String[]) pq.executeWith(sendList);
            // FIXME: compare
        }
    }

    @Test
    public void testBool() throws IOException {
        try (PreparedQuery pq = roundtripQuery("types", "t_bool")) {
            assertTrue((Boolean) pq.executeWith(true));
            assertFalse((Boolean) pq.executeWith(false));

            // lol varargs ...
            List params = new ArrayList();
            params.add(null);
            assertNull(pq.execute(params));
        }
    }

    @Test
    public void testFloat4() throws IOException {
        try (PreparedQuery pq = roundtripQuery("types", "t_float4")) {
            assertEquals(3.14f, pq.executeWith(3.14f));
        }
    }

    @Test
    public void testFloat8() throws IOException {
        try (PreparedQuery pq = roundtripQuery("types", "t_float8")) {
            assertEquals(3.14d, pq.executeWith(3.14d));
        }
    }

    public static class GetById implements DatabaseTask<Object> {
        private final int id;

        public GetById(int id) {
            this.id = id;
        }

        @Override
        public Object withConnection(Connection con) throws Exception {
            return con.executeQueryWith("SELECT * FROM types WHERE id = $1", id);
        }
    }

    @Test
    public void testPool() throws Exception {
        DatabasePool pool = new DatabasePool(db);
        final int id = 1;
        Object r1 = pool.withConnection(con -> con.executeQueryWith("SELECT * FROM types WHERE id = $1", id));
        Object r2 = pool.withConnection(new GetById(id));
        // FIXME: you call this a test?
    }
}

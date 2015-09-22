package shadow.pgsql;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import shadow.pgsql.types.NBase;
import shadow.pgsql.types.Types;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * alright ... this is a very basic test.
 */
public class BasicTest {
    private Connection pg;
    private Database db;

    @Before
    public void setupConnection() throws Exception {
        this.db = new DatabaseConfig("localhost", 5432)
                .setUser("zilence")
                .setDatabase("shadow_pgsql")
                .get();

        this.pg = db.connect();

        pg.executeWith(SQL.statement("DELETE FROM types").create());
        pg.executeWith(SQL.statement("DELETE FROM num_types").create());
        pg.executeWith(SQL.statement("DELETE FROM timestamp_types").create());
        pg.executeWith(SQL.statement("DELETE FROM array_types").create());
    }

    @After
    public void closeConnection() throws Exception {
        this.pg.close();
    }

    @Test
    public void testTransactionWithSavepoint() throws IOException {
        pg.begin();

        try (PreparedSQL stmt = pg.prepare(SQL.statement("INSERT INTO num_types (fint8) VALUES ($1)").create())) {
            stmt.executeWith(1);
            Savepoint savepoint = pg.savepoint();
            stmt.executeWith(2);
            savepoint.rollback();
            stmt.executeWith(3);
        }

        pg.commit();

        SQL query = SQL.query("SELECT fint8 FROM num_types").buildRowsWith(Helpers.ONE_COLUMN).create();

        List numTypes = (List) pg.queryWith(query);
        assertTrue(numTypes.contains(1l));
        assertFalse(numTypes.contains(2l));
        assertTrue(numTypes.contains(3l));
    }

    @Test
    public void testBasicError() throws IOException {
        try {
            pg.queryWith(SQL.query("SELECT * FROM unknown_table").create());

            fail("Table doesn't exist");
        } catch (CommandException e) {
        }

        pg.checkReady();

        try {
            pg.prepare(SQL.query("INSERT INTO num_types VALUES ($1)").create());

            fail("Not a Query");
        } catch (IllegalStateException e) {
        }


        try (PreparedSQL q = pg.prepare(SQL.query("SELECT * FROM num_types WHERE id = $1").create())) {
            try {
                q.queryWith("test");

                fail("Illegal Argument! Not an int.");
            } catch (IllegalArgumentException e) {
            }

            q.queryWith(1);

            pg.checkReady();
        }

        pg.checkReady();
    }

    public void roundtripOffsetDateTime(PreparedSQL pq, OffsetDateTime obj) throws IOException {
        OffsetDateTime result = (OffsetDateTime) pq.queryWith(obj);
        assertTrue(obj.isEqual(result));
    }

    @Test
    public void testTimestampTz() throws IOException {
        try (PreparedSQL pq = roundtripQuery("timestamp_types", "ftimestamptz")) {
            roundtripOffsetDateTime(pq, OffsetDateTime.of(2014, 1, 1, 0, 0, 0, 123456000, ZoneOffset.of("Z")));
            roundtripOffsetDateTime(pq, OffsetDateTime.of(2014, 1, 1, 1, 1, 1, 0, ZoneOffset.of("+06:00")));
            roundtripOffsetDateTime(pq, OffsetDateTime.of(2014, 1, 1, 1, 1, 1, 0, ZoneOffset.of("-06:00")));
            roundtripOffsetDateTime(pq, OffsetDateTime.now());
        }
    }


    public void roundtripLocalDateTime(PreparedSQL pq, LocalDateTime obj) throws IOException {
        OffsetDateTime result = (OffsetDateTime) pq.queryWith(obj);
        assertTrue(obj.atZone(ZoneId.systemDefault()).toOffsetDateTime().isEqual(result));
    }

    @Test
    public void testTimestamp() throws IOException {
        try (PreparedSQL pq = roundtripQuery("timestamp_types", "ftimestamp")) {
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

    public void roundtripLocalDate(PreparedSQL pq, LocalDate obj) throws IOException {
        LocalDate result = (LocalDate) pq.queryWith(obj);
        assertTrue(obj.isEqual(result));
    }

    @Test
    public void testDate() throws IOException {
        try (PreparedSQL pq = roundtripQuery("timestamp_types", "fdate")) {
            roundtripLocalDate(pq, LocalDate.now());
            roundtripLocalDate(pq, LocalDate.of(1979, 1, 1));
        }
    }

    public void roundtrip(PreparedSQL pq, Object value) throws IOException {
        assertEquals(value, pq.queryWith(value));
    }


    @Test
    public void testSingleNumeric() throws IOException {

        SQL sql = SQL.query("SELECT fnumeric FROM num_types ORDER BY fnumeric LIMIT 1")
                .buildResultsWith(Helpers.ONE_ROW)
                .buildRowsWith(Helpers.ONE_COLUMN)
                .create();
        Object result = pg.query(sql);
        System.out.println(result);
    }

    @Test
    public void testNumeric() throws IOException {


        try (PreparedSQL pq = roundtripQuery("num_types", "fnumeric")) {
            roundtrip(pq, new BigDecimal("0.123456789"));
            roundtrip(pq, new BigDecimal("1.23456789"));
            roundtrip(pq, new BigDecimal("12.3456789"));
            roundtrip(pq, new BigDecimal("123.456789"));
            roundtrip(pq, new BigDecimal("1234.56789"));
            roundtrip(pq, new BigDecimal("12345.6789"));
            roundtrip(pq, new BigDecimal("123456.789"));
            roundtrip(pq, new BigDecimal("1234567.89"));
            roundtrip(pq, new BigDecimal("12345678.9"));
            roundtrip(pq, new BigDecimal("123456789.0"));
        }
    }

    public PreparedSQL roundtripQuery(String table, String field) throws IOException {
        SQL q = SQL.query(String.format("INSERT INTO %s (%s) VALUES ($1) RETURNING %s", table, field, field))
                .buildResultsWith(Helpers.ONE_ROW)
                .buildRowsWith(Helpers.ONE_COLUMN)
                .create();

        return pg.prepare(q);
    }

    @Test
    public void testInt2Array() throws IOException {
        try (PreparedSQL pq = roundtripQuery("array_types", "aint2")) {
            short[] send = new short[]{1, 2, 3};
            short[] recv = (short[]) pq.queryWith(send);
            assertArrayEquals(send, recv);
        }
    }

    @Test
    public void testInt4Array() throws IOException {
        try (PreparedSQL pq = roundtripQuery("array_types", "aint4")) {
            int[] send = new int[]{1, 2, 3};
            int[] recv = (int[]) pq.queryWith(send);

            assertArrayEquals(send, recv);
        }
    }

    @Test
    public void testInt8Array() throws IOException {
        try (PreparedSQL pq = roundtripQuery("array_types", "aint8")) {
            long[] send = new long[]{1, 2, 3};
            long[] recv = (long[]) pq.queryWith(send);

            assertArrayEquals(send, recv);
        }
    }

    @Test
    public void testByteA() throws IOException {
        try (PreparedSQL pq = roundtripQuery("binary_types", "fbytea")) {
            byte[] send = new byte[]{0, 1, 3, 3, 7, 0};
            byte[] recv = (byte[]) pq.queryWith(send);

            assertArrayEquals(send, recv);

            send = new byte[1000000];
            new Random().nextBytes(send);
            recv = (byte[]) pq.queryWith(send);

            assertArrayEquals(send, recv);
        }
    }

    @Test
    public void testTextArray() throws IOException {
        try (PreparedSQL pq = roundtripQuery("array_types", "atext")) {
            String[] send = new String[]{"clojure", "postgresql", "java"};

            // lol varargs ...
            List params = new ArrayList();
            params.add(send);

            String[] recv = (String[]) pq.query(params);

            assertArrayEquals(send, recv);

            List sendList = new ArrayList();
            sendList.add("4");
            sendList.add("5");
            sendList.add("6");

            recv = (String[]) pq.queryWith(sendList);
            // FIXME: compare
        }
    }

    @Test
    public void testNumericArray() throws IOException {
        Object result = pg.queryWith(SQL.query("SELECT anumeric FROM array_types").create());

        try (PreparedSQL pq = roundtripQuery("array_types", "anumeric")) {
            BigDecimal[] send = new BigDecimal[]{
                    new BigDecimal("0.123456789"),
                    new BigDecimal("1.23456789"),
                    new BigDecimal("12.3456789"),
                    new BigDecimal("123.456789"),
                    new BigDecimal("1234.56789"),
                    new BigDecimal("12345.6789"),
                    new BigDecimal("123456.789"),
                    new BigDecimal("1234567.89"),
                    new BigDecimal("12345678.9"),
                    new BigDecimal("123456789.0")
            };

            // lol varargs ...
            List params = new ArrayList();
            params.add(send);

            BigDecimal[] recv = (BigDecimal[]) pq.query(params);

            assertArrayEquals(send, recv);
        }
    }

    @Test
    public void testBool() throws IOException {
        try (PreparedSQL pq = roundtripQuery("types", "t_bool")) {
            assertTrue((Boolean) pq.queryWith(true));
            assertFalse((Boolean) pq.queryWith(false));

            // lol varargs ...
            List params = new ArrayList();
            params.add(null);
            assertNull(pq.query(params));
        }
    }

    @Test
    public void testFloat4() throws IOException {
        try (PreparedSQL pq = roundtripQuery("types", "t_float4")) {
            assertEquals(3.14f, pq.queryWith(3.14f));
        }
    }

    @Test
    public void testFloat8() throws IOException {
        try (PreparedSQL pq = roundtripQuery("types", "t_float8")) {
            assertEquals(3.14d, pq.queryWith(3.14d));
        }
    }

    @Test
    public void testUUID() throws IOException {
        try (PreparedSQL pq = roundtripQuery("types", "t_uuid")) {
            UUID uuid = UUID.randomUUID();
            assertEquals(uuid, pq.queryWith(uuid));
        }
    }

    public static class GetById implements DatabaseTask<Object> {
        private final SQL sql;
        private final int id;

        public GetById(SQL sql, int id) {
            this.sql = sql;
            this.id = id;
        }

        @Override
        public Object withConnection(Connection con) throws Exception {
            return con.queryWith(sql, id);
        }
    }

    @Test
    public void testPool() throws Exception {
        DatabasePool pool = new DatabasePool(db);

        SQL sql = SQL.query("SELECT * FROM types WHERE id = $1")
                .addParameterType(Types.INT4)
                .create();

        final int id = 1;

        Object r1 = pool.withConnection(con -> con.queryWith(sql, id));
        Object r2 = pool.withConnection(new GetById(sql, id));
        // FIXME: you call this a test?
    }


    @Test
    public void testCleanup() throws Exception {
        DatabasePool pool = new DatabasePool(db);

        try {
            pool.withConnection(con -> con.prepare(SQL.statement("DELETE FROM types").create()));
            fail("should have complained");
        } catch (IllegalStateException e) {
        }

        try {
            pool.withConnection(con -> {
                con.begin();
                return null;
            });
            fail("should have complained");
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void testHStore() throws Exception {

        Map<String, String> map = new HashMap<>();
        map.put("a", "1");
        map.put("b", "2");
        map.put("c", null);

        try (PreparedSQL pq = roundtripQuery("types", "t_hstore")) {
            Map<String, String> result = (Map<String, String>) pq.queryWith(map);

            assertTrue(result.containsKey("a"));
            assertTrue(result.containsKey("b"));
            assertTrue(result.containsKey("c"));
            assertEquals("1", result.get("a"));
            assertEquals("2", result.get("b"));
            assertNull(result.get("c"));
        }
    }


    @Test
    public void testNotNull() throws Exception {
        try {
            pg.executeWith(SQL.statement("INSERT INTO dummy (nnull) VALUES ($1)").create(), new Object[]{null});
            fail("not supposed to succeed, handed null value to not null field");
        } catch (CommandException e) {
            // TODO: verify
        }
    }

    public void nbaseRoundtrip(BigDecimal bd) {
        NBase nb = NBase.pack(bd);
        assertEquals(bd, nb.unpack());
    }

    public void nbaseRoundtripSQL(BigDecimal bd) {
    }

    @Test
    public void testNBase() throws IOException {
        // nbaseRoundtrip(new BigDecimal("124123424748280884901092740643973478506398851268580860000206792733576.28567546"));

        Random r = new Random();
        for (int i = 0; i < 1000000; i++) {
            // generate a bunch of random decimals
            BigInteger bi = new BigInteger(r.nextInt(512), r);
            BigDecimal bd = new BigDecimal(bi, r.nextInt(16));

            nbaseRoundtrip(bd);
        }
    }

    @Test
    public void testNBaseSQL() throws IOException {
        // nbaseRoundtrip(new BigDecimal("0"));

        try (PreparedSQL pq = roundtripQuery("num_types", "fnumeric")) {

            BigDecimal bd = BigDecimal.ZERO;
            assertEquals(bd, pq.queryWith(bd));
            bd = new BigDecimal("0E-7");
            assertEquals(bd, pq.queryWith(bd));
            bd = new BigDecimal("123112312340921837419304719028341724912571293487129348172349218759283512943871290487120985713290487109234871234091287431209348712034987");
            assertEquals(bd, pq.queryWith(bd));

            Random r = new Random();
            for (int i = 0; i < 10000; i++) {
                // generate a bunch of random decimals
                BigInteger bi = new BigInteger(r.nextInt(1024), r);
                bd = new BigDecimal(bi, r.nextInt(64));

                assertEquals(bd, pq.queryWith(bd));
            }
        }
    }
}

package shadow.pgsql;

import org.junit.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
    }

    @After
    public void closeConnection() throws Exception {
        this.pg.close();
    }

    @Test
    public void testTransactionWithSavepoint() throws IOException {
        pg.begin();

        try (PreparedStatement stmt = pg.prepare("INSERT INTO num_types (fint8) VALUES ($1)")) {
            stmt.execute(1);
            Savepoint savepoint = pg.savepoint();
            stmt.execute(2);
            savepoint.rollback();
            stmt.execute(3);
        }

        pg.commit();

        SimpleQuery query = new SimpleQuery("SELECT fint8 FROM num_types");
        query.setRowBuilder(Handlers.SINGLE_COLUMN);

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
                q.execute(Arrays.asList("test"));

                fail("Illegal Argument! Not an int.");
            } catch (IllegalArgumentException e) {
            }

            pg.checkReady();
        }

        pg.checkReady();
    }
}

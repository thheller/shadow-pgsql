package shadow.pgsql;

import java.io.IOException;
import java.util.List;

/**
 * haha, you fool. There are no tests here.
 */
public class BasicTest {

    public static void main(String[] args) throws IOException {
        try (Connection pg = Database.setup("localhost", 5432, "zilence", "shadow_pgsql")
                .connect()) {

            pg.execute("DELETE FROM num_types");

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

            // wait ... this is starting to look like a test
            List numTypes = (List) pg.executeQuery(query);
            boolean n1 = numTypes.contains(1l);
            boolean n2 = numTypes.contains(2l);
            boolean n3 = numTypes.contains(3l);

            /*
            // basic INSERT
            long rows = pg.execute("INSERT INTO num_types (fint4) VALUES ($1)", 1).getRowsAffected();

            // INSERT with SERIAL id, returns generated id
            SimpleQuery query = new SimpleQuery(
                    "INSERT INTO num_types (fint4) VALUES ($1) RETURNING id"
            );

            query.setRowBuilder(Handlers.SINGLE_COLUMN);

            int insertedId = (int) pg.executeQuery(query, 1);

            */

            System.out.println("Fancy.");
        }
    }
}

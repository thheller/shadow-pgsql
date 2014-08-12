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
            // basic INSERT
            long rows = pg.execute("INSERT INTO num_types (fint4) VALUES ($1)", 1);

            // INSERT with SERIAL id, returns generated id
            SimpleQuery query = new SimpleQuery(
                    "INSERT INTO num_types (fint4) VALUES ($1) RETURNING id"
            );

            query.setResultBuilder(Handlers.SINGLE_ROW);
            query.setRowBuilder(Handlers.SINGLE_COLUMN);

            int insertedId = (int) pg.executeQuery(query, 1);

            // SELECT
            List numTypes = (List) pg.executeQuery("SELECT * FROM num_types");

            System.out.println("Fancy.");
        }
    }
}

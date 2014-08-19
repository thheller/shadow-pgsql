package shadow.pgsql;

import java.io.IOException;

/**
 * Created by zilence on 13.08.14.
 */
public class Savepoint {
    private final Connection con;
    private final String name;

    public Savepoint(Connection con, String name) {
        this.con = con;
        this.name = name;
    }

    public void release() throws IOException {
        con.simpleStatement(String.format("RELEASE SAVEPOINT %s", name));
    }

    public void rollback() throws IOException {
        con.simpleStatement(String.format("ROLLBACK TO SAVEPOINT %s", name));
    }
}

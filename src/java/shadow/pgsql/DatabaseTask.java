package shadow.pgsql;

/**
 * Created by zilence on 21.08.14.
 */
@FunctionalInterface
public interface DatabaseTask<RESULT> {
    public RESULT withConnection(Connection con) throws Exception;
}

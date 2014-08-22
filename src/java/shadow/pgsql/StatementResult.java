package shadow.pgsql;

/**
 * Created by zilence on 13.08.14.
 */
public class StatementResult {
    final String tag;

    public StatementResult(String tag) {
        this.tag = tag;
    }

    public String getTag() {
        return tag;
    }

    public int getRowsAffected() {
        return Integer.parseInt(tag.substring(tag.lastIndexOf(" ") + 1));
    }

    @Override
    public String toString() {
        return tag;
    }
}

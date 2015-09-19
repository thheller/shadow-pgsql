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
        int i = tag.lastIndexOf(" ");
        if (i != -1) {
            return Integer.parseInt(tag.substring(i + 1));
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return tag;
    }
}

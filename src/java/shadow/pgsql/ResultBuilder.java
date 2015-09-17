package shadow.pgsql;

/**
 * Created by zilence on 09.08.14.
 */
public interface ResultBuilder<ACC, RESULT, ROW> {
    ACC init();

    ACC add(ACC state, ROW row);

    RESULT complete(ACC state);

    @FunctionalInterface
    interface Factory {
        ResultBuilder create(ColumnInfo [] columns);
    }
}

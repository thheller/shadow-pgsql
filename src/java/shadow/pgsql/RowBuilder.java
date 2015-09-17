package shadow.pgsql;

/**
 * Created by zilence on 10.08.14.
 */
public interface RowBuilder<ACC, ROW> {
    ACC init();

    ACC add(ACC state, ColumnInfo columnInfo, int fieldIndex, Object value);

    ROW complete(ACC state);

    @FunctionalInterface
    interface Factory {
        RowBuilder create(ColumnInfo[] columns);
    }
}

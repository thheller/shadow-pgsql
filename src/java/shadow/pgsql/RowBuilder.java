package shadow.pgsql;

/**
 * Created by zilence on 10.08.14.
 */
public interface RowBuilder<ACC, ROW> {
    public ACC init();

    public ACC add(ACC state, ColumnInfo columnInfo, int fieldIndex, Object value);

    public ROW complete(ACC state);
}

package shadow.pgsql;

/**
 * Created by zilence on 12.08.14.
 */
public interface Query<RESULT> extends Statement {
    ResultBuilder<?, RESULT, ?> createResultBuilder(ColumnInfo[] columns);

    RowBuilder<?, ?> createRowBuilder(ColumnInfo[] columns);
}

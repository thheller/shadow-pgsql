package shadow.pgsql;

import java.util.List;

/**
 * Created by zilence on 12.08.14.
 */
public interface Query<RESULT> {
    String getStatement();

    List<TypeHandler> getParameterTypes();

    ResultBuilder<?, RESULT, ?> createResultBuilder(ColumnInfo[] columns);

    RowBuilder<?, ?> createRowBuilder(ColumnInfo[] columns);
}

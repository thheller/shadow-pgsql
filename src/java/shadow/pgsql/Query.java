package shadow.pgsql;

import java.util.List;

/**
 * Query represents an SQL Statement that returns zero or more rows of data.
 */
public interface Query<RESULT> {
    String getSQLString();

    List<TypeHandler> getParameterTypes();

    TypeRegistry getTypeRegistry();

    ResultBuilder<?, RESULT, ?> createResultBuilder(ColumnInfo[] columns);

    RowBuilder<?, ?> createRowBuilder(ColumnInfo[] columns);
}

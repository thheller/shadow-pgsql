package shadow.pgsql;

import java.util.List;

/**
 * Statement is a SQL Statement that does not return data, only a "Tag"
 */
public interface Statement {
    /**
     * @return null | optional name for a statement, currently only used for metrics
     */
    String getName();

    String getSQLString();

    List<TypeHandler> getParameterTypes();

    TypeRegistry getTypeRegistry();
}

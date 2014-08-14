package shadow.pgsql;

import java.util.List;

/**
 * Statement is a SQL Statement that does not return data, only a "Tag"
 */
public interface Statement {
    String getSQLString();

    List<TypeHandler> getParameterTypes();

    TypeRegistry getTypeRegistry();
}

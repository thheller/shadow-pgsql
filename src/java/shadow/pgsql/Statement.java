package shadow.pgsql;

import java.util.List;

/**
 * Created by zilence on 12.08.14.
 */
public interface Statement {
    String getStatement();

    List<TypeHandler> getParameterTypes();
}

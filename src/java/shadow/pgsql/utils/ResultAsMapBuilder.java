package shadow.pgsql.utils;

import shadow.pgsql.ResultBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zilence on 11.08.14.
 */
public abstract class ResultAsMapBuilder implements ResultBuilder {
    @Override
    public Object init() {
        return new HashMap();
    }

    @Override
    public Object add(Object state, Object o) {
        ((Map) state).put(getKey(o), getValue(o));
        return state;
    }

    public abstract Object getKey(Object row);

    public abstract Object getValue(Object row);

    @Override
    public Object complete(Object state) {
        return state;
    }
}

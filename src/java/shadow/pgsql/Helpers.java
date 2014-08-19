package shadow.pgsql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zilence on 10.08.14.
 */
public class Helpers {
    public static final ResultBuilder RESULT_AS_LIST = new ResultBuilder<List, List, Object>() {
        @Override
        public List init() {
            return new ArrayList();
        }

        @Override
        public List add(List state, Object row) {
            state.add(row);
            return state;
        }

        @Override
        public List complete(List state) {
            return state;
        }
    };

    /**
     * returns row as a map of {column-name value}
     */
    public static final RowBuilder<Map<String, Object>, Map<String, Object>> ROW_AS_MAP = new RowBuilder<Map<String, Object>, Map<String, Object>>() {
        @Override
        public Map<String, Object> init() {
            return new HashMap<>();
        }

        @Override
        public Map<String, Object> add(Map<String, Object> state, ColumnInfo columnInfo, int fieldIndex, Object value) {
            state.put(columnInfo.name, value);
            return state;
        }

        @Override
        public Map<String, Object> complete(Map<String, Object> state) {
            return state;
        }
    };

    public static final ResultBuilder<Object, Object, Object> ONE_ROW = new ResultBuilder<Object, Object, Object>() {
        @Override
        public Object init() {
            return null;
        }

        @Override
        public Object add(Object state, Object row) {
            if (state != null) {
                throw new IllegalStateException("only expected a single row");
            }
            return row;
        }

        @Override
        public Object complete(Object row) {
            return row;
        }
    };

    public static final RowBuilder<Object, Object> ONE_COLUMN = new RowBuilder<Object, Object>() {
        @Override
        public Object init() {
            return null;
        }

        @Override
        public Object add(Object state, ColumnInfo columnInfo, int fieldIndex, Object value) {
            if (state != null) {
                throw new IllegalStateException("did only expect one column");
            }
            return value;
        }

        @Override
        public Object complete(Object state) {
            return state;
        }
    };

    /**
     * returns a row as a List of column values, column names are discarded
     */
    public static final RowBuilder<List, List> ROW_AS_LIST = new RowBuilder<List, List>() {
        @Override
        public List init() {
            return new ArrayList();
        }

        @Override
        public List add(List state, ColumnInfo columnInfo, int fieldIndex, Object value) {
            state.add(value);
            return state;
        }

        @Override
        public List complete(List state) {
            return state;
        }
    };

}

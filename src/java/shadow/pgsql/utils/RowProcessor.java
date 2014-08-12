package shadow.pgsql.utils;

import shadow.pgsql.ResultBuilder;

/**
 * Simple ResultBuilder that does not accumulate a Result.
 *
 * Instead you implement process(ROW)
 *
 * Returns the number of rows processed
 *
 * @author Thomas Heller
 */
public abstract class RowProcessor<ROW> implements ResultBuilder<Integer, Integer, ROW> {

    @Override
    public Integer init() {
        return 0;
    }

    public abstract void process(ROW row);

    @Override
    public Integer add(Integer state, ROW row) {
        process(row);
        return state + 1;
    }

    @Override
    public Integer complete(Integer state) {
        return state;
    }
}

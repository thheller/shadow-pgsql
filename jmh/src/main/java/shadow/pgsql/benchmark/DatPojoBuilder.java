package shadow.pgsql.benchmark;

import shadow.pgsql.ColumnInfo;
import shadow.pgsql.RowBuilder;

import java.math.BigDecimal;

/**
 * Created by zilence on 20.09.15.
 */
class DatPojoBuilder implements RowBuilder<DatPojo, DatPojo> {
    @Override
    public DatPojo init() {
        return new DatPojo();
    }

    @Override
    public DatPojo add(DatPojo state, ColumnInfo columnInfo, int fieldIndex, Object value) {
        switch (columnInfo.name) {
            case "test_string":
                state.setTestString((String) value);
                break;
            case "test_long":
                state.setTestLong((Long) value);
                break;
            case "test_int":
                state.setTestInt((Integer) value);
                break;
            case "test_double":
                state.setTestDouble((Double) value);
                break;
            case "test_bd":
                state.setTestBigDecimal((BigDecimal) value);
                break;
            default:
                throw new IllegalArgumentException(String.format("did not expected field: %s", columnInfo.name));
        }
        return state;
    }

    @Override
    public DatPojo complete(DatPojo state) {
        return state;
    }
}

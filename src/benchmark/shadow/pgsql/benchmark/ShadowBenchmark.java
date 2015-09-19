package shadow.pgsql.benchmark;

import shadow.pgsql.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

/**
 * Created by zilence on 19.09.15.
 */
public class ShadowBenchmark implements AutoCloseable {

    private final Database db;
    private final Connection pg;

    private static final RowBuilder ROW_TO_POJO = new RowBuilder<DatPojo, DatPojo>() {
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
    };

    private final SQL SELECT_POJOS = SQL.query("SELECT * FROM pojos")
            .buildRowsWith(ROW_TO_POJO)
            .create();


    public ShadowBenchmark() throws IOException {
        this.db = Database.setup("localhost", 5432, "zilence", "shadow_bench");
        this.pg = db.connect();
    }

    public List<DatPojo> selectPojos() throws IOException {
        return (List<DatPojo>) pg.query(SELECT_POJOS);
    }


    @Override
    public void close() throws Exception {
        this.pg.close();
    }


    public static void main(String[] args) throws Exception {
        ShadowBenchmark bench = new ShadowBenchmark();

        System.out.println("Press any key to start.");
        System.in.read();
        for (int i = 0; i < 100; i++) {
            List<DatPojo> pojos = bench.selectPojos();
            System.out.format("got %d pojos\n", pojos.size());
        }
        System.out.println("Completed press any key to quit");
        System.in.read();
        bench.close();
    }
}


package shadow.pgsql.benchmark;

import com.codahale.metrics.*;
import shadow.pgsql.*;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by zilence on 19.09.15.
 */
public class ShadowBenchmark implements AutoCloseable {

    private final Database db;
    private final Connection pg;

    private static final RowBuilder ROW_TO_POJO = new DatPojoBuilder();

    private static final SQL SELECT_POJOS = SQL.query("SELECT * FROM pojos")
            .withName("benchmark")
            // .buildResultsWith(Helpers.RESULT_AS_LINKED_LIST)
            .buildRowsWith(ROW_TO_POJO)
            .create();

    private static final SQL SELECT_ONE_POJO = SQL.query("SELECT * FROM pojos WHERE test_int = $1")
            .withName("benchmark")
            .buildResultsWith(Helpers.ONE_ROW)
            .buildRowsWith(ROW_TO_POJO)
            .create();


    public ShadowBenchmark() throws IOException {
        this.db = Database.setup("localhost", 5432, "zilence", "shadow_bench");
        this.pg = db.connect();
    }

    public List<DatPojo> selectPojos() throws IOException {
        return (List<DatPojo>) pg.query(SELECT_POJOS);
    }

    public DatPojo selectPojo(int id) throws IOException {
        return (DatPojo) pg.queryWith(SELECT_ONE_POJO, id);
    }


    @Override
    public void close() throws Exception {
        this.pg.close();
    }


    public static void main(String[] args) throws Exception {
        ShadowBenchmark bench = new ShadowBenchmark();

        Random r = new Random();

        Timer timer = bench.db.getMetricRegistry().timer("benchmark");

        System.out.println("Warmup");
        for (int i = 0; i < 1000; i++) {
            bench.selectPojo(r.nextInt(100));
        }

        System.out.println("Press any key to start.");
        // System.in.read();
        System.out.println("Looping");

        for (int i = 0; i < 10000; i++) {
            Timer.Context t = timer.time();
            bench.selectPojo(r.nextInt(100));

            long duration = t.stop();
            //System.out.format("got %d pojos\n", pojos.size());
            if (i % 500 == 0) {
                System.out.format("run: %d duration: %d\n", i, duration);
            }
        }
        System.out.println("Completed press any key to quit");
        //System.in.read();
        bench.close();

        ConsoleReporter report = ConsoleReporter.forRegistry(bench.db.getMetricRegistry())
                .filter(new MetricFilter() {
                    @Override
                    public boolean matches(String s, Metric metric) {
                        return s.equals("benchmark");
                    }
                })
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        report.report();
    }

}

/***
 * StreamIO (pojo)
 *
 count = 2000
 mean rate = 194.90 calls/second
 1-minute rate = 99.37 calls/second
 5-minute rate = 85.27 calls/second
 15-minute rate = 82.83 calls/second
 min = 3.02 milliseconds
 max = 8.90 milliseconds
 mean = 3.27 milliseconds
 stddev = 0.31 milliseconds
 median = 3.16 milliseconds
 75% <= 3.34 milliseconds
 95% <= 3.86 milliseconds
 98% <= 4.10 milliseconds
 99% <= 4.27 milliseconds
 99.9% <= 4.88 milliseconds
 */


/***
 * StreamIO (maps)
 *
 count = 2000
 mean rate = 191.06 calls/second
 1-minute rate = 91.97 calls/second
 5-minute rate = 77.56 calls/second
 15-minute rate = 75.06 calls/second
 min = 3.07 milliseconds
 max = 6.50 milliseconds
 mean = 3.34 milliseconds
 stddev = 0.34 milliseconds
 median = 3.22 milliseconds
 75% <= 3.39 milliseconds
 95% <= 4.11 milliseconds
 98% <= 4.35 milliseconds
 99% <= 4.71 milliseconds
 99.9% <= 5.44 milliseconds
 */

/***
 * StreamIO (linked list + pojo)
 *
 count = 2000
 mean rate = 200.77 calls/second
 1-minute rate = 89.20 calls/second
 5-minute rate = 89.20 calls/second
 15-minute rate = 89.20 calls/second
 min = 2.91 milliseconds
 max = 5.74 milliseconds
 mean = 3.15 milliseconds
 stddev = 0.27 milliseconds
 median = 3.06 milliseconds
 75% <= 3.21 milliseconds
 95% <= 3.67 milliseconds
 98% <= 4.06 milliseconds
 99% <= 4.29 milliseconds
 99.9% <= 4.73 milliseconds
 */


/***
 * StreamIO (without frameSizes)
 *
 count = 2000
 mean rate = 86.69 calls/second
 1-minute rate = 27.94 calls/second
 5-minute rate = 6.43 calls/second
 15-minute rate = 2.20 calls/second
 min = 2.71 milliseconds
 max = 6.14 milliseconds
 mean = 2.96 milliseconds
 stddev = 0.28 milliseconds
 median = 2.86 milliseconds
 75% <= 3.04 milliseconds
 95% <= 3.52 milliseconds
 98% <= 3.80 milliseconds
 99% <= 3.92 milliseconds
 99.9% <= 4.76 milliseconds
 */

/***
 * SocketIO (NIO)
 *
 count = 2000
 mean rate = 149.66 calls/second
 1-minute rate = 26.51 calls/second
 5-minute rate = 11.99 calls/second
 15-minute rate = 9.47 calls/second
 min = 3.82 milliseconds
 max = 6.40 milliseconds
 mean = 4.26 milliseconds
 stddev = 0.37 milliseconds
 median = 4.13 milliseconds
 75% <= 4.39 milliseconds
 95% <= 4.94 milliseconds
 98% <= 5.27 milliseconds
 99% <= 5.66 milliseconds
 99.9% <= 6.40 milliseconds
 */
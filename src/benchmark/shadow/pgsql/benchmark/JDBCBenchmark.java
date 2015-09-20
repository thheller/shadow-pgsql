package shadow.pgsql.benchmark;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by zilence on 19.09.15.
 */
public class JDBCBenchmark implements AutoCloseable {

    private final Connection con;

    public JDBCBenchmark(String url) throws SQLException {
        this.con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/shadow_bench", "zilence", "");
    }

    @Override
    public void close() throws Exception {
        this.con.close();
    }

    public Timestamp selectNow() throws SQLException {
        try (PreparedStatement s = con.prepareStatement("SELECT now()")) {
            try (ResultSet rs = s.executeQuery()) {
                if (rs.next()) {
                    return rs.getTimestamp(1);
                } else {
                    throw new IllegalStateException("no now?");
                }
            }
        }
    }

    public List<DatPojo> selectPojos() throws SQLException {
        List<DatPojo> result = new ArrayList<>();

        try (PreparedStatement s = con.prepareStatement("SELECT * FROM pojos")) {
            try (ResultSet rs = s.executeQuery()) {
                while (rs.next()) {
                   DatPojo pojo = new DatPojo();

                    pojo.setTestString(rs.getString("test_string"));
                    pojo.setTestInt(rs.getInt("test_int"));
                    pojo.setTestDouble(rs.getDouble("test_double"));
                    pojo.setTestBigDecimal(rs.getBigDecimal("test_bd"));

                    result.add(pojo);
                }
            }
        }

        return result;
    }

    public static void main(String[] args) throws Exception {
        JDBCBenchmark bench = new JDBCBenchmark("blubb");

        MetricRegistry mr = new MetricRegistry();
        Timer timer = mr.timer("benchmark");

        System.out.println("Press any key to start.");
        System.in.read();
        System.out.println("Warmup");
        for (int i = 0; i < 1000; i++) {
            bench.selectPojos();
        }

        System.out.println("Looping");
        for (int i = 0; i < 3000; i++) {
            Timer.Context t = timer.time();
            List pojos = bench.selectPojos();

            long duration = t.stop();
            //System.out.format("got %d pojos\n", pojos.size());
            if (i % 100 == 0) {
                System.out.format("run: %d duration: %d\n", i, duration);
            }
        }
        System.out.println("Completed press any key to quit");
        System.in.read();
        bench.close();

        ConsoleReporter report = ConsoleReporter.forRegistry(mr)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        report.report();
    }
}

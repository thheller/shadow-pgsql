package shadow.pgsql.benchmark;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.openjdk.jmh.annotations.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by zilence on 19.09.15.
 */
@State(Scope.Thread)
public class JDBCBenchmark implements AutoCloseable {

    private Connection con;

    public JDBCBenchmark() {
    }

    @Setup
    public void setup() throws SQLException {
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

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public List<DatPojo> selectPojos() throws SQLException {
        List<DatPojo> result = new ArrayList<>();

        try (PreparedStatement s = con.prepareStatement("SELECT test_int, test_double FROM pojos")) {
            try (ResultSet rs = s.executeQuery()) {
                while (rs.next()) {
                    DatPojo pojo = getDatPojo(rs);

                    result.add(pojo);
                }
            }
        }

        return result;
    }


    private DatPojo getDatPojo(ResultSet rs) throws SQLException {
        DatPojo pojo = new DatPojo();

        // pojo.setTestString(rs.getString("test_string"));
        pojo.setTestInt(rs.getInt("test_int"));
        pojo.setTestDouble(rs.getDouble("test_double"));
        // pojo.setTestBigDecimal(rs.getBigDecimal("test_bd"));
        return pojo;
    }

    public DatPojo selectPojo(int id) throws SQLException {
        try (PreparedStatement s = con.prepareStatement("SELECT * FROM pojos WHERE test_int = ?")) {
            s.setInt(1, id);
            try (ResultSet rs = s.executeQuery()) {
                if (rs.next()) {
                    return getDatPojo(rs);
                } else {
                    throw new IllegalStateException("pojo 1 not found");
                }

            }
        }
    }

    public static void main(String[] args) throws Exception {
        JDBCBenchmark bench = new JDBCBenchmark();

        Random r = new Random();

        MetricRegistry mr = new MetricRegistry();
        Timer timer = mr.timer("shadow/pgsql/benchmark");

        System.out.println("Press any key to start.");
        // System.in.read();

        System.out.println("Looping");
        for (int i = 0; i < 50000; i++) {
            Timer.Context t = timer.time();
            bench.selectPojos();

            long duration = t.stop();
            //System.out.format("got %d pojos\n", pojos.size());
            if (i % 500 == 0) {
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

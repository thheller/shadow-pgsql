package shadow.pgsql.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import shadow.pgsql.SQL;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zilence on 28.09.15.
 */
public class ParseBenchmark {

    private static final String testSQL = "UPDATE something SET a = $1, b=$3 WHERE a=$2";


    @Benchmark
    public void parseManual() {
        SQL.parseParamCount(testSQL);
    }

    private static final Pattern PARAM_PATTERN = Pattern.compile("\\$([0-9]+)", Pattern.MULTILINE | Pattern.DOTALL);

    public static int parseParamCountRegexp(final String sql) {
        Matcher m = PARAM_PATTERN.matcher(sql);
        int paramCount = 0;

        while (m.find()) {
            String d = m.group().substring(1);
            paramCount = Math.max(paramCount, Integer.parseInt(d));
        }
        return paramCount;
    }

    // expected regexp to be slow but not by that much
    // ParseBenchmark.parseManual  thrpt   20  20384728,548 ± 126483,398  ops/s
    // ParseBenchmark.parseRegexp  thrpt   20   1746319,678 ±   9665,410  ops/s

    @Benchmark
    public void parseRegexp() {
        parseParamCountRegexp(testSQL);
    }
}

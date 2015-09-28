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

    @Benchmark
    public void parseToCharArray() {
        parseParamCountCopy(testSQL);
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

    public static int parseParamCountCopy(final String sql) {
        // creates a copy of the internal array
        char[] chars = sql.toCharArray();
        int length = sql.length();
        int paramCount = 0;

        boolean param = false;
        int current = 0;
        for (int i = 0; i < length; i++) {
            char c = chars[i];

            if (param) {
                if (c >= 48 && c <= 57) {
                    current = (current * 10) + (c - 48);

                    if (current > 65536) {
                        throw new IllegalStateException("exceeded maximum parameter count");
                    }
                } else {
                    param = false;
                    paramCount = Math.max(paramCount, current);
                    current = 0;
                }
            } else if (c == '$') {
                param = true;
            }
        }

        // ends in param digits
        if (param) {
            paramCount = Math.max(paramCount, current);
        }

        return paramCount;
    }

    // expected regexp to be slow but not by that much
    // ParseBenchmark.parseManual       thrpt   20  20066966,291 ± 274651,720  ops/s
    // ParseBenchmark.parseRegexp       thrpt   20   1739917,590 ±   9985,324  ops/s
    // ParseBenchmark.parseToCharArray  thrpt   20  15097267,034 ± 190592,764  ops/s

    @Benchmark
    public void parseRegexp() {
        parseParamCountRegexp(testSQL);
    }
}

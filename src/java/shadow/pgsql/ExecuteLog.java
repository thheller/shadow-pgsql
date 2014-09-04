package shadow.pgsql;

import java.util.List;

/**
 * Created by zilence on 02.09.14.
 */
public interface ExecuteLog {
    /**
     * @param nanos
     */
    public void logPrepare(long nanos);

    /**
     * Log the time it took the server to Bind&Execute our Query
     *
     * It includes the time the server spent to execute our query but NOT the time
     * we needed to process all results.
     *
     * @param params
     * @param nanos
     */
    public void logBind(List params, long nanos);

    /**
     * Log the time it took for us to process all results of the Query.
     * <p/>
     * This is usually the raw time it took for the I/O bits since the
     * time spent in executing the actual query is already logged in logBind.
     *
     * @param params
     * @param numRows
     * @param nanos
     */
    public void logExecute(List params, long numRows, long nanos);
}

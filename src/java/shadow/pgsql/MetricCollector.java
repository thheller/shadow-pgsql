package shadow.pgsql;

/**
 * implementations must be thread-safe as this will be called from multiple threads
 */
public interface MetricCollector {
    /**
     * measured time from when we told the server we wanted to execute a query
     * and it telling us it is ready to do so (includes network latency)
     *
     * @param name of the query/statement, may be null
     * @param sql
     * @param nanos
     */
    void collectPrepareTime(String name, String sql, long nanos);

    /**
     * time spent to execute a "portal"
     *
     * this includes:
     * - serializing parameters
     * - -> sending those to server
     * - <- waiting for result
     * - parsing data and constructing result values
     *
     * @param name may be null
     * @param sql
     * @param nanos
     */
    void collectExecuteTime(String name, String sql, long nanos);
}

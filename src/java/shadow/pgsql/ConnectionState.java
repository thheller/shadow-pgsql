package shadow.pgsql;

/**
 * Created by zilence on 10.08.14.
 */
public enum ConnectionState {
    CONNECTED,
    START_UP,
    AUTHENTICATING,
    READY,
    QUERY_OPEN,
    QUERY_CLOSE,
    QUERY_RESULT,
    FUNCTION_CALL,
    CLOSED, ERROR
}

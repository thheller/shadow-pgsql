package shadow.pgsql;

import java.io.IOException;
import java.util.Map;

/**
 * Created by zilence on 10.08.14.
 */
public class CommandException extends IOException {
    private final Map<String, String> errorData;
    private final String causedByQuery;

    public CommandException(String query, Map<String, String> errorData) {
        super(String.format("%s: Query \"%s\" Message: \"%s\"", errorData.get("S"), query, errorData.get("M")));
        this.causedByQuery = query;
        this.errorData = errorData;
    }

    public String getCausedByQuery() {
        return causedByQuery;
    }

    public Map<String, String> getErrorData() {
        return errorData;
    }
}

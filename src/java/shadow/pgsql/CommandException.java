package shadow.pgsql;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zilence on 10.08.14.
 */
public class CommandException extends IOException {
    private final Map<String, String> errorData;

    public CommandException(String message, Throwable cause) {
        super(message, cause);
        this.errorData = new HashMap<>();
    }

    public CommandException(String message) {
        super(message);
        this.errorData = new HashMap<>();
    }

    public CommandException(String message, Map<String, String> errorData) {
        super(String.format("%s [%s]", message, errorData.get("M")));
        this.errorData = errorData;
    }

    public CommandException(String message, Map<String, String> errorData, Throwable t) {
        super(String.format("%s [%s]", message, errorData.get("M")), t);
        this.errorData = errorData;
    }

    public Map<String, String> getErrorData() {
        return errorData;
    }
}

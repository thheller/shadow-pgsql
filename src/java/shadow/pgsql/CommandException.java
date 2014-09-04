package shadow.pgsql;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zilence on 10.08.14.
 */
public class CommandException extends IOException {
    private final Map<String, String> errorData;

    public CommandException(String message) {
        super(message);
        this.errorData = new HashMap<>();
    }

    public CommandException(String message, Throwable cause) {
        super(message, cause);
        this.errorData = new HashMap<>();
    }

    public CommandException(String message, Map<String, String> errorData) {
        super(makeErrorMessage(message, errorData));
        this.errorData = errorData;
    }

    public CommandException(String message, Map<String, String> errorData, Throwable t) {
        super(makeErrorMessage(message, errorData), t);
        this.errorData = errorData;
    }

    public Map<String, String> getErrorData() {
        return errorData;
    }

    private static String makeErrorMessage(String message, Map<String, String> errorData) {
        final StringBuilder sb = new StringBuilder();

        sb.append(message);
        String v = errorData.get("S");
        if (v != null) {
            sb.append("\nSeverity: ").append(v);
        }
        v = errorData.get("M");
        if (v != null) {
            sb.append("\nMessage: ").append(v);
        }
        v = errorData.get("D");
        if (v != null) {
            sb.append("\nDetail: ").append(v);
        }
        v = errorData.get("s");
        if (v != null) {
            sb.append("\nSchema: ").append(v);
        }
        v = errorData.get("t");
        if (v != null) {
            sb.append("\nTable: ").append(v);
        }
        v = errorData.get("c");
        if (v != null) {
            sb.append("\nColumn: ").append(v);
        }

        return sb.toString();
    }

}

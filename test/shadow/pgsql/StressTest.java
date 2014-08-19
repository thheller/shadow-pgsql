package shadow.pgsql;

import org.junit.Test;

import java.io.IOException;

/**
 * Created by zilence on 19.08.14.
 */
public class StressTest {

    @Test
    public void testConnect() throws IOException {
        for (int i = 0; i < 500; i++) {
            // Database.setup("localhost", 5432, "zilence", "cms");
        }
    }
}

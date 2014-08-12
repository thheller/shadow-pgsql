package shadow.pgsql;

/**
* Created by zilence on 09.08.14.
*/
public interface ResultBuilder<ACC, RESULT, ROW> {
    public ACC init();

    public ACC add(ACC state, ROW row);

    public RESULT complete(ACC state);
}

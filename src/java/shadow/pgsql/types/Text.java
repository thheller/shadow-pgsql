package shadow.pgsql.types;

/**
 * Created by zilence on 11.08.14.
 */
public class Text extends AbstractText {

    private final int oid;

    public Text(int oid) {
        this.oid = oid;
    }

    @Override
    public int getTypeOid() {
        return oid;
    }

    @Override
    public String asString(Object param) {
        return param.toString();
    }

    @Override
    public Object fromString(String input) {
        return input;
    }
}

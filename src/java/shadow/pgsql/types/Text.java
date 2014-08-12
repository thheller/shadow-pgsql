package shadow.pgsql.types;

/**
 * Created by zilence on 11.08.14.
 */
public class Text extends AbstractText {
    public static final Text NAME = new Text(19);
    public static final Text TEXT = new Text(25);
    public static final Text CHAR = new Text(1042);
    public static final Text VARCHAR = new Text(1043);

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

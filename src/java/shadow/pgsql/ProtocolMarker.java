package shadow.pgsql;

/**
 * Created by zilence on 27.09.15.
 */
public class ProtocolMarker {
    final ProtocolOutput out;
    final int position;
    final int size;
    final boolean includeSize;

    public ProtocolMarker(ProtocolOutput out, int position, int size, boolean includeSize) {
        this.out = out;
        this.position = position;
        this.size = size;
        this.includeSize = includeSize;
    }

    public void complete() {
        out.completeCommand(this);
    }
}

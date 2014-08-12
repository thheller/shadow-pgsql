package shadow.pgsql;

/**
* Created by zilence on 09.08.14.
*/
public class ColumnInfo {
    public final String name;
    public final int tableOid;
    public final int positionInTable;
    public final int typeOid;
    public final int typeSize;
    public final int typeMod;

    public ColumnInfo(String name, int tableOid, int positionInTable, int typeOid, int typeSize, int typeMod) {
        this.name = name;
        this.tableOid = tableOid;
        this.positionInTable = positionInTable;
        this.typeOid = typeOid;
        this.typeSize = typeSize;
        this.typeMod = typeMod;
    }
}

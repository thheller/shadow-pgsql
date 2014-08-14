package shadow.pgsql.types;

import shadow.pgsql.Connection;
import shadow.pgsql.ColumnInfo;
import shadow.pgsql.ProtocolOutput;
import shadow.pgsql.TypeHandler;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Created by zilence on 10.08.14.
 */
public class TypedArray implements TypeHandler {
    private final TypeHandler itemType;
    private final ArrayReader arrayReader;

    public TypedArray(TypeHandler itemType, ArrayReader arrayReader) {
        if (!itemType.supportsBinary()) {
            throw new IllegalArgumentException("only binary arrays implemented");
        }

        this.itemType = itemType;
        this.arrayReader = arrayReader;
    }

    @Override
    public int getTypeOid() {
        return this.itemType.getTypeOid();
    }

    @Override
    public boolean supportsBinary() {
        return this.itemType.supportsBinary();
    }

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        throw new AbstractMethodError("TBD");
    }

    @Override
    public String encodeToString(Connection con, Object param) {
        throw new AbstractMethodError("TBD");
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, int colSize) throws IOException {
        final DataInputStream in = con.input.stream;

        final int dimensions = in.readInt();
        final boolean hasNull = in.readInt() == 1;
        final int elementOid = in.readInt();

        int itemCount = 0;

        int[] dimensionSizes = new int[dimensions];
        int[] dimensionBounds = new int[dimensions];

        for (int i = 0; i < dimensions; i++) {
            int dimensionSize = dimensionSizes[i] = in.readInt();
            dimensionBounds[i] = in.readInt();
            itemCount += dimensionSize;
        }

        if (dimensions == 1) {
            Object result = arrayReader.init(dimensionSizes[0]);
            for (int i = 0; i < itemCount; i++) {
                final int itemSize = in.readInt();
                if (itemSize == -1) {
                    result = arrayReader.addNull(result, i);
                } else {
                    result = arrayReader.add(result, i, itemType.decodeBinary(con, field, itemSize));
                }
            }

            return arrayReader.complete(result);
        } else {
            throw new UnsupportedOperationException("FIXME: implement 2-dim array");
        }
    }

    @Override
    public Object decodeString(Connection con, ColumnInfo field, String value) {
        throw new AbstractMethodError("TBD");
    }


}

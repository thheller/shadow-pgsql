package shadow.pgsql.types;

import shadow.pgsql.Connection;
import shadow.pgsql.ColumnInfo;
import shadow.pgsql.ProtocolOutput;
import shadow.pgsql.TypeHandler;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Array;

/**
 * Created by zilence on 10.08.14.
 */
// FIXME: this is probably a reflection nightmare
public class TypedArray implements TypeHandler {
    private final int oid;
    private final TypeHandler itemType;
    private final ArrayReader arrayReader;

    public static ArrayReader makeReader(Class elementType) {
        return new ArrayReader() {
            @Override
            public Object init(int size) {
                return Array.newInstance(elementType, size);
            }

            @Override
            public Object add(Object arr, int index, Object item) {
                Array.set(arr, index, item);
                return arr;
            }

            @Override
            public Object addNull(Object arr, int index) {
                throw new IllegalArgumentException("null not supported in array");
            }

            @Override
            public Object complete(Object arr) {
                return arr;
            }
        };
    }

    public TypedArray(int oid, TypeHandler itemType, ArrayReader arrayReader) {
        if (!itemType.supportsBinary()) {
            throw new IllegalArgumentException("only binary arrays implemented");
        }

        if (arrayReader == null) {
            throw new IllegalArgumentException("Need ArrayReader");
        }

        this.oid = oid;
        this.itemType = itemType;
        this.arrayReader = arrayReader;
    }

    @Override
    public int getTypeOid() {
        return oid;
    }

    @Override
    public boolean supportsBinary() {
        return this.itemType.supportsBinary();
    }

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        if (!param.getClass().isArray()) {
            throw new IllegalArgumentException(String.format("param is not an array: %s", param.getClass().getName()));
        }

        int dimensions = 1;

        output.int32(dimensions);
        output.int32(0); // hasnull FIXME: check nulls
        output.int32(itemType.getTypeOid()); // element oid

        int lbound = 1;

        for (int i = 0; i < dimensions; i++) {
            int length = Array.getLength(param);
            output.int32(length); // dimension size
            output.int32(lbound); // lower bound?
            lbound += length;
        }

        int length = Array.getLength(param);
        for (int i = 0; i < length; i++) {
            Object value = Array.get(param, i);
            if (value == null) {
                // output.int32(-1);
                throw new IllegalArgumentException("array has nulls");
            } else {
                output.beginExclusive();
                itemType.encodeBinary(con, output, value);
                output.complete();
            }
        }
    }

    @Override
    public String encodeToString(Connection con, Object param) {
        throw new UnsupportedOperationException("TBD");
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
        throw new UnsupportedOperationException("TBD");
    }


}

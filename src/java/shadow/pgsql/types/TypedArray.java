package shadow.pgsql.types;

import shadow.pgsql.ColumnInfo;
import shadow.pgsql.Connection;
import shadow.pgsql.ProtocolOutput;
import shadow.pgsql.TypeHandler;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collection;

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

    public TypedArray(TypeHandler itemType, ArrayReader arrayReader) {
        if (!itemType.supportsBinary()) {
            throw new IllegalArgumentException("only binary arrays implemented");
        }

        if (arrayReader == null) {
            throw new IllegalArgumentException("Need ArrayReader");
        }

        this.oid = arrayOidForType(itemType);
        this.itemType = itemType;
        this.arrayReader = arrayReader;
    }

    public static int arrayOidForType(TypeHandler type) {
        switch (type.getTypeOid()) {
            case Types.OID_INT4:
                return 1007;
            case Types.OID_INT2:
                return 1005;
            case Types.OID_INT8:
                return 1016;
            case Types.OID_TEXT:
                return 1009;
            case Types.OID_VARCHAR:
                return 1015;
            default:
                throw new IllegalArgumentException(String.format("don't know array oid for type: [%d,%s]", type.getTypeOid(), type.getClass().getName()));
        }
    }

    @Override
    public int getTypeOid() {
        return oid;
    }

    @Override
    public boolean supportsBinary() {
        return true; // binary-only actually
    }

    @Override
    public void encodeBinary(Connection con, ProtocolOutput output, Object param) {
        if (param instanceof Collection) {
            Collection coll = (Collection) param;
            int length = coll.size();

            // FIXME: 2 dim (List of Lists?)
            int dimensions = 1;

            output.int32(dimensions);
            output.int32(0); // hasnull FIXME: check nulls
            output.int32(itemType.getTypeOid()); // element oid

            int lbound = 1;

            for (int i = 0; i < dimensions; i++) {
                output.int32(length); // dimension size
                output.int32(lbound); // lower bound?
                lbound += length;
            }

            for (Object value : coll) {
                if (value == null) {
                    // output.int32(-1);
                    throw new IllegalArgumentException("array has nulls");
                } else {
                    output.beginExclusive();
                    itemType.encodeBinary(con, output, value);
                    output.complete();
                }
            }
        } else if (param.getClass().isArray()) {
            // FIXME: 2 dim
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
        } else {
            throw new IllegalArgumentException(String.format("param is not an array or Collection: %s", param.getClass().getName()));
        }
    }

    @Override
    public String encodeToString(Connection con, Object param) {
        throw new UnsupportedOperationException("TBD");
    }

    @Override
    public Object decodeBinary(Connection con, ColumnInfo field, ByteBuffer buf, int colSize) throws IOException {
        final int dimensions = buf.getInt();
        final boolean hasNull = buf.getInt() == 1;
        final int elementOid = buf.getInt();

        int itemCount = 0;

        int[] dimensionSizes = new int[dimensions];
        int[] dimensionBounds = new int[dimensions];

        for (int i = 0; i < dimensions; i++) {
            int dimensionSize = dimensionSizes[i] = buf.getInt();
            dimensionBounds[i] = buf.getInt();
            itemCount += dimensionSize;
        }

        if (dimensions == 1) {
            Object result = arrayReader.init(dimensionSizes[0]);
            for (int i = 0; i < itemCount; i++) {
                final int itemSize = buf.getInt();
                if (itemSize == -1) {
                    result = arrayReader.addNull(result, i);
                } else {
                    result = arrayReader.add(result, i, itemType.decodeBinary(con, field, buf, itemSize));
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

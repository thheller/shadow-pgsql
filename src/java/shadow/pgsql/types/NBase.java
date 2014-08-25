package shadow.pgsql.types;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Created by zilence on 25.08.14.
 */

// FIXME: I wish I was good at math, need to find efficient way to convert this
public class NBase {
    private final static BigInteger NBASE = BigInteger.valueOf(10000);

    private final static int DEC_DIGITS = 4;
    private final static int NUMERIC_POS = 0x0000;
    private static final short NUMERIC_NEG = 0x4000;
    private static final int NUMERIC_SHORT = 0x8000;
    private static final int NUMERIC_NAN = 0xC000;

    public final short weight;
    public final short sign;
    public final short dscale;
    public final short[] digits;

    NBase(short weight, short sign, short dscale, short[] digits) {
        this.weight = weight;
        this.sign = sign;
        this.dscale = dscale;
        this.digits = digits;
    }

    public static BigDecimal unpack(short weight, short sign, short dscale, short[] digits) {
        throw new UnsupportedOperationException("figure this out");
    }

    public static NBase pack(BigDecimal bd) {
        throw new UnsupportedOperationException("figure this out");
    }
}

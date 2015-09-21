package shadow.pgsql.types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zilence on 25.08.14.
 */

// FIXME: this is WAY TOOOO SLOW!! do some actual math instead of this string crap
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

    // there is no way that this stuff is efficient
    // brain is too fried to come up with something better
    // because maths

    public static NBase pack(BigDecimal bd) {
        BigDecimal abs = bd.abs();
        String s = abs.stripTrailingZeros().toPlainString();

        short sign = bd.signum() > 0 ? NUMERIC_POS : NUMERIC_NEG;
        short dscale = new Integer(abs.scale()).shortValue();

        int dot = s.indexOf(".");

        String sig;
        String dec;
        if (dot == -1) {
            sig = s;
            dec = "";
        } else {
            sig = s.substring(0, dot);
            dec = s.substring(dot + 1);
        }

        // re-append zeros so we get a multiple of 4 length (shorts between 0-9999)
        switch (dec.length() % 4) {
            case 1:
                dec += "000";
                break;
            case 2:
                dec += "00";
                break;
            case 3:
                dec += "0";
                break;
            case 0:
                break;
        }

        int decLength = dec.length(); // is now always multiple of 4
        int sigLength = sig.length();

        int digitCount = new Double((decLength / 4) + Math.ceil(sigLength / 4.0)).intValue();
        int digitIndex = digitCount - 1;

        short[] digits = new short[digitCount];

        for (int i = decLength; i > 0; i -= 4) {
            String digit = dec.substring(i - 4, i);
            digits[digitIndex--] = Short.parseShort(digit);
        }

        short weight = -1;
        for (int i = sigLength; i > 0; i -= 4) {
            weight++;

            String digit = sig.substring(Math.max(0, i - 4), i);
            digits[digitIndex--] = Short.parseShort(digit);
        }

        int zeros = 0;
        for (int i = 0; i < digitCount; i++) {
            if (digits[i] == 0) {
                zeros++;
            } else {
                break;
            }
        }

        if (zeros > 0) {
            weight -= zeros;

            short[] adjusted = new short[digitCount - zeros];
            System.arraycopy(digits, zeros, adjusted, 0, adjusted.length);
            digits = adjusted;
        }

        return new NBase(weight, sign, dscale, digits);
    }

    public BigDecimal unpack() {
        // feels stupid to go over a String?
        // probably faster to do actual math?
        // internal repr in java very different from pg

        if (weight == 0 && digits.length == 0) {
            return BigDecimal.ZERO.movePointLeft(dscale);
        }

        StringBuilder sb = new StringBuilder();
        if (sign == NUMERIC_NEG) {
            sb.append("-");
        }

        if (weight < 0) {
            sb.append("0");
        } else if (weight == 0) {
            sb.append(digits[0]);
        } else {
            for (int i = 0; i <= weight; i++) {
                if (i == 0) {
                    // don't want leading zeros
                    sb.append(digits[i]);
                } else {
                    appendDigit(sb, digits[i]);
                }
            }
        }

        sb.append(".");
        int decimalPointIndex = sb.length();

        for (int i = weight + 1; i < digits.length; i++) {
            if (i < 0) {
                sb.append("0000");
            } else {
                appendDigit(sb, digits[i]);
            }
        }

        int trailingZeros = dscale - (sb.length() - decimalPointIndex);
        if (trailingZeros < 0) {
            sb.setLength(sb.length() + trailingZeros);
        } else if (trailingZeros > 0) {
            for (int i = 0; i < trailingZeros; i++) {
                sb.append("0");
            }
        }

        return new BigDecimal(sb.toString());
    }

    private static void appendDigit(StringBuilder sb, short digit) {
        if (digit < 10) {
            sb.append("000");
        } else if (digit < 100) {
            sb.append("00");
        } else if (digit < 1000) {
            sb.append("0");
        }
        sb.append(digit);
    }
}

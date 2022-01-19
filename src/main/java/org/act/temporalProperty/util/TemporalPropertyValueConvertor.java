package org.act.temporalProperty.util;

import com.google.common.base.Preconditions;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.meta.ValueContentType;

import java.nio.charset.Charset;

public class TemporalPropertyValueConvertor
{
    public static final String TemporalPropertyMarker = "TGRAPH_TEMPORAL_PROPERTY";
    public static final String CLASS_NAME_LENGTH_SEPERATOR = "%";
    
    private static final String INT_NAME = "Integer";
    private static final String CHAR_NAME = "Character";
    private static final String DOUBLE_NAME = "Double";
    private static final String FLOAT_NAME = "Float";
    private static final String LONG_NAME = "Long";
    private static final String BYTE_NAME = "Byte";
    private static final String STRING_NAME = "String";
    private static final String SHORT_NAME = "Short";
    
    private static final String INT_ARRAY_NAME = "int[]";
    private static final String CHAR_ARRAY_NAME = "char[]";
    private static final String DOUBLE_ARRAY_NAME = "double[]";
    private static final String FLOAT_ARRAY_NAME = "float[]";
    private static final String LONG_ARRAY_NAME = "long[]";
    private static final String BYTE_ARRAY_NAME = "byte[]";
    private static final String STRING_ARRAY_NAME = "String[]";
    private static final String SHORT_ARRAY_NAME = "short[]";

    public static Slice toSlice( Object value )
    {
        SliceOutput o = new DynamicSliceOutput( 8 );
        if(value instanceof Integer) o.writeInt( (Integer) value );
        else if(value instanceof Double) o.writeDouble( (Double) value );
        else if(value instanceof Float) o.writeFloat( (Float) value );
        else if(value instanceof Long) o.writeLong( (Long) value );
        else if(value instanceof String) o.writeBytes( ((String) value).getBytes() );
        else throw new UnsupportedOperationException("Unsupported value type");
        return o.slice();
    }

    public static Object fromSlice(String className, Slice value)
    {
        SliceInput in = value.input();
        switch ( className )
        {
        case INT_NAME:
            return in.readInt();
        case DOUBLE_NAME:
            return in.readDouble();
        case FLOAT_NAME:
            return in.readFloat();
        case LONG_NAME:
            return in.readLong();
        case STRING_NAME:
            return value.toString();
        case BYTE_NAME:
        case CHAR_NAME:
        case INT_ARRAY_NAME:
        case CHAR_ARRAY_NAME:
        case DOUBLE_ARRAY_NAME:
        case FLOAT_ARRAY_NAME:
        case LONG_ARRAY_NAME:
        case BYTE_ARRAY_NAME:
        case STRING_ARRAY_NAME:
        case SHORT_NAME:
        case SHORT_ARRAY_NAME:
        default:
            throw new UnsupportedOperationException("Unsupported value type");
        }
    }

    public static Object fromSlice(ValueContentType valueType, Slice value)
    {
        SliceInput in = value.input();
        switch ( valueType )
        {
        case INT:
            return in.readInt();
        case DOUBLE:
            return in.readDouble();
        case FLOAT:
            return in.readFloat();
        case LONG:
            return in.readLong();
        case STRING:
            return in.toString( Charset.defaultCharset() );
        default:
            throw new UnsupportedOperationException("Unsupported value type");
        }
    }

    public static ValueContentType str2type(String className)
    {
        switch (className) {
        case INT_NAME:
            return ValueContentType.INT;
        case DOUBLE_NAME:
            return ValueContentType.DOUBLE;
        case FLOAT_NAME:
            return ValueContentType.FLOAT;
        case LONG_NAME:
            return ValueContentType.LONG;
        case STRING_NAME:
            return ValueContentType.STRING;
        case BYTE_NAME:
        case CHAR_NAME:
        case INT_ARRAY_NAME:
        case FLOAT_ARRAY_NAME:
        case CHAR_ARRAY_NAME:
        case DOUBLE_ARRAY_NAME:
        case LONG_ARRAY_NAME:
        case BYTE_ARRAY_NAME:
        case STRING_ARRAY_NAME:
        case SHORT_NAME:
        case SHORT_ARRAY_NAME:
        default:
            throw new UnsupportedOperationException("Unsupported value type");
        }
    }
    
//    public static int convert( byte[] result, Object value )
//    {
//        switch (value.getClass().getSimpleName()) {
//            case INT_NAME:
//                return intConvert(result, (int) value);
//            case CHAR_NAME:
//                return charConvert(result, (char) value);
//            case DOUBLE_NAME:
//                return doubleConvert(result, (double) value);
//            case FLOAT_NAME:
//                return floatConvert(result, (float) value);
//            case LONG_NAME:
//                return longConvert(result, (long) value);
//            case BYTE_NAME:
//                return byteConvert(result, (byte) value);
//            case STRING_NAME:
//                return stringConvert(result, (String) value);
//            case INT_ARRAY_NAME:
//                return intArrayConvert(result, (int[]) value);
//            case CHAR_ARRAY_NAME:
//                return charArrayConvert(result, (char[]) value);
//            case DOUBLE_ARRAY_NAME:
//                return doubleArrayConvert(result, (double[]) value);
//            case FLOAT_ARRAY_NAME:
//                return floatArrayConvert(result, (float[]) value);
//            case LONG_ARRAY_NAME:
//                return longArrayConvert(result, (long[]) value);
//            case BYTE_ARRAY_NAME:
//                return byteArrayConvert(result, (byte[]) value);
//            case STRING_ARRAY_NAME:
//                return StringArrayConvert(result, (String[]) value);
//            case SHORT_NAME:
//                return shortConvert(result, (short) value);
//            case SHORT_ARRAY_NAME:
//                return shortArrayConvert(result, (short[]) value);
//            default:
//                throw new RuntimeException("Unsupported value type");
//        }
//    }
    
//    public static Object revers( String claszname, byte[] value )
//    {
//        if( claszname.equals( INT_NAME ) )
//        {
//            return toInt( value );
//        }
//        else if( claszname.equals( CHAR_NAME ) )
//        {
//            return toChar( value );
//        }
//        else if( claszname.equals( DOUBLE_NAME ) )
//        {
//            return toDouble( value );
//        }
//        else if( claszname.equals( FLOAT_NAME ) )
//        {
//            return toFloat( value );
//        }
//        else if( claszname.equals( LONG_NAME ) )
//        {
//            return toLong( value );
//        }
//        else if( claszname.equals( BYTE_NAME ) )
//        {
//            return toByte( value );
//        }
//        else if( claszname.equals( STRING_NAME ) )
//        {
//            return toString( value );
//        }
//        else if( claszname.equals( INT_ARRAY_NAME ) )
//        {
//            return toIntA( value );
//        }
//        else if( claszname.equals( CHAR_ARRAY_NAME ) )
//        {
//            return toCharA( value );
//        }
//        else if( claszname.equals( DOUBLE_ARRAY_NAME ) )
//        {
//            return toDoubleA( value );
//        }
//        else if( claszname.equals( FLOAT_ARRAY_NAME ) )
//        {
//            return toFloatA( value );
//        }
//        else if( claszname.equals( LONG_ARRAY_NAME ) )
//        {
//            return toLongA( value );
//        }
//        else if( claszname.equals( BYTE_ARRAY_NAME ) )
//        {
//            return toByteA( value );
//        }
//        else if( claszname.equals( STRING_ARRAY_NAME ) )
//        {
//            return toStringA( value );
//        }
//        else if( claszname.equals( SHORT_NAME ) )
//        {
//            return toShort( value );
//        }
//        else if( claszname.equals( SHORT_ARRAY_NAME ) )
//        {
//            return toShortA( value );
//        }
//        throw new RuntimeException("Unsupported value type");
//    }
//
////    private static int shortArrayConvert( byte[] result, short[] value )
////    {
////        byte[] head = toByta( value );
////        Preconditions.checkArgument( head.length <= result.length,"The value length should not larger than the max length" );
////        for( int i = 0; i<head.length; i++ )
////        {
////            result[i] = head[i];
////        }
////        return head.length;
////    }
////
////    private static int shortConvert( byte[] result, short value )
////    {
////        byte[] head = toByta( value );
////        Preconditions.checkArgument( head.length <= result.length,"The value length should not larger than the max length" );
////        for( int i = 0; i<head.length; i++ )
////        {
////            result[i] = head[i];
////        }
////        return head.length;
////    }
////
////    private static int StringArrayConvert( byte[] result, String[] value )
////    {
////        byte[] head = toByta( value );
////        Preconditions.checkArgument( head.length <= result.length,"The value length should not larger than the max length" );
////        for( int i = 0; i<head.length; i++ )
////        {
////            result[i] = head[i];
////        }
////        return head.length;
////    }
////
////    private static int byteArrayConvert( byte[] result, byte[] value )
////    {
////        byte[] head = toByta( value );
////        Preconditions.checkArgument( head.length <= result.length,"The value length should not larger than the max length" );
////        for( int i = 0; i<head.length; i++ )
////        {
////            result[i] = head[i];
////        }
////        return head.length;
////    }
////
////    private static int longArrayConvert( byte[] result, long[] value )
////    {
////        byte[] head = toByta( value );
////        Preconditions.checkArgument( head.length <= result.length,"The value length should not larger than the max length" );
////        for( int i = 0; i<head.length; i++ )
////        {
////            result[i] = head[i];
////        }
////        return head.length;
////    }
////
////    private static int floatArrayConvert( byte[] result, float[] value )
////    {
////        byte[] head = toByta( value );
////        Preconditions.checkArgument( head.length <= result.length,"The value length should not larger than the max length" );
////        for( int i = 0; i<head.length; i++ )
////        {
////            result[i] = head[i];
////        }
////        return head.length;
////    }
////
////    private static int doubleArrayConvert( byte[] result, double[] value )
////    {
////        byte[] head = toByta( value );
////        Preconditions.checkArgument( head.length <= result.length,"The value length should not larger than the max length" );
////        for( int i = 0; i<head.length; i++ )
////        {
////            result[i] = head[i];
////        }
////        return head.length;
////    }
////
////    private static int charArrayConvert( byte[] result, char[] value )
////    {
////        byte[] head = toByta( value );
////        Preconditions.checkArgument( head.length <= result.length,"The value length should not larger than the max length" );
////        for( int i = 0; i<head.length; i++ )
////        {
////            result[i] = head[i];
////        }
////        return head.length;
////    }
////
////    private static int intArrayConvert( byte[] result, int[] value )
////    {
////        byte[] head = toByta( value );
////        Preconditions.checkArgument( head.length <= result.length,"The value length should not larger than the max length" );
////        for( int i = 0; i<head.length; i++ )
////        {
////            result[i] = head[i];
////        }
////        return head.length;
////    }
////
////    private static int stringConvert( byte[] result, String value )
////    {
////        byte[] head = toByta( value );
////        Preconditions.checkArgument( head.length <= result.length,"The value length should not larger than the max length" );
////        for( int i = 0; i<head.length; i++ )
////        {
////            result[i] = head[i];
////        }
////        return head.length;
////    }
////
////    private static int byteConvert( byte[] result, byte value )
////    {
////        byte[] head = toByta( value );
////        Preconditions.checkArgument( head.length <= result.length,"The value length should not larger than the max length" );
////        for( int i = 0; i<head.length; i++ )
////        {
////            result[i] = head[i];
////        }
////        return head.length;
////    }
////
////    private static int longConvert( byte[] result, long value )
////    {
////        byte[] head = toByta( value );
////        Preconditions.checkArgument( head.length <= result.length,"The value length should not larger than the max length" );
////        for( int i = 0; i<head.length; i++ )
////        {
////            result[i] = head[i];
////        }
////        return head.length;
////    }
////
////    private static int floatConvert( byte[] result, float value )
////    {
////        byte[] head = toByta( value );
////        Preconditions.checkArgument( head.length <= result.length,"The value length should not larger than the max length" );
////        for( int i = 0; i<head.length; i++ )
////        {
////            result[i] = head[i];
////        }
////        return head.length;
////    }
////
////    private static int doubleConvert( byte[] result, double value )
////    {
////        byte[] head = toByta( value );
////        Preconditions.checkArgument( head.length <= result.length,"The value length should not larger than the max length" );
////        for( int i = 0; i<head.length; i++ )
////        {
////            result[i] = head[i];
////        }
////        return head.length;
////    }
////
////    private static int charConvert( byte[] result, char value )
////    {
////        byte[] head = toByta( value );
////        Preconditions.checkArgument( head.length <= result.length,"The value length should not larger than the max length" );
////        for( int i = 0; i<head.length; i++ )
////        {
////            result[i] = head[i];
////        }
////        return head.length;
////    }
////
////    private static int intConvert( byte[] result, int value )
////    {
////        byte[] head = toByta( value );
////        Preconditions.checkArgument( head.length <= result.length,"The value length should not larger than the max length" );
////        for( int i = 0; i<head.length; i++ )
////        {
////            result[i] = head[i];
////        }
////        return head.length;
////    }
//
//    /* ========================= */
//    /* "primitive type --> byte[] data" Methods */
//    /* ========================= */
//    private static byte[] toByta(byte data)
//    {
//        return new byte[]{data};
//    }
//    private static byte[] toByta(byte[] data)
//    {
//        return data;
//    }
//    /* ========================= */
//    private static byte[] toByta(short data)
//    {
//        return new byte[] {
//            (byte)((data >> 8) & 0xff),
//            (byte)((data >> 0) & 0xff),
//        };
//    }
//    private static byte[] toByta(short[] data)
//    {
//        if (data == null) return null;
//        // ----------
//        byte[] byts = new byte[data.length * 2];
//        for (int i = 0; i < data.length; i++)
//            System.arraycopy(toByta(data[i]), 0, byts, i * 2, 2);
//        return byts;
//    }
//    /* ========================= */
//    private static byte[] toByta(char data)
//    {
//        return new byte[] {
//            (byte)((data >> 8) & 0xff),
//            (byte)((data >> 0) & 0xff),
//        };
//    }
//    private static byte[] toByta(char[] data)
//    {
//        if (data == null) return null;
//        // ----------
//        byte[] byts = new byte[data.length * 2];
//        for (int i = 0; i < data.length; i++)
//            System.arraycopy(toByta(data[i]), 0, byts, i * 2, 2);
//        return byts;
//    }
//    /* ========================= */
//    private static byte[] toByta(int data)
//    {
//        return new byte[] {
//            (byte)((data >> 24) & 0xff),
//            (byte)((data >> 16) & 0xff),
//            (byte)((data >> 8) & 0xff),
//            (byte)((data >> 0) & 0xff),
//        };
//    }
//    private static byte[] toByta(int[] data)
//    {
//        if (data == null) return null;
//        // ----------
//        byte[] byts = new byte[data.length * 4];
//        for (int i = 0; i < data.length; i++)
//            System.arraycopy(toByta(data[i]), 0, byts, i * 4, 4);
//        return byts;
//    }
//    /* ========================= */
//    private static byte[] toByta(long data)
//    {
//        return new byte[] {
//            (byte)((data >> 56) & 0xff),
//            (byte)((data >> 48) & 0xff),
//            (byte)((data >> 40) & 0xff),
//            (byte)((data >> 32) & 0xff),
//            (byte)((data >> 24) & 0xff),
//            (byte)((data >> 16) & 0xff),
//            (byte)((data >> 8) & 0xff),
//            (byte)((data >> 0) & 0xff),
//        };
//    }
//    private static byte[] toByta(long[] data)
//    {
//        if (data == null) return null;
//        // ----------
//        byte[] byts = new byte[data.length * 8];
//        for (int i = 0; i < data.length; i++)
//            System.arraycopy(toByta(data[i]), 0, byts, i * 8, 8);
//        return byts;
//    }
//    /* ========================= */
//    private static byte[] toByta(float data)
//    {
//        return toByta(Float.floatToRawIntBits(data));
//    }
//    private static byte[] toByta(float[] data)
//    {
//        if (data == null) return null;
//        // ----------
//        byte[] byts = new byte[data.length * 4];
//        for (int i = 0; i < data.length; i++)
//            System.arraycopy(toByta(data[i]), 0, byts, i * 4, 4);
//        return byts;
//    }
//    /* ========================= */
//    private static byte[] toByta(double data)
//    {
//        return toByta(Double.doubleToRawLongBits(data));
//    }
//    private static byte[] toByta(double[] data)
//    {
//        if (data == null) return null;
//        // ----------
//        byte[] byts = new byte[data.length * 8];
//        for (int i = 0; i < data.length; i++)
//            System.arraycopy(toByta(data[i]), 0, byts, i * 8, 8);
//        return byts;
//    }
//    /* ========================= */
//    private static byte[] toByta(boolean data)
//    {
//        return new byte[]{(byte)(data ? 0x01 : 0x00)}; // bool -> {1 byte}
//    }
//    private static byte[] toByta(boolean[] data)
//    {
//        if (data == null) return null;
//        // ----------
//        int len = data.length;
//        byte[] lena = toByta(len); // int conversion; length array = lena
//        byte[] byts = new byte[lena.length + (len / 8) + (len % 8 != 0 ? 1 : 0)];
//        // (Above) length-array-length + sets-of-8-booleans +? byte-for-remainder
//        System.arraycopy(lena, 0, byts, 0, lena.length);
//        // ----------
//        // (Below) algorithm by Matthew Cudmore: boolean[] -> bits -> byte[]
//        for (int i = 0, j = lena.length, k = 7; i < data.length; i++)
//        {
//            byts[j] |= (data[i] ? 1 : 0) << k--;
//            if (k < 0) { j++; k = 7; }
//        }
//        // ----------
//        return byts;
//    }
//    /* ========================= */
//    private static byte[] toByta(String data)
//    {
//        return (data == null) ? null : data.getBytes();
//    }
//    private static byte[] toByta(String[] data)
//    {
//        // Advanced Technique: Generates an indexed byte array
//        // which contains the array of Strings. The byte array
//        // contains information about the number of Strings and
//        // the length of each String.
//        // ----------
//        if (data == null) return null;
//        // ---------- flags:
//        int totalLength = 0; // Measure length of final byte array
//        int bytesPos = 0; // Used later
//        // ----- arrays:
//        byte[] dLen = toByta(data.length); // byte array of data length
//        totalLength += dLen.length;
//        int[] sLens = new int[data.length]; // String lengths = sLens
//        totalLength += (sLens.length * 4);
//        byte[][] strs = new byte[data.length][]; // array of String bytes
//        // ----- pack strs:
//        for (int i = 0; i < data.length; i++)
//        {
//            if (data[i] != null) {
//                strs[i] = toByta(data[i]);
//                sLens[i] = strs[i].length;
//                totalLength += strs[i].length;
//            } else {
//                sLens[i] = 0;
//                strs[i] = new byte[0]; // prevent null entries
//            }
//        }
//        // ----------
//        byte[] bytes = new byte[totalLength]; // final array
//        System.arraycopy(dLen, 0, bytes, 0, 4);
//        byte[] bsLens = toByta(sLens); // byte version of String sLens
//        System.arraycopy(bsLens, 0, bytes, 4, bsLens.length);
//        // -----
//        bytesPos += 4 + bsLens.length; // mark position
//        // -----
//        for (byte[] sba : strs)
//        {
//            System.arraycopy(sba, 0, bytes, bytesPos, sba.length);
//            bytesPos += sba.length;
//        }
//        // ----------
//        return bytes;
//    }
//    private static byte toByte(byte[] data)
//    {
//        return (data == null || data.length == 0) ? 0x0 : data[0];
//    }
//    private static byte[] toByteA(byte[] data)
//    {
//        return data;
//    }
//    /* ========================= */
//    private static short toShort(byte[] data)
//    {
//        if (data == null || data.length != 2) return 0x0;
//        // ----------
//        return (short)(
//                (0xff & data[0]) << 8   |
//                (0xff & data[1]) << 0
//                );
//    }
//    private static short[] toShortA(byte[] data)
//    {
//        if (data == null || data.length % 2 != 0) return null;
//        // ----------
//        short[] shts = new short[data.length / 2];
//        for (int i = 0; i < shts.length; i++)
//        {
//            shts[i] = toShort( new byte[]
//                    {
//                data[(i*2)],
//                data[(i*2)+1]
//            } );
//        }
//        return shts;
//    }
//    /* ========================= */
//    private static char toChar(byte[] data)
//    {
//        if (data == null || data.length != 2) return 0x0;
//        // ----------
//        return (char)(
//                (0xff & data[0]) << 8   |
//                (0xff & data[1]) << 0
//                );
//    }
//    private static char[] toCharA(byte[] data)
//    {
//        if (data == null || data.length % 2 != 0) return null;
//        // ----------
//        char[] chrs = new char[data.length / 2];
//        for (int i = 0; i < chrs.length; i++)
//        {
//            chrs[i] = toChar( new byte[]
//                    {
//                data[(i*2)],
//                data[(i*2)+1],
//            } );
//        }
//        return chrs;
//    }
//    /* ========================= */
//    private static int toInt(byte[] data)
//    {
//        if (data == null || data.length != 4) return 0x0;
//        // ----------
//        return (int)(
//                (0xff & data[0]) << 24  |
//                (0xff & data[1]) << 16  |
//                (0xff & data[2]) << 8   |
//                (0xff & data[3]) << 0
//                );
//    }
//    private static int[] toIntA(byte[] data)
//    {
//        if (data == null || data.length % 4 != 0) return null;
//        // ----------
//        int[] ints = new int[data.length / 4];
//        for (int i = 0; i < ints.length; i++)
//            ints[i] = toInt( new byte[]
//                    {
//                data[(i*4)],
//                data[(i*4)+1],
//                data[(i*4)+2],
//                data[(i*4)+3],
//            } );
//        return ints;
//    }
//    /* ========================= */
//    private static long toLong(byte[] data)
//    {
//        if (data == null || data.length != 8) return 0x0;
//        // ----------
//        return (long)(
//                (long)(0xff & data[0]) << 56  |
//                (long)(0xff & data[1]) << 48  |
//                (long)(0xff & data[2]) << 40  |
//                (long)(0xff & data[3]) << 32  |
//                (long)(0xff & data[4]) << 24  |
//                (long)(0xff & data[5]) << 16  |
//                (long)(0xff & data[6]) << 8   |
//                (long)(0xff & data[7]) << 0
//                );
//    }
//    private static long[] toLongA(byte[] data)
//    {
//        if (data == null || data.length % 8 != 0) return null;
//        // ----------
//        long[] lngs = new long[data.length / 8];
//        for (int i = 0; i < lngs.length; i++)
//        {
//            lngs[i] = toLong( new byte[]
//                    {
//                data[(i*8)],
//                data[(i*8)+1],
//                data[(i*8)+2],
//                data[(i*8)+3],
//                data[(i*8)+4],
//                data[(i*8)+5],
//                data[(i*8)+6],
//                data[(i*8)+7],
//            } );
//        }
//        return lngs;
//    }
//    /* ========================= */
//    private static float toFloat(byte[] data)
//    {
//        if (data == null || data.length != 4) return 0x0;
//        // ---------- simple:
//        return Float.intBitsToFloat(toInt(data));
//    }
//    private static float[] toFloatA(byte[] data)
//    {
//        if (data == null || data.length % 4 != 0) return null;
//        // ----------
//        float[] flts = new float[data.length / 4];
//        for (int i = 0; i < flts.length; i++)
//        {
//            flts[i] = toFloat( new byte[]
//                    {
//                data[(i*4)],
//                data[(i*4)+1],
//                data[(i*4)+2],
//                data[(i*4)+3],
//            } );
//        }
//        return flts;
//    }
//    /* ========================= */
//    private static double toDouble(byte[] data)
//{
//        if (data == null || data.length != 8) return 0x0;
//        // ---------- simple:
//        return Double.longBitsToDouble(toLong(data));
//    }
//    private static double[] toDoubleA(byte[] data)
//    {
//        if (data == null) return null;
//        // ----------
//        if (data.length % 8 != 0) return null;
//        double[] dbls = new double[data.length / 8];
//        for (int i = 0; i < dbls.length; i++)
//        {
//            dbls[i] = toDouble( new byte[]
//                    {
//                data[(i*8)],
//                data[(i*8)+1],
//                data[(i*8)+2],
//                data[(i*8)+3],
//                data[(i*8)+4],
//                data[(i*8)+5],
//                data[(i*8)+6],
//                data[(i*8)+7],
//            } );
//        }
//        return dbls;
//    }
//    /* ========================= */
//    private static boolean toBoolean(byte[] data)
//    {
//        return (data == null || data.length == 0) ? false : data[0] != 0x00;
//    }
//    private static boolean[] toBooleanA(byte[] data)
//    {
//        if (data == null || data.length < 4) return null;
//        // ----------
//        int len = toInt(new byte[]{data[0], data[1], data[2], data[3]});
//        boolean[] bools = new boolean[len];
//        // ----- pack bools:
//        for (int i = 0, j = 4, k = 7; i < bools.length; i++)
//        {
//            bools[i] = ((data[j] >> k--) & 0x01) == 1;
//            if (k < 0) { j++; k = 7; }
//        }
//        // ----------
//        return bools;
//    }
//    /* ========================= */
//    private static String toString(byte[] data)
//    {
//        return (data == null) ? null : new String(data);
//    }
//    private static String[] toStringA(byte[] data)
//    {
//        // ----------
//        if (data == null || data.length < 4) return null;
//        // ----------
//        byte[] bBuff = new byte[4]; // Buffer
//        // -----
//        System.arraycopy(data, 0, bBuff, 0, 4);
//        int saLen = toInt(bBuff);
//        if (data.length < (4 + (saLen * 4))) return null;
//        // -----
//        bBuff = new byte[saLen * 4];
//        System.arraycopy(data, 4, bBuff, 0, bBuff.length);
//        int[] sLens = toIntA(bBuff);
//        if (sLens == null) return null;
//        // ----------
//        String[] strs = new String[saLen];
//        for (int i = 0, dataPos = 4 + (saLen * 4); i < saLen; i++)
//        {
//            if (sLens[i] > 0)
//            {
//                if (data.length >= (dataPos + sLens[i]))
//                {
//                    bBuff = new byte[sLens[i]];
//                    System.arraycopy(data, dataPos, bBuff, 0, sLens[i]);
//                    dataPos += sLens[i];
//                    strs[i] = toString(bBuff);
//                } else return null;
//            }
//        }
//        // ----------
//        return strs;
//    }
    
}

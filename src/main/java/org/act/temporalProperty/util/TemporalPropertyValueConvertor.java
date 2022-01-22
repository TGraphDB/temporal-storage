package org.act.temporalProperty.util;

import org.act.temporalProperty.meta.ValueContentType;

public class TemporalPropertyValueConvertor
{
    public static final String TemporalPropertyMarker = "TGRAPH_TEMPORAL_PROPERTY";
    public static final String CLASS_NAME_LENGTH_SEPERATOR = "%";
    
    private static final String INT_NAME = "Integer";
    private static final String DOUBLE_NAME = "Double";
    private static final String FLOAT_NAME = "Float";
    private static final String LONG_NAME = "Long";
    private static final String STRING_NAME = "String";
//    private static final String BYTE_ARRAY_NAME = "byte[]";

    public static Slice toSlice( Object value )
    {
        SliceOutput o = new DynamicSliceOutput( 8 );
        if(value instanceof Integer) o.writeInt( (Integer) value );
        else if(value instanceof Double) o.writeDouble( (Double) value );
        else if(value instanceof Float) o.writeFloat( (Float) value );
        else if(value instanceof Long) o.writeLong( (Long) value );
        else if(value instanceof String) {
            o.writeInt(((String) value).length());
            o.writeBytes( ((String) value).getBytes() );
//        } else if(value instanceof byte[]){
//            o.writeInt(((byte[]) value).length);
//            o.writeBytes( (byte[]) value );
        } else throw new UnsupportedOperationException("Unsupported value type");
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
            int len = in.readInt();
            byte[] content = in.readByteArray(len);
            return new String(content);
//        case BYTE_ARRAY_NAME:
//            len = in.readInt();
//            content = new byte[len];
//            in.readBytes(content);
//            return content;
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
            int len = in.readInt();
            byte[] content = in.readByteArray(len);
            return new String(content);
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
//        case BYTE_ARRAY_NAME:
        default:
            throw new UnsupportedOperationException("Unsupported value type");
        }
    }
    
}

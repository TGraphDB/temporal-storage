package org.act.temporalProperty.index;

import com.google.common.base.Preconditions;
import org.act.temporalProperty.util.Slice;

/**
 * Created by song on 2018-01-20.
 */
public enum IndexValueType {
    INT(0){
        @Override
        public int compare(Slice entry1, Slice entry2) {
            return Integer.compare(entry1.getInt(0), entry2.getInt(0));
        }

        @Override
        public int compareRange(Slice min1, Slice max1, Slice min2, Slice max2) {
            long tmp1 = min1.getInt(0);
            tmp1 += max1.getInt(0);
            long tmp2 = min2.getInt(0);
            tmp2 += max2.getInt(0);
            return Long.compare(tmp1, tmp2);
        }

        @Override
        public String toString(Slice field) {
            return null;
        }
    },
    LONG(1){
        @Override
        public int compare(Slice entry1, Slice entry2) {
            return Long.compare(entry1.getLong(0), entry2.getLong(0));
        }

        @Override
        public int compareRange(Slice min1, Slice max1, Slice min2, Slice max2) {
            long tmp1 = min1.getLong(0);
            tmp1 += max1.getLong(0);
            long tmp2 = min2.getLong(0);
            tmp2 += max2.getLong(0);
            return Long.compare(tmp1, tmp2);
        }

        @Override
        public String toString(Slice field) {
            return null;
        }
    },
    FLOAT(2){
        @Override
        public int compare(Slice entry1, Slice entry2) {
            return Float.compare(entry1.getFloat(0), entry2.getFloat(0));
        }

        @Override
        public int compareRange(Slice min1, Slice max1, Slice min2, Slice max2) {
            float tmp1 = min1.getFloat(0);
            tmp1 += max1.getFloat(0);
            float tmp2 = min2.getFloat(0);
            tmp2 += max2.getFloat(0);
            return Float.compare(tmp1, tmp2);
        }

        @Override
        public String toString(Slice field) {
            return null;
        }
    },
    DOUBLE(3){
        @Override
        public int compare(Slice entry1, Slice entry2) {
            return Double.compare(entry1.getDouble(0), entry2.getDouble(0));
        }

        @Override
        public int compareRange(Slice min1, Slice max1, Slice min2, Slice max2) {
            double tmp1 = min1.getDouble(0);
            tmp1 += max1.getDouble(0);
            double tmp2 = min2.getDouble(0);
            tmp2 += max2.getDouble(0);
            return Double.compare(tmp1, tmp2);
        }

        @Override
        public String toString(Slice field) {
            return null;
        }
    },
    STRING(4){
        @Override
        public int compare(Slice entry1, Slice entry2) {
            return entry1.compareTo(entry2);

        }

        @Override
        public int compareRange(Slice min1, Slice max1, Slice min2, Slice max2) {
            int i;
            for(i=0; i<min1.length() && i<max1.length() && i<min2.length() && i<max2.length(); i++){
                int result = compareRange(min1.getByte(i), max1.getByte(i), min2.getByte(i), max2.getByte(i));
                if(result!=0) return result;
            }
            int maxLen = Math.max(min1.length(), max1.length());
            maxLen = Math.max(maxLen, min2.length());
            maxLen = Math.max(maxLen, max2.length());
            for(; i<maxLen; i++){
                byte min1Byte, max1Byte, min2Byte, max2Byte;
                if(i>=min1.length()) min1Byte=0; else min1Byte=min1.getByte(i);
                if(i>=max1.length()) max1Byte=0; else max1Byte=max1.getByte(i);
                if(i>=min2.length()) min2Byte=0; else min2Byte=min2.getByte(i);
                if(i>=max2.length()) max2Byte=0; else max2Byte=max2.getByte(i);
                int result = compareRange(min1Byte, max1Byte, min2Byte, max2Byte);
                if(result!=0) return result;
            }
            return 0;
        }

        private int compareRange(byte min1, byte max1, byte min2, byte max2) {
            int tmp1 = min1;
            tmp1+=max1;
            int tmp2 = min2;
            tmp2+=max2;
            return Integer.compare(tmp1, tmp2);
        }

        @Override
        public String toString(Slice field) {
            return null;
        }
    };


    private int id;
    IndexValueType(int id){
        this.id = id;
    }
    int getId(){
        return id;
    }

    static IndexValueType decode(int id){
        Preconditions.checkArgument(0<=id && id<=4);
        switch(id){
            case 0: return INT;
            case 1: return LONG;
            case 2: return FLOAT;
            case 3: return DOUBLE;
            default:return STRING;
        }
    }

    public abstract int compare(Slice entry1, Slice entry2);
    public abstract int compareRange(Slice min1, Slice max1, Slice min2, Slice max2);

    public abstract String toString(Slice field);
}

package org.act.temporalProperty.meta;

import org.act.temporalProperty.impl.FileMetaData;
import org.act.temporalProperty.query.TimePointL;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;
import org.act.temporalProperty.util.SliceOutput;

/**
 * Created by song on 2018-01-17.
 */
public class FileMetaDataController {
    public static Slice encode(FileMetaData meta){
        SliceOutput out = new DynamicSliceOutput(24);
        encode(out, meta);
        return out.slice();
    }

    public static void encode(SliceOutput out, FileMetaData meta){
        out.writeLong(meta.getNumber());
        out.writeLong(meta.getFileSize());
        meta.getSmallest().encode(out);
        meta.getLargest().encode(out);
    }

    public static FileMetaData decode(SliceInput in){
        long fNum = in.readLong();
        long fSize = in.readLong();
        TimePointL smallest = TimePointL.decode(in);
        TimePointL largest = TimePointL.decode(in);
        return new FileMetaData(fNum, fSize, smallest, largest);
    }

    public static FileMetaData decode(Slice in){
        return decode(in.input());
    }
}

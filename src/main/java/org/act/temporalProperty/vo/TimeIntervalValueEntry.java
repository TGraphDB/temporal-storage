package org.act.temporalProperty.vo;

import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.MemTable;
import org.act.temporalProperty.query.TimeIntervalKey;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;

import java.util.Map;

public class TimeIntervalValueEntry implements Map.Entry<TimeIntervalKey, Slice> {

    private final TimeIntervalKey key;
    private final Slice val;

    public TimeIntervalValueEntry(TimeIntervalKey key, Slice val )
    {
        this.key = key;
        this.val = val;
    }
    @Override
    public TimeIntervalKey getKey()
    {
        return key;
    }

    @Override
    public Slice getValue()
    {
        return val;
    }

    @Override
    public Slice setValue( Slice value )
    {
        throw new UnsupportedOperationException();
    }

    public static TimeIntervalValueEntry decode(SliceInput in)
    {
        long endTime = in.readLong();
        int len = in.readInt();
        InternalKey start = new InternalKey( in.readSlice( len ) );
        len = in.readInt();
        Slice value = in.readSlice( len );
        return new TimeIntervalValueEntry( new TimeIntervalKey( start, endTime ), value );
    }

    public static Slice encode(TimeIntervalKey key, Slice value)
    {
        DynamicSliceOutput out = new DynamicSliceOutput( 64 );
        out.writeLong( key.to() );
        Slice start = key.getStartKey().encode();
        out.writeInt( start.length() );
        out.writeBytes( start );
        out.writeInt( value.length() );
        out.writeBytes( value );
        return out.slice();
    }
}

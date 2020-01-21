package org.act.temporalProperty.query;


import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;
import org.act.temporalProperty.util.SliceOutput;
import org.act.temporalProperty.util.Slices;

import java.util.Objects;

import static org.act.temporalProperty.util.SizeOf.SIZE_OF_LONG;

/**
 * Created by song on 2018-05-09.
 *
 * Implementation of TimePoint, valid range [0, 2^61-2] (2^61-2==2305843009213693949==about 73 years' nanoseconds, 1 seconds = 10^9 nanoseconds).
 * 2^61 is used as NOW. -2 is used as INIT
 * because when store, we use first 3 bit to represent value type (8 types), check InternalKey.encode for details
 */
public class TimePointL implements TPoint<TimePointL>
{
    private static final long INIT_VAL_LONG = -2L;
    private static final long NOW_VAL_LONG = Long.MAX_VALUE >> 2; // (Long.MAX_VALUE == 2^63)
    public static final TimePointL Now = new TimePointL(true){
        @Override public boolean isNow() { return true; }
        @Override public boolean isInit(){ return false; }
        @Override public TimePointL pre() { throw new UnsupportedOperationException("should not call pre on TimePoint.NOW"); }
        @Override public TimePointL next() { throw new UnsupportedOperationException("should not call next on TimePoint.NOW"); }
        @Override public String toString() { return "NOW"; }
    };
    public static final TimePointL Init = new TimePointL(false){
        @Override public boolean isNow() { return false; }
        @Override public boolean isInit(){ return true; }
        @Override public TimePointL pre() { throw new UnsupportedOperationException("should not call pre on TimePoint.INIT"); }
        @Override public TimePointL next() { throw new UnsupportedOperationException("should not call next on TimePoint.INIT"); }
        @Override public String toString() { return "INIT"; }
    };

    protected long time;

    public TimePointL( long time )
    {
        this.time = time;
        assert (0<=time && time<=NOW_VAL_LONG-2): new IllegalArgumentException("invalid time value "+ time +", only support 0 to "+(NOW_VAL_LONG -2));
    }

    // this constructor is used for now and init only.
    protected TimePointL( boolean isNow )
    {
        if(isNow) this.time = NOW_VAL_LONG;
        else this.time = INIT_VAL_LONG;
    }

    @Override
    public TimePointL pre()
    {
        return new TimePointL( time - 1 );
    }

    @Override
    public TimePointL next()
    {
        return new TimePointL( time + 1 );
    }

    @Override
    public boolean isNow()
    {
        return false;
    }

    @Override
    public boolean isInit()
    {
        return false;
    }

    public long val()
    {
        return time;
    }

    public int valInt()
    {
        return Math.toIntExact(time);
    }

    @Override
    public int compareTo( TimePointL o )
    {
        return Long.compare( val(), o.val() );
    }

    @Override
    public String toString()
    {
        return String.valueOf( val() );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimePointL that = (TimePointL) o;
        return time == that.time;
    }

    @Override
    public int hashCode() {
        return Objects.hash(time);
    }

    public Slice encode(){
        Slice raw = Slices.allocate(SIZE_OF_LONG);
        encode(raw.output());
        return raw;
    }

    public static TimePointL decode(Slice in) {
        return decode(in.input());
    }

    public static TimePointL decode(SliceInput in) {
        long t = in.readLong();
        if(t==NOW_VAL_LONG) return Now;
        else if(t==INIT_VAL_LONG) return Init;
        else return new TimePointL(t);
    }

    public void encode(SliceOutput out) {
        out.writeLong(time);
    }
}

package org.act.temporalProperty.query;


/**
 * Created by song on 2018-05-09.
 */
public class TimePointL implements TPoint<TimePointL>
{
    private static final long INIT = -2;
    private static final long NOW = Long.MAX_VALUE;
    public static final TimePointL Now = new TimePointL(){
        @Override public long val() { return NOW; }
        @Override public boolean isNow() { return true; }
        @Override public boolean isInit(){ return false; }
        @Override public TimePointL pre() { throw new UnsupportedOperationException("should not call pre on TimePoint.NOW"); }
        @Override public TimePointL next() { throw new UnsupportedOperationException("should not call next on TimePoint.NOW"); }
        @Override public String toString() { return "NOW"; }
    };
    public static final TimePointL Init = new TimePointL(){
        @Override public long val() { return INIT; }
        @Override public boolean isNow() { return false; }
        @Override public boolean isInit(){ return true; }
        @Override public TimePointL pre() { throw new UnsupportedOperationException("should not call pre on TimePoint.INIT"); }
        @Override public TimePointL next() { throw new UnsupportedOperationException("should not call next on TimePoint.INIT"); }
        @Override public String toString() { return "INIT"; }
    };

    private long time;

    public TimePointL( long time )
    {
        this.time = time;
        assert (time>=0 && time!=NOW && time!=NOW-1): new IllegalArgumentException("invalid time value "+ time +", only support 0 to "+(NOW-2));
    }

    private TimePointL(){}

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
}

package org.act.temporalProperty.query;

import com.google.common.base.Objects;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.vo.EntityPropertyId;

/**
 * Created by song on 2018-05-05.
 */
public class TimeIntervalKey extends TimeInterval
{
    private final EntityPropertyId id;
    private final ValueType valueType;

    public TimeIntervalKey( InternalKey start, long end )
    {
        super( start.getStartTime(), end );
        this.id = start.getId();
        valueType = start.getValueType();
    }

    public TimeIntervalKey( InternalKey start )
    {
        super( start.getStartTime() );
        this.id = start.getId();
        valueType = start.getValueType();
    }

    public TimeIntervalKey(EntityPropertyId id, long newStart, long end, ValueType valueType)
    {
        super( newStart, end );
        this.id = id;
        this.valueType = valueType;
    }

    public EntityPropertyId getId()
    {
        return id;
    }

    public ValueType getValueType() {
        return valueType;
    }

    public InternalKey getStartKey()
    {
        return new InternalKey( id, Math.toIntExact( from() ), valueType );
    }

    public InternalKey getEndKey()
    {
        return new InternalKey( id, Math.toIntExact( to() + 1 ), ValueType.UNKNOWN );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        TimeIntervalKey that = (TimeIntervalKey) o;
        return from() == that.from();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode( from() );
    }

    public TimeIntervalKey changeEnd( long newEnd )
    {
        return new TimeIntervalKey( this.id, from(), newEnd, valueType);
    }

    public TimeIntervalKey changeStart( long newStart )
    {
        return new TimeIntervalKey( this.id, newStart, to(), valueType );
    }

    @Override
    public String toString()
    {
        return "TimeIntervalKey{start=" + from() + ", end=" + to() + ", pro=" + id.getPropertyId() + ", eid=" + id.getEntityId() + ", type=" +
                valueType + '}';
    }

    public boolean lessThan( int time )
    {
        return this.lessThan( new TimePointL( time ) );
    }

    public boolean greaterOrEq( int time )
    {
        return this.greaterOrEq( new TimePointL( time ) );
    }

    public boolean span( int minTime, int maxTime )
    {
        return this.span( new TimePointL( minTime ), new TimePointL( maxTime ) );
    }

    public boolean span( int time )
    {
        return this.span( new TimePointL( time ) );
    }

    public boolean between( int min, int maxTime )
    {
        return this.between( new TimePointL( min ), new TimePointL( maxTime ) );
    }
}

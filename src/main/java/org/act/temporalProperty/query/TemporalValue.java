package org.act.temporalProperty.query;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.act.temporalProperty.exception.TPSRuntimeException;
import org.apache.commons.lang3.tuple.Triple;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Created by song on 2018-05-09.
 * 用于表示一个时态值：（时间区间，值）元组构成的序列，时间区间不相交
 * 实现时用的是（时间点，值）组成的TreeMap，注意其中时间点不含有TimePoint.NOW，但可以有TimePoint.Init
 */
public class TemporalValue<V>
{
    private final TreeMap<TimePointL,Value> map = new TreeMap<>();

    public TemporalValue( V initialValue )
    {
        map.put( TimePointL.Init, val( initialValue ) );
    }

    public TemporalValue()
    {
    }

    public void put( TimeInterval interval, V value )
    {
        if ( interval.end().isNow() )
        {
            map.tailMap( interval.start(), true ).clear();
            map.put( interval.start(), val( value ) );
        }else{
            Value end = map.get( interval.end().next() );
            map.subMap( interval.start(), true, interval.end(), true ).clear();
            map.put( interval.start(), val( value ) );
            if ( end == null )
            {
                map.put( interval.end().next(), valUnknown() );
            }
            else
            {
                map.put( interval.end().next(), end );
            }
        }
    }

    public V get( TimePointL time )
    {
        Entry<TimePointL,Value> entry = map.floorEntry( time );
        if(entry!=null)
        {
            Value val = entry.getValue();
            if ( val == null || val.isUnknown ) { return null; }
            else {
                return val.value;
            }
        }else{
            return null;
        }
    }

    private Value valUnknown()
    {
        return new Value( true, null );
    }

    private Value val( V value )
    {
        return new Value( false, value );
    }

    public boolean overlap( TimePointL startTime, TimePointL endTime )
    {
        Entry<TimePointL,Value> floor = map.floorEntry( endTime );
        if ( floor == null )
        {
            return false;
        }
        else if ( floor.getValue().isUnknown )
        {
            return (floor.getKey().compareTo( startTime ) >= 0);
        }
        else
        {
            return true;
        }
    }

    public TimeInterval covered()
    {
        if ( map.size() > 2 )
        {
            return new TimeInterval( map.firstKey(), map.lastKey().pre() );
        }
        else if ( map.size() == 1 )
        {
            assert map.firstKey().isInit();
            return new TimeInterval( map.firstKey(), TimePointL.Now );
        }
        else
        {
            throw new TPSRuntimeException( "contain no value!" );
        }
    }

    public PeekingIterator<Entry<TimeInterval,V>> intervalEntries()
    {
        return new IntervalIterator();
    }

    //    public PeekingIterator<Entry<TimeInterval,V>> intervalEntries( TimePointL start )
    //    {
    //        return null;
    //    }

    public PeekingIterator<Entry<TimeInterval,V>> intervalEntries( TimePointL start, TimePointL end )
    {
        return new AddIntervalIterator( start, end );
    }

    public PeekingIterator<Triple<TimePointL,Boolean,V>> pointEntries()
    {
        return Iterators.peekingIterator( Iterators.transform( map.entrySet().iterator(),
                                                               input -> Triple.of( input.getKey(), input.getValue().isUnknown, input.getValue().value ) ) );
    }

    public PeekingIterator<Triple<TimePointL,Boolean,V>> pointEntries( TimePointL startTime )
    {
        Entry<TimePointL,Value> floor = map.floorEntry( startTime );
        if ( floor != null && floor.getKey().compareTo( startTime ) < 0 && !floor.getValue().isUnknown )
        {
            return new AddFirstPointIterator( startTime, map.tailMap( startTime, true ).entrySet().iterator() );
        }
        else
        {
            return Iterators.peekingIterator( Iterators.transform( map.tailMap( startTime, true ).entrySet().iterator(),
                                                                   input -> Triple.of( input.getKey(), input.getValue().isUnknown, input.getValue().value ) ) );
        }
    }

    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    public TemporalValue<V> slice( TimePointL min, boolean includeMin, V max, boolean includeMax )
    {

        return null;
    }

    private class Value
    {
        private boolean isUnknown;
        private V value;

        Value( boolean isUnknown, V value )
        {
            this.isUnknown = isUnknown;
            this.value = value;
        }
    }

    private class TimeIntervalValueEntry implements Entry<TimeInterval,V>
    {

        private final TimeInterval key;
        private final V val;

        public TimeIntervalValueEntry( TimeInterval key, V val )
        {
            this.key = key;
            this.val = val;
        }

        @Override
        public TimeInterval getKey()
        {
            return key;
        }

        @Override
        public V getValue()
        {
            return val;
        }

        @Override
        public V setValue( V value )
        {
            throw new UnsupportedOperationException();
        }
    }

    private class IntervalIterator extends AbstractIterator<Entry<TimeInterval,V>> implements PeekingIterator<Entry<TimeInterval,V>>
    {
        PeekingIterator<Entry<TimePointL,Value>> iterator = Iterators.peekingIterator( map.entrySet().iterator() );

        @Override
        protected Entry<TimeInterval,V> computeNext()
        {
            while ( iterator.hasNext() )
            {
                if ( !isUnknown( iterator.peek() ) )
                {
                    Entry<TimePointL,Value> start = iterator.next();
                    if ( iterator.hasNext() )
                    {
                        Entry<TimePointL,Value> endNext = iterator.peek();
                        return new TimeIntervalValueEntry( new TimeInterval( start.getKey(), endNext.getKey().pre() ), start.getValue().value );
                    }
                    else
                    {
                        return new TimeIntervalValueEntry( new TimeInterval( start.getKey(), TimePointL.Now ), start.getValue().value );
                    }
                }
                else
                {
                    iterator.next();
                }
            }
            return endOfData();
        }

        private boolean isUnknown( Entry<TimePointL,Value> entry )
        {
            return entry.getValue().isUnknown;
        }
    }

    private class AddIntervalIterator extends AbstractIterator<Entry<TimeInterval,V>> implements PeekingIterator<Entry<TimeInterval,V>>
    {
        private final TimePointL end;
        private TimeIntervalValueEntry start;
        PeekingIterator<Entry<TimePointL,Value>> iterator;

        public AddIntervalIterator( TimePointL start, TimePointL end )
        {
            this.end = end;
            this.iterator = Iterators.peekingIterator( map.subMap( start, end ).entrySet().iterator() );
            Entry<TimePointL,Value> floorEntry = map.floorEntry( start );
            if ( !floorEntry.getValue().isUnknown && floorEntry.getKey().compareTo( start ) < 0 )
            {
                this.start = new TimeIntervalValueEntry( new TimeInterval( start, floorEntry.getKey().pre() ), floorEntry.getValue().value );
            }
        }

        @Override
        protected Entry<TimeInterval,V> computeNext()
        {
            if ( start != null )
            {
                TimeIntervalValueEntry tmp = start;
                start = null;
                return tmp;
            }

            while ( iterator.hasNext() )
            {
                if ( !isUnknown( iterator.peek() ) )
                {
                    Entry<TimePointL,Value> start = iterator.next();
                    if ( iterator.hasNext() )
                    {
                        Entry<TimePointL,Value> end = iterator.peek();
                        return new TimeIntervalValueEntry( new TimeInterval( start.getKey(), end.getKey() ), start.getValue().value );
                    }
                    else
                    {
                        return new TimeIntervalValueEntry( new TimeInterval( start.getKey(), this.end ), start.getValue().value );
                    }
                }
            }
            return endOfData();
        }

        private boolean isUnknown( Entry<TimePointL,Value> entry )
        {
            return entry.getValue().isUnknown;
        }
    }

    private class AddFirstPointIterator extends AbstractIterator<Triple<TimePointL,Boolean,V>> implements PeekingIterator<Triple<TimePointL,Boolean,V>>
    {

        private final TimePointL firstTime;
        private final Iterator<Entry<TimePointL,Value>> in;
        private Value firstValue;

        public AddFirstPointIterator( TimePointL startTime, Iterator<Entry<TimePointL,Value>> iterator )
        {
            this.in = iterator;
            this.firstTime = startTime;
            this.firstValue = map.floorEntry( startTime ).getValue();
        }

        @Override
        protected Triple<TimePointL,Boolean,V> computeNext()
        {
            if ( firstValue != null )
            {
                Triple<TimePointL,Boolean,V> tmp = Triple.of( firstTime, firstValue.isUnknown, firstValue.value );
                firstValue = null;
                return tmp;
            }
            else if ( in.hasNext() )
            {
                Entry<TimePointL,Value> entry = in.next();
                return Triple.of( entry.getKey(), entry.getValue().isUnknown, entry.getValue().value );
            }
            else
            {
                return endOfData();
            }
        }
    }
}

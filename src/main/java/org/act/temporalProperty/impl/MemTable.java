package org.act.temporalProperty.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.act.temporalProperty.exception.ValueUnknownException;
import org.act.temporalProperty.helper.AbstractSearchableIterator;
import org.act.temporalProperty.query.TemporalValue;
import org.act.temporalProperty.query.TimeInterval;
import org.act.temporalProperty.query.TimeIntervalKey;
import org.act.temporalProperty.query.TimePointL;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.Slices;
import org.act.temporalProperty.vo.EntityPropertyId;
import org.act.temporalProperty.vo.TimeIntervalValueEntry;
import org.apache.commons.lang3.tuple.Triple;

import java.util.*;
import java.util.Map.Entry;

/**
 * Modified MemTable, which only expose time interval API.
 */
public class MemTable
{
    private final TreeMap<EntityPropertyId, TemporalValue<Value>> table = new TreeMap<>(EntityPropertyId::compareTo);

    private long approximateMemoryUsage = 0;

    public boolean isEmpty()
    {
        return table.isEmpty();
    }

    public long approximateMemUsage()
    {
        return approximateMemoryUsage;
    }

    public void addToNow( InternalKey key, Slice value )
    {
        Preconditions.checkArgument( key.getValueType() != ValueType.UNKNOWN );
        EntityPropertyId id = key.getId();
        add( id, new TimeInterval( key.getStartTime() ), new Value( key.getId(), key.getValueType(), value ) );
    }

    public void addInterval( InternalKey key, TimePointL endTime, Slice value )
    {
        Preconditions.checkNotNull( key );
        Preconditions.checkArgument( key.getStartTime().compareTo(endTime) <= 0 );
        add( key.getId(), new TimeInterval( key.getStartTime(), endTime ), new Value( key.getId(), key.getValueType(), value ) );
    }

    public void addInterval( TimeIntervalKey key, Slice value )
    {
        Preconditions.checkNotNull( key );
        ValueType valueType = key.getValueType();
        Preconditions.checkArgument( valueType != ValueType.UNKNOWN && valueType!=ValueType.VALUE);
        add( key.getId(), key, new Value( key.getId(), key.getValueType(), value ) );
    }

    private void add(EntityPropertyId id, TimeInterval interval, Value value )
    {
        table.computeIfAbsent( id, ( i ) -> new TemporalValue<>() ).put( interval, value );
        approximateMemoryUsage += (12 + 8 + value.val.length());
    }

    public Slice get( InternalKey key ) throws ValueUnknownException
    {
        Preconditions.checkNotNull( key, "key is null" );
        TemporalValue<Value> entityMap = table.get( key.getId() );
        if ( entityMap == null )
        {
            throw new ValueUnknownException(); //no entity
        }
        Value entry = entityMap.get( key.getStartTime() );
        if ( entry != null )
        {
            if ( entry.valueType != ValueType.INVALID )
            {
                return entry.val;
            }
            else //else: invalid value
            {
                return null;
            }
        }
        else //else: value is unknown.
        {
            throw new ValueUnknownException();
        }
    }

    public void merge( MemTable toMerge )
    {
        this.table.putAll( toMerge.table );
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

    public String toString()
    {
        Map<Integer,TemporalValue<Boolean>> result = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        for ( Entry<EntityPropertyId, TemporalValue<Value>> entityEntry : table.entrySet() )
        {
            int proId = entityEntry.getKey().getPropertyId();
            result.putIfAbsent( proId, new TemporalValue<>() );
            TemporalValue<Boolean> tMap = result.get( proId );
            TemporalValue<Value> entityMap = entityEntry.getValue();
            Iterator<Entry<TimeInterval,Value>> it = entityMap.intervalEntries();
            while ( it.hasNext() )
            {
                Entry<TimeInterval,Value> entry = it.next();
                tMap.put( entry.getKey(), true );
            }
        }
        sb.append( "propertyCount(" ).append( result.size() ).append( ")" );
        for ( Entry<Integer,TemporalValue<Boolean>> entry : result.entrySet() )
        {
            sb.append( '[' ).append( entry.getKey() ).append( ']' );
            TimeInterval interval = entry.getValue().covered();
            sb.append( '(' ).append( interval.start() ).append( '~' ).append( interval.end() ).append( ") " );
        }
        return sb.toString();
    }

    public MemTableIterator iterator()
    {
        return new MemTableIterator(table);
    }

    public PeekingIterator<Entry<TimeIntervalKey,Slice>> intervalEntryIterator()
    {
        return new IntervalIterator();
    }

    public boolean overlap( EntityPropertyId id, int startTime, int endTime )
    {
        TemporalValue<Value> entityMap = table.get( id );
        if ( entityMap == null )
        {
            return false;
        }
        else
        {
            return entityMap.overlap( new TimePointL( startTime ), new TimePointL( endTime ) );
        }
    }

    public Map<Integer,MemTable> separateByProperty()
    {
        Map<Integer,MemTable> result = new TreeMap<>();
        for ( Entry<EntityPropertyId, TemporalValue<Value>> e : table.entrySet() )
        {
            int proId = e.getKey().getPropertyId();
            result.computeIfAbsent( proId, i -> new MemTable() ).addEntry( e.getKey(), e.getValue() );
        }
        return result;
    }

    private void addEntry( EntityPropertyId key, TemporalValue<Value> value )
    {
        table.put( key, value );
    }

    public boolean overlap( int proId, int startTime, int endTime )
    {
        for ( Entry<EntityPropertyId,TemporalValue<Value>> entityEntry : table.entrySet() )
        {
            if ( entityEntry.getKey().getPropertyId() == proId )
            {
                TemporalValue<Value> entityMap = entityEntry.getValue();
                if ( entityMap.overlap( new TimePointL( startTime ), new TimePointL( endTime ) ) )
                {
                    return true;
                }
            }
        }
        return false;
    }

    public void coverTime( TemporalValue<Boolean> tMap, Set<Integer> proIds, int timeMin, int timeMax )
    {
        TimePointL start = new TimePointL( timeMin );
        TimePointL end = new TimePointL( timeMax );
        for ( Entry<EntityPropertyId, TemporalValue<Value>> entityEntry : table.entrySet() )
        {
            if ( proIds.contains( entityEntry.getKey().getPropertyId() ) )
            {
                TemporalValue<Value> entityMap = entityEntry.getValue();
                Iterator<Entry<TimeInterval,Value>> it = entityMap.intervalEntries( start, end );
                while ( it.hasNext() )
                {
                    Entry<TimeInterval,Value> entry = it.next();
                    tMap.put( entry.getKey(), true );
                }
            }
        }
    }

    private class Value
    {
        EntityPropertyId id;
        ValueType valueType;
        Slice val;

        Value(EntityPropertyId id, ValueType valueType, Slice val)
        {
            this.id = id;
            this.valueType = valueType;
            this.val = val;
        }
    }

    /**
     *
     */
    public static class MemTableIterator extends AbstractSearchableIterator
    {
        private final TreeMap<EntityPropertyId, TemporalValue<Value>> table;
        private PeekingIterator<Entry<EntityPropertyId, TemporalValue<Value>>> tPropIter;
        private PeekingIterator<Triple<TimePointL,Boolean,Value>> tValIter;

        MemTableIterator(TreeMap<EntityPropertyId, TemporalValue<Value>> table)
        {
            this.table = table;
            this.tPropIter = Iterators.peekingIterator( table.entrySet().iterator() );
        }

        @Override
        protected InternalEntry computeNext()
        {
            while ( tValIter == null || !tValIter.hasNext() )
            {
                if ( tPropIter.hasNext() ) {
                    TemporalValue<Value> tpValue = tPropIter.next().getValue();
                    tValIter = tpValue.pointEntries();
                } else {
                    return endOfData();
                }
            }
            Triple<TimePointL,Boolean,Value> entry = tValIter.next();
            Value v = entry.getRight();
            TimePointL startTime = entry.getLeft();
            Boolean isUnknown = entry.getMiddle();
            if ( isUnknown ) {
                return new InternalEntry( new InternalKey( v.id, startTime, ValueType.UNKNOWN ), Slices.EMPTY_SLICE );
            } else {
                return new InternalEntry( new InternalKey( v.id, startTime, v.valueType ), v.val );
            }
        }

        @Override
        public void seekToFirst()
        {
            super.resetState();
            tPropIter = Iterators.peekingIterator( table.entrySet().iterator() );
            tValIter = null;
        }

        @Override
        public boolean seekFloor(InternalKey targetKey )
        {
            super.resetState();
            Entry<EntityPropertyId, TemporalValue<Value>> result = table.floorEntry(targetKey.getId());
            if(result != null){
                tPropIter = Iterators.peekingIterator( table.tailMap(result.getKey(), true).entrySet().iterator() );
                tValIter = result.getValue().pointEntries(targetKey.getStartTime());
                return super.seekFloor(targetKey);
            }else{
                tPropIter = Iterators.peekingIterator( table.tailMap(targetKey.getId(), true).entrySet().iterator() );
                return false;
            }
        }

        @Override
        public String toString() {
            return "MemTableIterator{}";
        }
    }

    private class IntervalIterator extends AbstractIterator<Entry<TimeIntervalKey,Slice>> implements PeekingIterator<Entry<TimeIntervalKey,Slice>>
    {
        private PeekingIterator<Entry<EntityPropertyId, TemporalValue<Value>>> tpIter;
        private PeekingIterator<Entry<TimeInterval,Value>> tvIntIter;

        private IntervalIterator()
        {
            tpIter = Iterators.peekingIterator( table.entrySet().iterator() );
        }

        @Override
        protected Entry<TimeIntervalKey,Slice> computeNext()
        {
            while ( tvIntIter == null || !tvIntIter.hasNext() )
            {
                if ( tpIter.hasNext() )
                {
                    tvIntIter = tpIter.next().getValue().intervalEntries();
                }
                else
                {
                    return endOfData();
                }
            }
            Entry<TimeInterval,Value> entry = tvIntIter.next();
            TimeInterval tInt = entry.getKey();
            Value value = entry.getValue();
            TimeIntervalKey intervalKey = new TimeIntervalKey( value.id, tInt.from(), tInt.to(), value.valueType);
            return new TimeIntervalValueEntry( intervalKey, value.val );
        }
    }

}

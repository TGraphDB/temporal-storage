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
import org.act.temporalProperty.util.SliceInput;
import org.act.temporalProperty.util.Slices;
import org.act.temporalProperty.vo.EntityPropertyId;
import org.apache.commons.lang3.tuple.Triple;

import java.util.*;
import java.util.Map.Entry;

/**
 * Modified MemTable, which only expose time interval API.
 */
public class MemTable
{

    private final TreeMap<EntityPropertyId,TemporalValue<Value>> table;

    private long approximateMemoryUsage = 0;

    public MemTable()
    {
        this.table = new TreeMap<>(EntityPropertyId::compareTo);
    }

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

    public void addInterval( InternalKey key, int endTime, Slice value )
    {
        Preconditions.checkNotNull( key );
        Preconditions.checkArgument( key.getStartTime() <= endTime );
        add( key.getId(), new TimeInterval( key.getStartTime(), endTime ), new Value( key.getId(), key.getValueType(), value ) );
    }

    public void addInterval( TimeIntervalKey key, Slice value )
    {
        Preconditions.checkNotNull( key );
        InternalKey startKey = key.getStartKey();
        Preconditions.checkArgument( startKey.getValueType() != ValueType.UNKNOWN );
        Preconditions.checkArgument( startKey.getStartTime() <= key.to() );
        EntityPropertyId id = startKey.getId();
        add( id, key, new Value( key.getId(), key.getValueType(), value ) );
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
        Value entry = entityMap.get( new TimePointL( key.getStartTime() ) );
        if ( entry != null )
        {
            EntityPropertyId ansKey = entry.key;
            if ( ansKey.getValueType() != ValueType.INVALID )
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

    public static TimeIntervalValueEntry decode(SliceInput in)
    {
        long endTime = in.readLong();
        int len = in.readInt();
        InternalKey start = new InternalKey( in.readSlice( len ) );
        len = in.readInt();
        Slice value = in.readSlice( len );
        return new TimeIntervalValueEntry( new TimeIntervalKey( start, endTime ), value );
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
        EntityPropertyId key;
        ValueType valueType;
        Slice val;

        public Value(EntityPropertyId key, ValueType valueType, Slice val )
        {
            this.key = key;
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
        private Triple<TimePointL,Boolean,Value> lastEntry = null;

        public MemTableIterator(TreeMap<EntityPropertyId, TemporalValue<Value>> table )
        {
            this.table = table;
            this.tPropIter = Iterators.peekingIterator( table.entrySet().iterator() );
        }

        @Override
        protected InternalEntry computeNext()
        {
            this.tValIter = getValidEntryIterator();
            if ( tValIter != null )
            {
                return getNextEntry(tValIter);
            }
            else
            {
                return endOfData();
            }
        }

        private InternalEntry getNextEntry( PeekingIterator<Triple<TimePointL,Boolean,Value>> entryIter )
        {
            assert entryIter.hasNext();

            Triple<TimePointL,Boolean,Value> entry = entryIter.next();
            Boolean isUnknown = entry.getMiddle();
            if ( isUnknown )
            {
                assert lastEntry != null;
                InternalKey origin = lastEntry.getRight().key;
                int startTime = Math.toIntExact( entry.getLeft().val() );
                InternalKey key = new InternalKey( origin.getId(), startTime, ValueType.UNKNOWN );
                return new InternalEntry( key, Slices.EMPTY_SLICE );
            }
            else
            {
                Value v = entry.getRight();
                InternalKey origin = v.key;
                int startTime = Math.toIntExact( entry.getLeft().val() );
                InternalKey key = new InternalKey( origin.getId(), startTime, origin.getValueType() );
                lastEntry = entry;
                return new InternalEntry( key, v.val );
            }
        }

        private PeekingIterator<Triple<TimePointL,Boolean,Value>> getValidEntryIterator()
        {
            while ( tValIter == null || !tValIter.hasNext() )
            {
                if ( tPropIter.hasNext() )
                {
                    TemporalValue<Value> tpValue = tPropIter.next().getValue();
                    tValIter = tpValue.pointEntries();
                }
                else
                {
                    return null;
                }
            }
            return tValIter;
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
            tPropIter = Iterators.peekingIterator( table.entrySet().iterator() );
            while ( tPropIter.hasNext() )
            {
                Entry<EntityPropertyId, TemporalValue<Value>> entry = tPropIter.next();
                if ( entry.getKey().equals( targetKey.getId() ) )
                {
                    TemporalValue<Value> entityMap = entry.getValue();
                    tValIter = entityMap.pointEntries( new TimePointL( targetKey.getStartTime() ) );
                    return false;
                }
            }
            return super.seekFloor(targetKey);
        }

        @Override
        public String toString() {
            return "MemTableIterator{}";
        }
    }

    private class IntervalIterator extends AbstractIterator<Entry<TimeIntervalKey,Slice>> implements PeekingIterator<Entry<TimeIntervalKey,Slice>>
    {

        private PeekingIterator<Entry<EntityPropertyId, TemporalValue<Value>>> iterator;
        private PeekingIterator<Entry<TimeInterval,Value>> entryIter;

        private IntervalIterator()
        {
            iterator = Iterators.peekingIterator( table.entrySet().iterator() );
        }

        @Override
        protected Entry<TimeIntervalKey,Slice> computeNext()
        {
            while ( entryIter == null || !entryIter.hasNext() )
            {
                if ( iterator.hasNext() )
                {
                    entryIter = iterator.next().getValue().intervalEntries();
                }
                else
                {
                    return endOfData();
                }
            }
            Entry<TimeInterval,Value> entry = entryIter.next();
            InternalKey origin = entry.getValue().key;
            TimeIntervalKey intervalKey = new TimeIntervalKey( origin.getId(), entry.getKey().from(), entry.getKey().to() );
            return new TimeIntervalValueEntry( intervalKey, entry.getValue().val );
        }
    }

    public static class TimeIntervalValueEntry implements Entry<TimeIntervalKey, Slice>{

        private final TimeIntervalKey key;
        private final Slice val;

        public TimeIntervalValueEntry( TimeIntervalKey key, Slice val )
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
    }
}

package org.act.temporalProperty.query.aggr;

import com.google.common.base.Preconditions;
import org.act.temporalProperty.impl.InternalEntry;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.query.TimePointL;
import org.act.temporalProperty.query.range.InternalEntryRangeQueryCallBack;
import org.act.temporalProperty.util.Slice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Users who want custom aggregation query should extend this class
 * Created by song on 2018-04-01.
 */
public abstract class AbstractTimeIntervalAggrQuery<K,V> implements TimeIntervalEntryAggrQuery<K,V>, InternalEntryRangeQueryCallBack {
    private final Map<K, V> groupValMap = new HashMap<>();
    private final Map<K, List<TimeIntervalEntry>> groupListMap = new HashMap<>();
    private final TimePointL endTime;
    private final TimePointL startTime;
    private boolean hasEntry = false;
    private InternalEntry lastEntry;

    protected AbstractTimeIntervalAggrQuery( TimePointL startTime, TimePointL endTime )
    {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public boolean onNewEntry(InternalEntry entry) {
        hasEntry = true;
        InternalKey key = entry.getKey();
        TimePointL time = key.getStartTime();
        if ( lastEntry != null )
        {
            TimePointL lastTime = lastEntry.getKey().getStartTime();
            if ( lastTime.compareTo(startTime) < 0 )
            {
                lastTime = startTime;
                assert time.compareTo(startTime) > 0;
            }
            onEntry( lastTime, time.pre(), lastEntry.getValue() );
        }//else: do nothing
        if ( key.getValueType().isValue() )
        {
            lastEntry = entry;
        }
        else
        {
            lastEntry = null;
        }
        return true;
    }

    public Object onReturn() {
        if(hasEntry && lastEntry!=null && lastEntry.getKey().getStartTime().compareTo(endTime)<=0){
            onEntry(lastEntry.getKey().getStartTime(), endTime, lastEntry.getValue());
        }
        for(Entry<K, List<TimeIntervalEntry>> entry : groupListMap.entrySet()){
            V aggrValue = aggregate(entry.getKey(), entry.getValue());
            if(aggrValue!=null) groupValMap.put(entry.getKey(), aggrValue);
        }
        return onResult(groupValMap);
    }

    private void onEntry(TimePointL start, TimePointL end, Slice value){
        TimeIntervalEntry entry = new TimeIntervalEntry(start, end, value);
        K groupId = computeGroupId(entry);
        if(groupId!=null) {
            groupListMap.computeIfAbsent(groupId, k -> new ArrayList<>());
            groupListMap.get(groupId).add(entry);
        }
    }

    protected int asInt(Slice value){
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(value.length()>=4);
        return value.getInt(0);
    }

    protected float asFloat(Slice value){
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(value.length()>=4);
        return value.getFloat(0);
    }

    protected long asLong(Slice value){
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(value.length()>=8);
        return value.getLong(0);
    }

    protected double asDouble(Slice value){
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(value.length()>=8);
        return value.getDouble(0);
    }

}

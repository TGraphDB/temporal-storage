package org.act.temporalProperty.query.aggr;

import com.google.common.base.Preconditions;
import org.act.temporalProperty.impl.InternalEntry;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.query.range.InternalEntryRangeQueryCallBack;
import org.act.temporalProperty.util.Slice;

import java.util.*;
import java.util.Map.Entry;

/**
 * Users who want custom aggregation query should extend this class
 * Created by song on 2018-04-01.
 */
public abstract class SampleTimePointAggrQuery<K,V> implements InternalEntryRangeQueryCallBack {
    private final Map<K, V> groupValMap = new HashMap<>();
    private final Map<K, List<Entry<Integer, Slice>>> groupListMap = new HashMap<>();

    private int time = -1;
    private Slice lastValue;

    protected abstract K computeGroupId(int t, Slice value);

    protected abstract int computeNextTime(int time);

    protected abstract int computeStartTime();

    protected abstract Object onResult(Map<K, V> result);

    protected abstract V aggregate(K groupId, Collection<Entry<Integer, Slice>> groupItems);

    @Override
    public void onNewEntry(InternalEntry entry) {
        InternalKey key = entry.getKey();
        int curT = key.getStartTime();
        int t;
        if(time==-1) {
            t = computeStartTime();
        }else{
            t = computeNextTime(time);
        }
        while(t<curT) {
            addNewValue(t, lastValue);
            t = computeNextTime(t);
        }
        addNewValue(t, entry.getValue());
        time = t;
        lastValue = entry.getValue();
    }

    @Override
    public void setValueType(String valueType) {
        // do nothing.
    }

    @Override
    public Object onReturn() {
        for(Entry<K, List<Entry<Integer, Slice>>> e: groupListMap.entrySet()){
            groupValMap.put(e.getKey(), aggregate(e.getKey(), e.getValue()));
        }
        return onResult(groupValMap);
    }

    private void addNewValue(int t, Slice value){
        K groupId = computeGroupId(t, value);
        groupListMap.computeIfAbsent(groupId, k -> new ArrayList<>());
        groupListMap.get(groupId).add(new TimePointValueEntry(t, value));
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

    private static class TimePointValueEntry implements Entry<Integer, Slice>{

        private int time;
        private Slice value;

        private TimePointValueEntry(int time, Slice value) {
            this.time = time;
            this.value = value;
        }

        @Override
        public Integer getKey() {
            return time;
        }

        @Override
        public Slice getValue() {
            return value;
        }

        @Override
        public Slice setValue(Slice value) {
            throw new UnsupportedOperationException();
        }
    }
}

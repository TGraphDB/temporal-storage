package org.act.temporalProperty.index.aggregation;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import org.act.temporalProperty.exception.TPSNHException;
import org.act.temporalProperty.index.EntityTimeIntervalEntry;
import org.act.temporalProperty.query.TimePointL;
import org.act.temporalProperty.query.aggr.AggregationIndexKey;
import org.act.temporalProperty.query.aggr.ValueGroupingMap;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.TimeIntervalUtil;

import java.util.*;

/**
 * Created by song on 2018-04-06.
 */
public class Interval2AggrEntryIterator extends AbstractIterator<AggregationIndexEntry> implements PeekingIterator<AggregationIndexEntry> {
    private final Iterator<EntityTimeIntervalEntry> tpIter;
    private final ValueGroupingMap valueGrouping;
    private final NavigableSet<TimePointL> intervalStarts;
    private final TimePointL intervalBegin;
    private final TimePointL intervalFinish;

    private TimePointL eStart;
    private TimePointL eEnd;
    private EntityTimeIntervalEntry lastEntry;

    /**
     * @param iterator should only contains one property.
     * @param valueGrouping
     * @param intervalStarts interval start time point TreeSet
     */
    public Interval2AggrEntryIterator(Iterator<EntityTimeIntervalEntry> iterator, ValueGroupingMap valueGrouping, NavigableSet<TimePointL> intervalStarts ) {
        this.tpIter = iterator;
        this.valueGrouping = valueGrouping;
        this.intervalStarts = intervalStarts;
        if(intervalStarts.size()<2) throw new TPSNHException("time interval too less!");
        this.intervalBegin = intervalStarts.first();
        this.intervalFinish = intervalStarts.last().pre();
        if(intervalBegin.compareTo(intervalFinish)>0) throw new TPSNHException("time interval begin > finish!");
    }

    protected AggregationIndexEntry computeNext() {
        if(lastEntry!=null){
            return computeTimeGroup(this.eStart, this.eEnd, lastEntry);
        }else {
            while (tpIter.hasNext()) {
                EntityTimeIntervalEntry cur = tpIter.next();
                TimePointL eStart = cur.start();
                TimePointL eEnd = cur.end();
                if(eEnd.compareTo(eStart)<0) throw new TPSNHException("end({}) < start({})", eEnd, eStart);
                if(TimeIntervalUtil.overlap(eStart, eEnd, intervalBegin, intervalFinish)){
                    if(eStart.compareTo(intervalBegin)<0) eStart=intervalBegin;
                    if(eEnd.compareTo(intervalFinish)>0) eEnd=intervalFinish;
                    return computeTimeGroup(eStart, eEnd, cur);
                }//else: not in [intervalBegin, intervalFinish], do nothing, continue next loop;
            }
            return endOfData();
        }
    }

    private AggregationIndexEntry computeTimeGroup(TimePointL eStart, TimePointL eEnd, EntityTimeIntervalEntry entry) {
        long duration;
        TimePointL timeGroupId = intervalStarts.floor(eStart);
        // if eStart and eEnd both in the same time range.
        if(Objects.equals(timeGroupId, intervalStarts.floor(eEnd))){
            duration = eEnd.val() - eStart.val() + 1;
            lastEntry = null;
            return outputEntry(entry, timeGroupId, duration);
        }else{
            duration = intervalStarts.higher(eStart).val() - eStart.val(); //no need +1.
            this.eStart = intervalStarts.higher(eStart);
            this.eEnd = eEnd;
            lastEntry = entry;
            return outputEntry(entry, timeGroupId, duration);
        }
    }

    private AggregationIndexEntry outputEntry(EntityTimeIntervalEntry entry, TimePointL timeGroupId, long duration) {
        int valGroup = valueGrouping.group(entry.value());
        AggregationIndexKey key = new AggregationIndexKey(entry.entityId(), timeGroupId, valGroup);
        return new AggregationIndexEntry(key, duration);
    }

}

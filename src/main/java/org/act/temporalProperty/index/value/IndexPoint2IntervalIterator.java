package org.act.temporalProperty.index.value;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import org.act.temporalProperty.index.value.rtree.IndexEntry;
import org.act.temporalProperty.index.value.rtree.IndexEntryOperator;
import org.act.temporalProperty.query.TimePointL;
import org.act.temporalProperty.util.Slice;

import java.util.*;

/**
 * Created by song on 2018-01-19.
 */
public class IndexPoint2IntervalIterator extends AbstractIterator<IndexEntry> implements PeekingIterator<IndexEntry>{
    private final Iterator<TimePointEntry> tpIter;
    private final TimePointL startTime;
    private final TimePointL endTime;
    private final Map<Integer, TimePointEntry> map = new HashMap<>();
    private final List<Integer> proIdList;
    private final IndexEntryOperator op;
    private TimePointEntry lastEntry;
    private boolean reachEnd=false;

    public IndexPoint2IntervalIterator(List<Integer> proIds, List<TimePointEntry> data, TimePointL startTime, TimePointL endTime, IndexEntryOperator op){
        this.proIdList = proIds;
        this.startTime = startTime;
        this.endTime = endTime;
        this.op = op;
        data.sort((o1, o2) -> {
            int eidCmp = Long.compare(o1.getEntityId(), o2.getEntityId());
            if(eidCmp==0) {
                int timeCmp = o1.getTimePoint().compareTo(o2.getTimePoint());
                if(timeCmp==0){
                    return Integer.compare(o1.getPropertyId(), o2.getPropertyId());
                }else{
                    return timeCmp;
                }
            }else{
                return eidCmp;
            }
        });
        this.tpIter = data.iterator();
    }

    protected IndexEntry computeNext()
    {
        if(reachEnd) return endOfData();
        while(tpIter.hasNext()) {
            TimePointEntry cur = tpIter.next();
            long curEID = cur.getEntityId();
            TimePointL curTime = cur.getTimePoint();
            int curProId = cur.getPropertyId();

//            if(curTime.endTime>0) { //skip//todo: seems error, need rethink.
//                //do nothing, skip cur without update lastEntry: it is ok
//            }else
                if(lastEntry==null){ // start
                map.put(curProId, cur);
                lastEntry = cur;
            }else if(curEID != lastEntry.getEntityId()){ //cross entity: output then state to start
                IndexEntry result = outputEntry(endTime);
                map.clear();
                map.put(curProId, cur);
                lastEntry = cur;
                return result;
            }else{ // same entity
                if(curTime.compareTo(lastEntry.getTimePoint())>0){
                    if(curTime.compareTo(startTime)>0) { // should output
                        IndexEntry result = outputEntry(curTime.pre());
                        map.put(curProId, cur);
                        lastEntry = cur;
                        return result;
                    }else{
                        map.put(curProId, cur);
                        lastEntry = cur;
                    }
                }else if(curTime == lastEntry.getTimePoint()){
                    map.put(curProId, cur);
                    lastEntry = cur;
                }else{ // curTime < lastEntry.time
                    throw new RuntimeException("SNH: time not inc!");
                }
            }
        }
        if(map.isEmpty()){
            return endOfData();
        }else {
            reachEnd = true;
            return outputEntry(endTime);
        }
    }

    private IndexEntry outputEntry(TimePointL endTime) {
        Slice[] vList = new Slice[proIdList.size()];
        TimePointL latestStartTime = TimePointL.Init;
        long entityId = -1;
        for(int i=0; i<proIdList.size(); i++){
            Integer proId = proIdList.get(i);
            TimePointEntry point = map.get(proId);
            if(point==null){
                vList[i] = null;
            }else{
                vList[i] = point.getValue();
                if(latestStartTime.compareTo(point.getTimePoint())<0) latestStartTime = point.getTimePoint();
                if(entityId!=-1 && entityId!=point.getEntityId()){
                    throw new RuntimeException("SNH: entity not equal");
                }
                if(entityId==-1) entityId = point.getEntityId();
            }
        }
        if(latestStartTime.compareTo(startTime)<0) latestStartTime = startTime;
        return new IndexEntry(entityId, latestStartTime, endTime, vList);
    }


}

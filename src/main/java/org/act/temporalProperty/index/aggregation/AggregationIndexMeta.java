package org.act.temporalProperty.index.aggregation;

import com.google.common.collect.Lists;
import org.act.temporalProperty.index.IndexFileMeta;
import org.act.temporalProperty.index.IndexValueType;
import org.act.temporalProperty.index.value.IndexMetaData;
import org.act.temporalProperty.index.IndexType;
import org.act.temporalProperty.query.TimePointL;
import org.act.temporalProperty.query.aggr.ValueGroupingMap;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;
import org.act.temporalProperty.util.SliceOutput;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by song on 2018-04-06.
 */
public class AggregationIndexMeta extends IndexMetaData {
    private final TreeMap<Slice, Integer> vGroupMap;
    private final int tEvery;
    private final int timeUnit;
    private TimeGroupBuilder tGroupMap;

    public AggregationIndexMeta(long indexId, IndexType type, int pid, IndexValueType vType, TimePointL start, TimePointL end,
                                int tEvery, int timeUnit, TreeMap<Slice, Integer> valueGroup) {
        super( indexId, type, Lists.newArrayList( pid ), Lists.newArrayList( vType ), start, end );
        this.vGroupMap = valueGroup;
        this.tEvery = tEvery;
        this.timeUnit = timeUnit;
        this.tGroupMap = new UnixTimestampTimeGroupBuilder( getTimeStart(), getTimeEnd(), tEvery, timeUnit );
    }

    public TreeMap<Slice, Integer> getValGroupMap() {
        return vGroupMap;
    }

    public TimeGroupBuilder getTimeGroupMap()
    {
        return tGroupMap;
    }

    public int getTEvery() {
        return tEvery;
    }

    public int getTimeUnit() {
        return timeUnit;
    }

    @Override
    public String toString() {
        return "AggregationIndexMeta{" +
                "fileId=" + getId() +
                ", type=" + getType() +
                ", propertyId=" + getPropertyIdList().get(0) +
                ", timeStart=" + getTimeStart() +
                ", timeEnd=" + getTimeEnd() +
                '}';
    }


    public Slice encode(){
        DynamicSliceOutput out = new DynamicSliceOutput(128);
        encode(out);
        return out.slice();
    }

    public void encode(SliceOutput out){
        super.encode(out);
        out.writeInt(tEvery);
        out.writeInt(timeUnit);
        out.writeInt(vGroupMap.size());
        for(Map.Entry<Slice, Integer> entry : vGroupMap.entrySet()){
            out.writeInt(entry.getKey().length());
            out.writeBytes(entry.getKey());
            out.writeInt(entry.getValue());
        }
    }

    public static AggregationIndexMeta decode(SliceInput in){
        IndexType type = IndexType.decode(in.readInt());
        long fileId = in.readLong();
        TimePointL timeStart = TimePointL.decode(in);
        TimePointL timeEnd = TimePointL.decode(in);
        int count = in.readInt();
        assert count==1:"more then one property!";
        List<Integer> pidList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            pidList.add(in.readInt());
        }
        List<IndexValueType> valueTypes = new ArrayList<>();
        for (int i = 0; i < count; i++ )
        {
            valueTypes.add(IndexValueType.decode( in.readInt() ));
        }
        int tEvery = in.readInt();
        int timeUnit = in.readInt();
        TreeMap<Slice, Integer> valGroupMap = new TreeMap<>(ValueGroupingMap.getComparator(valueTypes.get(0)));
        count = in.readInt();
        for(int i=0; i<count; i++){
            int len = in.readInt();
            Slice key = in.readBytes(len);
            int groupId = in.readInt();
            valGroupMap.put(key, groupId);
        }
        return new AggregationIndexMeta(fileId, type, pidList.get(0), valueTypes.get(0), timeStart, timeEnd, tEvery, timeUnit, valGroupMap);
    }

    public static AggregationIndexMeta decode(Slice in){
        return decode(in.input());
    }

    public TreeSet<TimePointL> getTimeGroupAvailable(TimePointL start, TimePointL end )
    {
        TreeSet<TimePointL> result = new TreeSet<>();
        Collection<IndexFileMeta> files = this.getFilesByTime( start, end );
        for ( IndexFileMeta f : files )
        {
            result.addAll( f.getTimeGroups() );
        }
        return result;
    }
}

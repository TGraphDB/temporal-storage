package org.act.temporalProperty.index.value;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.act.temporalProperty.index.IndexFileMeta;
import org.act.temporalProperty.index.IndexType;
import org.act.temporalProperty.index.IndexValueType;
import org.act.temporalProperty.index.aggregation.AggregationIndexMeta;
import org.act.temporalProperty.query.TimePointL;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;
import org.act.temporalProperty.util.SliceOutput;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Created by song on 2018-01-17.
 */
public class IndexMetaData {
    private final List<IndexValueType> valueTypes;
    private long id;
    private IndexType type;
    private List<Integer> propertyIdList;
    private TimePointL timeStart;
    private TimePointL timeEnd;
    private Map<Long,IndexFileMeta> stableFileIds = new HashMap<>(); // key is corFileId, not indexFile id.
    private Map<Long,IndexFileMeta> unstableFileIds = new HashMap<>();
    private TreeMap<TimePointL,IndexFileMeta> fileByTime = new TreeMap<>();
    private boolean online;

    public IndexMetaData(long id, IndexType type, List<Integer> pidList, List<IndexValueType> types, TimePointL start, TimePointL end ) {
        Preconditions.checkArgument( start.compareTo(end) <= 0 );
        this.id = id;
        this.type = type;
        this.propertyIdList = pidList;
        this.valueTypes = types;
        this.timeStart = start;
        this.timeEnd = end;
        this.online = false;
    }

    public long getId() {
        return id;
    }

    public IndexType getType() {
        return type;
    }

    public TimePointL getTimeStart() {
        return timeStart;
    }

    public TimePointL getTimeEnd() {
        return timeEnd;
    }

    public List<Integer> getPropertyIdList(){
        return propertyIdList;
    }

    public void setOnline()
    {
        online = true;
    }

    public boolean isOnline()
    {
        return online;
    }

    public List<IndexValueType> getValueTypes()
    {
        return valueTypes;
    }

    @Override
    public String toString() {
        return "IndexMetaData{" +
                "id=" + id +
                ", type=" + type +
                ", propertyIdList=" + propertyIdList +
                ", timeStart=" + timeStart +
                ", timeEnd=" + timeEnd +
                '}';
    }

    public Slice encode(){
        DynamicSliceOutput out = new DynamicSliceOutput(128);
        encode(out);
        return out.slice();
    }

    public void encode(SliceOutput out){
        out.writeInt(this.getType().getId());
        out.writeLong(this.getId());
        out.writeInt(this.getTimeStart());
        out.writeInt(this.getTimeEnd());
        out.writeInt(this.getPropertyIdList().size());
        for(Integer pid : this.getPropertyIdList()){
            out.writeInt(pid);
        }
        for(IndexValueType type : this.getValueTypes()){
            out.writeInt(type.getId());
        }
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
        IndexMetaData that = (IndexMetaData) o;
        return id == that.id;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode( id );
    }

    public IndexMetaData( SliceInput in ){
        this.type = IndexType.decode(in.readInt());
        this.id = in.readInt();
        this.timeStart = in.readInt();
        this.timeEnd = in.readInt();
        int count = in.readInt();
        List<Integer> pidList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            pidList.add(in.readInt());
        }
        List<IndexValueType> valueTypes = new ArrayList<>();
        for (int i = 0; i < count; i++ )
        {
            valueTypes.add(IndexValueType.decode( in.readInt() ));
        }
        this.propertyIdList = pidList;
        this.valueTypes = valueTypes;
    }

    public static IndexMetaData decode(Slice in){
        IndexType type = IndexType.decode(in.getInt(0));
        if(type==IndexType.SINGLE_VALUE || type==IndexType.MULTI_VALUE){
            return new IndexMetaData(in.input());
        }else{
            return AggregationIndexMeta.decode(in);
        }
    }

    public void addFile( IndexFileMeta fileMeta )
    {
        if ( fileMeta.isCorIsStable() )
        {
            stableFileIds.put( fileMeta.getCorFileId(), fileMeta );
            fileByTime.put( fileMeta.getStartTime(), fileMeta );
        }
        else
        {
            unstableFileIds.put( fileMeta.getCorFileId(), fileMeta );
            fileByTime.put( fileMeta.getStartTime(), fileMeta );
        }
    }

    public IndexFileMeta getByCorFileId( long fileId, boolean isStable )
    {
        if ( isStable )
        {
            return stableFileIds.get( fileId );
        }
        else
        {
            return unstableFileIds.get( fileId );
        }
    }

    public List<IndexFileMeta> allFiles()
    {
        List<IndexFileMeta> result = new ArrayList<>( stableFileIds.values() );
        result.addAll( unstableFileIds.values() );
        return result;
    }

    public void delFileByCorFileId( long fileId, boolean isStable )
    {
        if ( isStable )
        {
            stableFileIds.remove( fileId );
        }
        else
        {
            unstableFileIds.remove( fileId );
        }
        fileByTime.entrySet().removeIf( entry -> {
            IndexFileMeta fMeta = entry.getValue();
            return fMeta.isCorIsStable()==isStable && fMeta.getCorFileId() == fileId;
        });
    }

    /**
     * @param start inclusive
     * @param end   inclusive
     * @return file which time overlaps this range.
     */
    public Collection<IndexFileMeta> getFilesByTime(TimePointL start, TimePointL end )
    {
        Entry<TimePointL, IndexFileMeta> floorKey = fileByTime.floorEntry( start );
        if ( floorKey == null )
        {
            return fileByTime.subMap( start, true, end, true ).values();
        }
        else if ( floorKey.getKey() == start )
        {
            return fileByTime.subMap( start, true, end, true ).values();
        }else{
            List<IndexFileMeta> result = new ArrayList<>();
            result.add(floorKey.getValue());
            result.addAll(fileByTime.subMap( start, true, end, true ).values());
            return result;
        }
    }
}

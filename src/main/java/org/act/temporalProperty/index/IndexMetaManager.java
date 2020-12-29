package org.act.temporalProperty.index;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import org.act.temporalProperty.index.aggregation.AggregationIndexMeta;
import org.act.temporalProperty.index.value.IndexMetaData;
import org.act.temporalProperty.query.TimePointL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * just like systemMeta.
 * Created by song on 2018-05-07.
 */
public class IndexMetaManager
{
    private final AtomicLong nextIndexId;
    private final AtomicLong nextFileId;

    private final HashMap<Long,IndexMetaData> byId = new HashMap<>();
    private final Map<Integer,TreeMap<TimePointL,IndexMetaData>> byProIdTime = new HashMap<>(); // proId, time

    private final LinkedList<IndexMetaData> offLineIndexes = new LinkedList<>();

    public IndexMetaManager( Set<IndexMetaData> indexes, long nextId, long nextFileId )
    {
        this.nextIndexId = new AtomicLong( nextId );
        this.nextFileId = new AtomicLong( nextFileId );
        for ( IndexMetaData meta : indexes )
        {
            addMeta( meta );
        }
    }

    public long nextIndexId()
    {
        return nextIndexId.getAndIncrement();
    }

    public long nextFileId()
    {
        return nextFileId.getAndIncrement();
    }

    public long getNextIndexId() {
        return nextIndexId.get();
    }

    public long getNextFileId() {
        return nextFileId.get();
    }

    public void addOfflineMeta(IndexMetaData indexMetaData )
    {
        offLineIndexes.add( indexMetaData );
    }

    public List<IndexMetaData> offLineValueIndexes()
    {
        return offLineIndexes.stream().filter( indexMetaData ->
                                               {
                                                   IndexType t = indexMetaData.getType();
                                                   return t == IndexType.SINGLE_VALUE || t == IndexType.MULTI_VALUE;
                                               } ).collect( Collectors.toList() );
    }

    public List<AggregationIndexMeta> offLineAggrIndexes()
    {
        return offLineIndexes.stream().filter( indexMetaData ->
                                               {
                                                   IndexType t = indexMetaData.getType();
                                                   return t.getId() >= 2;
                                               } ).map( indexMetaData -> (AggregationIndexMeta) indexMetaData ).collect( Collectors.toList() );
    }

    public void setOnline( IndexMetaData meta )
    {
        meta.setOnline();
        offLineIndexes.remove( meta );
        addMeta( meta );
    }

    public IndexMetaData getByIndexId( long indexId )
    {
        return byId.get( indexId );
    }

    public List<IndexMetaData> getByProId( int propertyId )
    {
        TreeMap<TimePointL,IndexMetaData> indexTimeMap = byProIdTime.get( propertyId );
        if ( indexTimeMap != null )
        {
            return new ArrayList<>( indexTimeMap.values() );
        }
        else
        {
            return Collections.emptyList();
        }
    }

    public void addMeta( IndexMetaData indexMeta )
    {
        if ( indexMeta.isOnline() )
        {
            byId.put( indexMeta.getId(), indexMeta );
            for ( Integer proId : indexMeta.getPropertyIdList() )
            {
                byProIdTime.computeIfAbsent( proId, i -> new TreeMap<>() ).put( indexMeta.getTimeStart(), indexMeta );
            }
        }
        else
        {
            offLineIndexes.add( indexMeta );
        }
    }

    public List<IndexMetaData> getValueIndex(List<Integer> pids, TimePointL timeMin, TimePointL timeMax )
    {
        return pids.stream()
                .map(proId -> byProIdTime.get(proId).subMap(timeMin, true, timeMax, true).values())
                .flatMap(Collection::stream)
                .filter(indexMetaData -> containsAll(indexMetaData.getPropertyIdList(), pids)).distinct().collect(Collectors.toList());
    }

    private boolean containsAll( List<Integer> a, List<Integer> b )
    {
        return new HashSet<>( a ).containsAll(b);
    }

    public List<IndexMetaData> allIndexes()
    {
        List<IndexMetaData> all = new ArrayList<>();
        all.addAll( byId.values() );
        all.addAll( offLineIndexes );
        return all;
    }

    public boolean isOnline( long indexId )
    {
        return byId.get( indexId ) != null;
    }
}

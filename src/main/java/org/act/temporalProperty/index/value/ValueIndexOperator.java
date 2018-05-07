package org.act.temporalProperty.index.value;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import org.act.temporalProperty.exception.TPSRuntimeException;
import org.act.temporalProperty.helper.SameLevelMergeIterator;
import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.index.IndexFileMeta;
import org.act.temporalProperty.index.IndexMetaManager;
import org.act.temporalProperty.index.IndexTableCache;
import org.act.temporalProperty.index.IndexValueType;
import org.act.temporalProperty.index.PropertyFilterIterator;
import org.act.temporalProperty.index.value.rtree.IndexEntry;
import org.act.temporalProperty.index.value.rtree.IndexEntryOperator;
import org.act.temporalProperty.util.TimeIntervalUtil;
import org.apache.commons.lang3.tuple.Triple;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.Map.Entry;

import static org.act.temporalProperty.index.IndexType.*;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Created by song on 2018-04-07.
 */
public class ValueIndexOperator
{
    private final TemporalPropertyStoreImpl tpStore;
    private final IndexTableCache cache;
    private final File indexDir;
    private final IndexMetaManager sysIndexMeta;

    public ValueIndexOperator( File indexDir, TemporalPropertyStoreImpl store, IndexTableCache cache, IndexMetaManager meta )
    {
        this.indexDir = indexDir;
        this.tpStore = store;
        this.cache = cache;
        this.sysIndexMeta = meta;
    }

    public long create(int start, int end, List<Integer> proIds, List<IndexValueType> types) throws IOException {
        long indexId = sysIndexMeta.nextIndexId();
        if (proIds.size() == 1) {
            sysIndexMeta.addOfflineMeta( new IndexMetaData( indexId, SINGLE_VALUE, proIds, types, start, end ) );
        } else {
            sysIndexMeta.addOfflineMeta( new IndexMetaData( indexId, MULTI_VALUE, proIds, types, start, end ) );
        }
        return indexId;
    }

    public List<IndexEntry> query(IndexQueryRegion condition) throws IOException {
        IndexMetaData meta = valIndexCoverRegion(condition);
        Iterator<IndexEntry> iter = this.getQueryIterator(condition, meta);
        List<IndexEntry> result = new ArrayList<>();
        while(iter.hasNext()){
            result.add(iter.next());
        }
        return result;
    }

    private IndexMetaData valIndexCoverRegion(IndexQueryRegion condition) {
        List<PropertyValueInterval> proIntervals = condition.getPropertyValueIntervals();
        List<Integer> pids = proIntervals.stream().map( PropertyValueInterval::getProId ).collect( Collectors.toList() );
        List<IndexMetaData> indexList = sysIndexMeta.getValueIndex( pids, condition.getTimeMin(), condition.getTimeMax() );
        for(IndexMetaData meta : indexList){
            int s = meta.getTimeStart();
            int e = meta.getTimeEnd();
            if(TimeIntervalUtil.contains( s, e, condition.getTimeMin(), condition.getTimeMax())){
                Set<Integer> pSetMeta = new HashSet<>(meta.getPropertyIdList());
                if(pSetMeta.size()==pids.size() && !pSetMeta.retainAll(pids)) {
                    return meta;
                }
            }
        }
        throw new TPSRuntimeException("no valid index for query!");
    }

    private Iterator<IndexEntry> getQueryIterator(IndexQueryRegion condition, IndexMetaData meta) throws IOException {
        String fileName = Filename.valIndexFileName(meta.getId());
        return cache.getTable(new File(this.indexDir, fileName).getAbsolutePath()).iterator(condition);
    }

    public List<BackgroundTask> createIndexTasks()
    {
        List<BackgroundTask> result = new ArrayList<>();
        List<IndexMetaData> notCreated = sysIndexMeta.offLineValueIndexes();
        for ( IndexMetaData meta : notCreated )
        {
            if ( meta.getType() == MULTI_VALUE )
            {
                result.add( new CreateMultiPropertyValueIndexTask( meta ) );
            }
            else
            {
                result.add( new CreateSinglePropertyValueIndexTask( meta ) );
            }
        }
        return result;
    }

    public class CreateSinglePropertyValueIndexTask implements BackgroundTask
    {
        private final IndexMetaData iMeta;

        public CreateSinglePropertyValueIndexTask( IndexMetaData meta )
        {
            this.iMeta = meta;
            Preconditions.checkArgument( meta.getPropertyIdList().size() == 1 );
        }

        @Override
        public void runTask() throws IOException
        {
            IndexEntryOperator op = new IndexEntryOperator( iMeta.getValueTypes(), 4096 );
            List<Triple<Boolean,FileMetaData,SearchableIterator>> iterators =
                    tpStore.buildIndexIterator( iMeta.getTimeStart(), iMeta.getTimeEnd(), iMeta.getPropertyIdList() );
            for ( Triple<Boolean,FileMetaData,SearchableIterator> i : iterators )
            {
                SearchableIterator iterator = new PropertyFilterIterator( iMeta.getPropertyIdList(), i.getRight() );
                IndexBuilderCallback dataCollector = new IndexBuilderCallback( iMeta.getPropertyIdList(), op );
                while ( iterator.hasNext() )
                {
                    InternalEntry entry = iterator.next();
                    InternalKey key = entry.getKey();
                    if ( key.getValueType() == ValueType.INVALID )
                    {
                        dataCollector.onCall( key.getPropertyId(), key.getEntityId(), key.getStartTime(), null );
                    }
                    else
                    {
                        dataCollector.onCall( key.getPropertyId(), key.getEntityId(), key.getStartTime(), entry.getValue() );
                    }
                }
                FileMetaData dataFileMetaData = i.getMiddle();
                PeekingIterator<IndexEntry> data = dataCollector.getIterator( dataFileMetaData.getSmallest(), dataFileMetaData.getLargest() );

                long fileId = sysIndexMeta.nextFileId();
                String indexFilePath = Filename.valIndexFileName( fileId );
                try ( FileChannel channel = new FileOutputStream( new File( indexDir, indexFilePath ) ).getChannel() )
                {
                    IndexTableWriter writer = new IndexTableWriter( channel, op );
                    while ( data.hasNext() )
                    {
                        writer.add( data.next() );
                    }
                    writer.finish();
                    long fileSize = channel.size();
                    IndexFileMeta fileMeta = new IndexFileMeta( iMeta.getId(), fileId, fileSize, i.getMiddle().getNumber(), i.getLeft() );
                    iMeta.addFile( fileMeta );
                }
            }
        }

        @Override
        public void updateMeta() throws IOException
        {
            sysIndexMeta.setOnline( iMeta );
        }

        @Override
        public void cleanUp() throws IOException
        {
            // nothing to do. fresh creation.
        }
    }

    public class CreateMultiPropertyValueIndexTask implements BackgroundTask
    {
        private final IndexMetaData iMeta;

        public CreateMultiPropertyValueIndexTask( IndexMetaData meta )
        {
            this.iMeta = meta;
            Preconditions.checkArgument( meta.getPropertyIdList().size() > 1 );
        }

        @Override
        public void runTask() throws IOException
        {
            IndexEntryOperator op = new IndexEntryOperator( iMeta.getValueTypes(), 4096 );
            List<Triple<Boolean,FileMetaData,SearchableIterator>> iterators =
                    tpStore.buildIndexIterator( iMeta.getTimeStart(), iMeta.getTimeEnd(), iMeta.getPropertyIdList() );

            List<SearchableIterator> iteratorList = new ArrayList<>();
            for ( Triple<Boolean,FileMetaData,SearchableIterator> i : iterators )
            {
                iteratorList.add( i.getRight() );
            }
            SearchableIterator iterator = new PropertyFilterIterator( iMeta.getPropertyIdList(), new SameLevelMergeIterator( iteratorList ) );
            IndexBuilderCallback dataCollector = new IndexBuilderCallback( iMeta.getPropertyIdList(), op );
            while ( iterator.hasNext() )
            {
                InternalEntry entry = iterator.next();
                InternalKey key = entry.getKey();
                if ( key.getValueType() == ValueType.INVALID )
                {
                    dataCollector.onCall( key.getPropertyId(), key.getEntityId(), key.getStartTime(), null );
                }
                else
                {
                    dataCollector.onCall( key.getPropertyId(), key.getEntityId(), key.getStartTime(), entry.getValue() );
                }
            }
            PeekingIterator<IndexEntry> data = dataCollector.getIterator( iMeta.getTimeStart(), iMeta.getTimeEnd() );

            long fileId = sysIndexMeta.nextFileId();
            String indexFilePath = Filename.valIndexFileName( fileId );
            try ( FileChannel channel = new FileOutputStream( new File( indexDir, indexFilePath ) ).getChannel() )
            {
                IndexTableWriter writer = new IndexTableWriter( channel, op );
                while ( data.hasNext() )
                {
                    writer.add( data.next() );
                }
                writer.finish();
                long fileSize = channel.size();
                IndexFileMeta fileMeta = new IndexFileMeta( iMeta.getId(), fileId, fileSize, 0, false );
                iMeta.addFile( fileMeta );
            }
        }

        @Override
        public void updateMeta() throws IOException
        {
            sysIndexMeta.setOnline( iMeta );
        }

        @Override
        public void cleanUp() throws IOException
        {
            // nothing to do. fresh creation.
        }
    }
}

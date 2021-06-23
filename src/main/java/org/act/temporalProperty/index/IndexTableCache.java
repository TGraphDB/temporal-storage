package org.act.temporalProperty.index;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.index.value.IndexQueryRegion;
import org.act.temporalProperty.index.value.IndexTableIterator;
import org.act.temporalProperty.index.value.PropertyValueInterval;
import org.act.temporalProperty.index.value.cardinality.HyperLogLog;
import org.act.temporalProperty.index.value.cardinality.RTreeCardinality;
import org.act.temporalProperty.index.value.rtree.IndexEntry;
import org.act.temporalProperty.index.value.rtree.IndexEntryOperator;
import org.act.temporalProperty.query.aggr.AggregationIndexKey;
import org.act.temporalProperty.table.MMapTable;
import org.act.temporalProperty.util.Slice;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by song on 2018-01-19.
 */
public class IndexTableCache {

    private final LoadingCache<String, IndexTableFile> cache;
    private final Map<String, Long> loadFreq = new HashMap<>();

    public IndexTableCache(final File indexDir, int tableCacheSize)
    {
        Preconditions.checkNotNull(indexDir, "databaseName is null");
        cache = CacheBuilder.newBuilder()
                .maximumSize(tableCacheSize)
                .removalListener((RemovalListener<String, IndexTableFile>) notification -> {

                })
                .build(new CacheLoader<String, IndexTableFile>(){
                    @Override
                    public IndexTableFile load(String fileAbsPath) throws IOException{
                        loadFreq.compute(fileAbsPath, (s, aLong) -> aLong==null ? 1 : aLong+1);
                        return new IndexTableFile(fileAbsPath);
                    }
                });
    }

    public IndexTable getTable(String absPath)
    {
        IndexTable table;
        try {
            table = cache.get(absPath).getTable();
        } catch (ExecutionException e) {
            Throwable cause = e;
            if (e.getCause() != null) {
                cause = e.getCause();
            }
            throw new RuntimeException("Could not open table " + absPath, cause);
        }
        return table;
    }

    /**
     * 关闭缓存，将缓存在内存中的文件channel关闭
     */
    public void close(){
        System.out.println("IndexTableCache.close: "+ loadFreq);
        cache.invalidateAll();
    }

    /**
     * 将某个文件从缓存中排除
     * @param fileAbsPath 文件编号
     */
    public void evict(String fileAbsPath){
        cache.invalidate(fileAbsPath);
    }

    private static final class IndexTableFile
    {
        private final IndexTable table;

        private IndexTableFile(String fileAbsPath) throws IOException{
            FileChannel fileChannel = new RandomAccessFile(new File(fileAbsPath), "rw").getChannel();
            if(fileAbsPath.contains("value.")) {
                table = new TimeValueIndexTable(fileChannel);
            }else{
                table = new AggrIndexTable(fileAbsPath, fileChannel);
            }
        }

        public IndexTable getTable()
        {
            return table;
        }
    }

    /**
     * Created by song on 2018-01-19.
     */
    public interface IndexTable {

        Iterator<IndexEntry> iterator(IndexQueryRegion regions) throws IOException;

        SeekingIterator<Slice, Slice> iterator() throws IOException;

        HyperLogLog cardinalityEstimator(IndexQueryRegion regions) throws IOException;
    }

    private static class TimeValueIndexTable implements IndexTable{
        private final FileChannel channel;
        public TimeValueIndexTable(FileChannel fileChannel) {
            this.channel = fileChannel;
        }

        @Override
        public Iterator<IndexEntry> iterator(IndexQueryRegion regions) throws IOException {
            return new IndexTableIterator(this.channel, regions, extractOperator(regions));
        }

        private IndexEntryOperator extractOperator(IndexQueryRegion regions) {
            List<IndexValueType> types = new ArrayList<>();
            for(PropertyValueInterval p : regions.getPropertyValueIntervals()){
                types.add(p.getType());
            }
            return new IndexEntryOperator(types, 4096);
        }

        @Override
        public SeekingIterator<Slice, Slice> iterator() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public HyperLogLog cardinalityEstimator(IndexQueryRegion regions) throws IOException {
            return new RTreeCardinality(this.channel, regions, extractOperator(regions)).cardinalityEstimator();
        }
    }

    private static class AggrIndexTable implements IndexTable{
        private final MMapTable table;

        public AggrIndexTable(String filePath, FileChannel channel) throws IOException {
            this.table = new MMapTable( filePath, channel, AggregationIndexKey.sliceComparator, false);
        }

        public SeekingIterator<Slice, Slice> iterator(){
            return table.iterator();
        }

        @Override
        public Iterator<IndexEntry> iterator(IndexQueryRegion regions) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public HyperLogLog cardinalityEstimator(IndexQueryRegion regions) throws IOException {
            throw new UnsupportedOperationException();
        }
    }
}

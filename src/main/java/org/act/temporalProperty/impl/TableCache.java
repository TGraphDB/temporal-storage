
package org.act.temporalProperty.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.RemovalListener;
import org.act.temporalProperty.table.FileChannelTable;
import org.act.temporalProperty.table.MMapTable;
import org.act.temporalProperty.table.Table;
import org.act.temporalProperty.table.UserComparator;
import org.act.temporalProperty.util.Closeables;
import org.act.temporalProperty.util.Finalizer;
import org.act.temporalProperty.util.Slice;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * 对所有存储文件的缓存机制，使用FIFO的方式进行，如果需要对某个文件进行访问，可以直接从TableCache这里对文件的Iterator进行查询
 *
 */
public class TableCache
{
    private final LoadingCache<String, TableAndFile> cache;
    private final Finalizer<Table> finalizer = new Finalizer<>(1);
    private final Map<String, Long> loadFreq = new HashMap<>();

    public TableCache(int tableCacheSize, final UserComparator userComparator, final boolean verifyChecksums)
    {
        cache = CacheBuilder.newBuilder()
                .maximumSize(tableCacheSize)
                .removalListener((RemovalListener<String, TableAndFile>) notification -> {
                    Table table = notification.getValue().getTable();
                    finalizer.addCleanup(table, table.closer());
                })
                .build(new CacheLoader<String, TableAndFile>(){
                    @Override
                    public TableAndFile load(String filePath) throws IOException{
                        loadFreq.compute(filePath, (s, aLong) -> aLong==null ? 1 : aLong+1);
                        return new TableAndFile(filePath, userComparator, verifyChecksums);
                    }
                });
    }

    /**
     * 通过文件的编号，得到相应文件的Iterator
     * @param filePath 文件编号
     * @return 相应文件的Iterator
     */
    public SearchableIterator newIterator(String filePath)
    {
        return new PackInternalKeyIterator(getTable(filePath).iterator());
    }

    public Table getTable(String filePath)
    {
        Table table;
        try {
            table = cache.get(filePath).getTable();
        }
        catch (ExecutionException e) {
            Throwable cause = e;
            if (e.getCause() != null) {
                cause = e.getCause();
            }
            throw new RuntimeException("Could not open table " + filePath, cause);
        }
        return table;
    }

    /**
     * 关闭缓存，将缓存在内存中的文件channel关闭
     */
    public void close()
    {
        System.out.println("TableCache.close: "+loadFreq);
        cache.invalidateAll();
        finalizer.destroy();
    }

    /**
     * 将某个文件从缓存中排除
     * @param filePath 文件绝对路径
     */
    public void evict(String filePath)
    {
        cache.invalidate(filePath);
    }

    private static final class TableAndFile
    {
        private final Table table;
        private final FileChannel fileChannel;
    	
    	private TableAndFile(String filePath, UserComparator userComparator, boolean verifyChecksums) throws IOException{
            fileChannel = new RandomAccessFile(filePath,"rw").getChannel();
            try {
                //FIXME 
                if ( true ) {
                    table = new MMapTable(filePath, fileChannel, userComparator, verifyChecksums);
                }else{
                    table = new FileChannelTable(filePath, fileChannel, userComparator, verifyChecksums);
                }
            } catch (IOException e) {
                Closeables.closeQuietly(fileChannel);
                throw e;
            }
        }

        public Table getTable()
        {
            return table;
        }
    }
}

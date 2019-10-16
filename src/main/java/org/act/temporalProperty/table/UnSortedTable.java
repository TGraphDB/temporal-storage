package org.act.temporalProperty.table;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.*;

import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.vo.TimeIntervalValueEntry;
import org.act.temporalProperty.query.TimeIntervalKey;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;

/**
 * Buffer的备份文件。文件中的写入都是在末尾进行append。文件内数据没有顺序
 */
public class UnSortedTable implements Closeable
{
    private final FileChannelLogWriter log;
    private final File file;

    public UnSortedTable( File tableFile ) throws IOException
    {
        this.file = tableFile;
        this.log = new FileChannelLogWriter(tableFile, 0);
    }

    public void initFromFile( MemTable table ) throws IOException
    {
        FileInputStream inputStream = new FileInputStream(file);
        FileChannel channel = inputStream.getChannel();
        LogReader logReader = new LogReader(channel, null, false, 0);
        Slice record;
        List<TimeIntervalValueEntry> entries = new LinkedList<>();
        while ((record = logReader.readRecord()) != null) {
            SliceInput in = record.input();
            boolean isCheckPoint = in.readBoolean();
            if(!isCheckPoint) {
                int entryLen = in.readInt();
                Slice entryRaw = in.readBytes(entryLen);
                TimeIntervalValueEntry entry = TimeIntervalValueEntry.decode( entryRaw.input() );
                entries.add(entry);
            }else{
                Iterator<TimeIntervalValueEntry> iterator = entries.iterator();
                while(iterator.hasNext()) {
                    TimeIntervalValueEntry entry = iterator.next();
                    table.addInterval(entry.getKey(), entry.getValue());
                }
                entries.clear();
            }
        }
        channel.close();
        inputStream.close();
    }

    public void add( TimeIntervalKey key, Slice value ) throws IOException
    {
        DynamicSliceOutput out = new DynamicSliceOutput(64);
        boolean isCheckPoint = false;
        out.writeBoolean(isCheckPoint);
        Slice entry = TimeIntervalValueEntry.encode( key, value );
        out.writeInt( entry.length() );
        out.writeBytes( entry );
        this.log.addRecord(out.slice(), false);
    }

    public void addCheckPoint() throws IOException {
        Slice checkPoint = new Slice(1);
        checkPoint.setByte(0, 1);
        this.log.addRecord(checkPoint, true);
    }

    @Override
    public void close() throws IOException
    {
        this.log.close();
    }
}

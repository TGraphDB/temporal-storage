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
        this.log = new FileChannelLogWriter(tableFile, false);
    }

    public UnSortedTable( File tableFile, MemTable table ) throws IOException
    {
        this.file = tableFile;
        try{this.initFromFile( table);}catch (RuntimeException e){
            System.out.println("UnSortedTable.initFromFile: "+tableFile+" "+e.getMessage());
        }
        this.log = new FileChannelLogWriter(tableFile, true);
    }

    private void initFromFile( MemTable table ) throws IOException
    {
        try(    FileInputStream inputStream = new FileInputStream(file);
                FileChannel channel = inputStream.getChannel()) {
            LogReader logReader = new LogReader(channel, null, false, 0);
            Slice record;
            while ((record = logReader.readRecord()) != null) {
                SliceInput in = record.input();
                boolean isCheckPoint = in.readBoolean();
                if (!isCheckPoint) {
                    TimeIntervalValueEntry entry = TimeIntervalValueEntry.decode(in);
                    table.addInterval(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    public void add( TimeIntervalKey key, Slice value ) throws IOException
    {
        DynamicSliceOutput out = new DynamicSliceOutput(64);
        boolean isCheckPoint = false;
        out.writeBoolean(isCheckPoint);
        new TimeIntervalValueEntry( key, value ).encode(out);
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

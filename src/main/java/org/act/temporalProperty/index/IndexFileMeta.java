package org.act.temporalProperty.index;

import org.act.temporalProperty.query.TimePointL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by song on 2018-05-06.
 */
public class IndexFileMeta
{
    private final long indexId;
    private final long fileId;
    private final long fileSize;
    private final TimePointL startTime;
    private final TimePointL endTime;

    //corresponding storage file properties, for single-property time value index and aggregation index.
    private long corFileId;
    private boolean corIsStable;

    //time group start point for aggregation index only. the last point is endTime + 1
    private final List<TimePointL> timeGroup;

    public IndexFileMeta(long indexId, long fileId, long fileSize, TimePointL startTime, TimePointL endTime, long corFileId, Boolean corIsStable, Collection<TimePointL> timeGroup )
    {
        this.indexId = indexId;
        this.fileId = fileId;
        this.fileSize = fileSize;
        this.startTime = startTime;
        this.endTime = endTime;
        this.corFileId = corFileId;
        this.corIsStable = corIsStable;
        this.timeGroup = new ArrayList<>();
        this.timeGroup.addAll( timeGroup );
        this.timeGroup.sort( TimePointL::compareTo );
    }

    public long getIndexId()
    {
        return indexId;
    }

    public long getFileId()
    {
        return fileId;
    }

    public long getFileSize()
    {
        return fileSize;
    }

    public TimePointL getStartTime()
    {
        return startTime;
    }

    public TimePointL getEndTime()
    {
        return endTime;
    }

    public long getCorFileId()
    {
        return corFileId;
    }

    public boolean isCorIsStable()
    {
        return corIsStable;
    }

    public List<TimePointL> getTimeGroups()
    {
        return timeGroup;
    }
}

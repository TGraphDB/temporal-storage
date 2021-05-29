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
    private long indexId;
    private long fileId;
    private long fileSize;
    private TimePointL startTime;
    private TimePointL endTime;

    //corresponding storage file properties, for single-property time value index and aggregation index.
    private long corFileId;
    private boolean corIsStable;

    //time group start point for aggregation index only. the last point is endTime + 1
    private List<TimePointL> timeGroup;

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
    }

    public IndexFileMeta() {
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

    public void setIndexId(long indexId) {
        this.indexId = indexId;
    }

    public void setFileId(long fileId) {
        this.fileId = fileId;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public void setStartTime(TimePointL startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(TimePointL endTime) {
        this.endTime = endTime;
    }

    public void setCorFileId(long corFileId) {
        this.corFileId = corFileId;
    }

    public void setCorIsStable(boolean corIsStable) {
        this.corIsStable = corIsStable;
    }

    public List<TimePointL> getTimeGroup() {
        return timeGroup;
    }

    public void setTimeGroup(List<TimePointL> timeGroup) {
        this.timeGroup = timeGroup;
    }
}

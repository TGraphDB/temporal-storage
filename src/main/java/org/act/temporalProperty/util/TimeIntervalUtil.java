package org.act.temporalProperty.util;

import org.act.temporalProperty.query.TimeInterval;
import org.act.temporalProperty.query.TimePointL;

public class TimeIntervalUtil
{
    public static boolean overlap(TimePointL start1, TimePointL end1, TimePointL start2, TimePointL end2 )
    {
        return !( end1.compareTo(start2) < 0 || end2.compareTo(start1) < 0 );
    }

    public static boolean contains(TimePointL start1, TimePointL end1, TimePointL start2, TimePointL end2 )
    {
        return start1.compareTo(start2)<=0 && end1.compareTo(end2)>=0;
    }

    public static TimeInterval overlapInterval(TimePointL min, TimePointL max, TimePointL timeStart, TimePointL timeEnd) {
        TimePointL begin = maxTimePoint(min, timeStart);
        TimePointL finish = minTimePoint(max, timeEnd);
        return new TimeInterval(begin, finish);
    }

    private static TimePointL maxTimePoint(TimePointL a, TimePointL b) {
        return a.compareTo(b)>0 ? a : b;
    }

    private static TimePointL minTimePoint(TimePointL a, TimePointL b) {
        return a.compareTo(b)<0 ? a : b;
    }


}

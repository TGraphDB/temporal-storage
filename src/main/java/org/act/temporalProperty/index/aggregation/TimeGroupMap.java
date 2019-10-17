package org.act.temporalProperty.index.aggregation;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.act.temporalProperty.exception.TPSRuntimeException;
import org.act.temporalProperty.query.TimeInterval;
import org.act.temporalProperty.query.TimePointL;
import org.act.temporalProperty.util.TimeIntervalUtil;
import org.apache.commons.lang3.time.DateUtils;

import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * 计算指定时间区间上建立索引分组的每组的起始时间及组ID。
 * 例如，给定start, end为某天的8:04:33到8:23:15, timeUnit为分钟, every=7, 则切分结果为:
 * [groupId=0]  8:05:00~8:11:59
 * [groupId=1]  8:12:00~8:18:59
 * Aggregation索引会在每个group上分别计算索引值, 并在查询时读取被查询时间区间完全覆盖的group的索引值以加速查询.
 * 返回结果为TreeMap{<8:05:00的时间戳, 0>, <8:12:00的时间戳, 1>}
 * 其中8:05:00是时间轴按分钟划分后, start之后(>=)group开始的最小时间.(通过DateUtils.round函数实现)
 * 8:19:00=8:19:00-0:0:1是时间轴按分钟划分后, end之前的最大时间时间减一秒.
 * 由于every=7, 所以每连续的7分钟作为一组
 * <p>
 * param start in second
 * param end in second
 * param every count
 * param timeUnit can be Calendar.SECOND|HOUR|DAY|WEEK|SEMI_MONTH|MONTH|YEAR
 * return TemporalValue{Key(group start time), Value(group id)}
 * <p>
 * <p>
 * Created by song on 2018-05-10.
 */
public class TimeGroupMap
{
    private static Set<Integer> allowedTimeUnit =
            new HashSet<>( Arrays.asList( Calendar.YEAR, Calendar.MONTH, Calendar.DATE, Calendar.HOUR, Calendar.MINUTE, Calendar.SECOND ) );

    private final TimePointL timeStart;
    private final TimePointL timeEnd;
    private final int tEvery;
    private final int timeUnit;

    public TimeGroupMap(TimePointL timeStart, TimePointL timeEnd, int tEvery, int timeUnit )
    {
        if ( !allowedTimeUnit.contains( timeUnit ) )
        { throw new TPSRuntimeException( "invalid timeUnit!" ); }

        this.timeStart = timeStart;
        this.timeEnd = timeEnd;
        this.tEvery = tEvery;
        this.timeUnit = timeUnit;
    }

    public NavigableSet<TimePointL> calcNewGroup(TimePointL min, TimePointL max )
    {
        TimeInterval common = TimeIntervalUtil.overlapInterval(min, max, timeStart, timeEnd);
        TreeSet<TimePointL> set = new TreeSet<>();
        Iterators.addAll( set, new TimeGroupIterator( common.start(), common.end(), true ) );
        return set;
    }

    public NavigableSet<TimePointL> groupAvailable( TimePointL min, TimePointL max )
    {
        TimeInterval common = TimeIntervalUtil.overlapInterval(min, max, timeStart, timeEnd);
        TreeSet<TimePointL> set = new TreeSet<>();
        Iterators.addAll( set, new TimeGroupIterator( min, max, false ) );
        return set;
    }

    private class TimeGroupIterator extends AbstractIterator<TimePointL> implements PeekingIterator<TimePointL>
    {
        private final TimePointL max;
        private final Calendar intervalStart;
        private final boolean isCreation;
        private boolean addFirst;
        private boolean reachEnd = false;
        private TimePointL min;

        public TimeGroupIterator(TimePointL min, TimePointL max, boolean isCreation )
        {
            assert timeStart.compareTo(min) <= 0 && max.compareTo(timeEnd)<=0;
            this.min = min;
            this.max = max;
            this.isCreation = isCreation;
            Calendar start = Calendar.getInstance();
            start.setTimeInMillis( timeStart * 1000 );
            Calendar truncated = DateUtils.truncate( start, timeUnit );
            if ( truncated.getTimeInMillis() < start.getTimeInMillis() )
            {
                addFirst = (min * 1000 == start.getTimeInMillis());
                this.intervalStart = DateUtils.ceiling( start, timeUnit );// be careful with `daylight saving time`, check doc.
            }
            else
            {
                this.intervalStart = start;
            }
            this.seek( min );
        }

        private void seek( int min )
        {
            while ( intervalStart.getTimeInMillis() < min * 1000 )
            {
                intervalStart.add( timeUnit, tEvery );
            }
            if ( isCreation && intervalStart.getTimeInMillis() > min * 1000 ){
                addFirst = true;
            }
        }

        @Override
        protected TimePointL computeNext()
        {
            if ( reachEnd )
            { return endOfData(); }

            if ( addFirst )
            {
                addFirst = false;
                return this.min;
            }
            else if ( intervalStart.getTimeInMillis() < this.max * 1000 )
            {
                int time = Math.toIntExact( intervalStart.getTimeInMillis() / 1000 );
                intervalStart.add( timeUnit, tEvery );
                return time;
            }
            else
            {
                reachEnd = true;
                return this.max + 1;
            }
        }
    }
}

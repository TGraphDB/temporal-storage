package org.act.temporalProperty.helper;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.exception.TPSNHException;
import org.act.temporalProperty.impl.InternalEntry;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.IntervalIterator;
import org.act.temporalProperty.impl.SearchableIterator;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.query.TimeIntervalKey;
import org.act.temporalProperty.util.Slice;

import java.util.Map.Entry;

/**
 * Created by song on 2018-03-28.
 */
public class DebugIterator extends AbstractSearchableIterator
{

    private final SearchableIterator in;
    private final String name;
    private InternalKey preKey = null;
    private InternalKey min = null;
    private InternalKey max = null;

    public DebugIterator( SearchableIterator in, String name )
    {
        this.in = in;
        this.name = name;
    }

    public DebugIterator( SearchableIterator in )
    {
        this.in = in;
        this.name = "";
    }

    @Override
    protected InternalEntry computeNext()
    {
        return check();
    }

    private InternalEntry noCheck()
    {
        return in.hasNext() ? in.next() : endOfData();
    }

    private InternalEntry check()
    {
        if(in.hasNext()){
            InternalEntry curEntry = in.next();
            assertKeyInc( curEntry );
            updateMinMax( curEntry );
            isKeyEqual( curEntry );
            return curEntry;
        }
        else
        {
            return endOfData();
        }
    }

    private void isKeyEqual( InternalEntry entry )
    {
        InternalKey curT = entry.getKey();
        if(curT.compareTo( new InternalKey( 0, 0, 349529, ValueType.VALUE ) )==0){
            System.out.println("reach pre correct key: "+curT);
        }
    }

    private void assertKeyInc( InternalEntry entry )
    {
        if ( preKey == null )
        {
            preKey = entry.getKey();
        }
        else
        {
            InternalKey curT = entry.getKey();
            if ( curT.compareTo( preKey ) <= 0 )
            {
                throw new TPSNHException( "key dec! " + preKey + " " + curT );
            }
            else
            {
                preKey = curT;
            }
        }
    }

    private void updateMinMax( InternalEntry entry )
    {
        if ( min == null || entry.getKey().compareTo( min ) < 0 )
        { min = entry.getKey(); }
        if ( max == null || entry.getKey().compareTo( max ) > 0 )
        { max = entry.getKey(); }
    }

    @Override
    public void seekToFirst() {
        super.resetState();
        in.seekToFirst();
    }

    @Override
    public boolean seekFloor(InternalKey targetKey )
    {
        super.resetState();
        return in.seekFloor(targetKey);
    }

    @Override
    public String toString() {
        return "DebugIterator{" +
                "in=" + in +
                ", preKey=" + preKey +
                ", min=" + min +
                ", max=" + max +
                '}';
    }

    public static class ForInterval extends AbstractIterator<Entry<TimeIntervalKey,Slice>> implements IntervalIterator
    {
        private TimeIntervalKey preKey;
        private IntervalIterator in;

        public ForInterval( IntervalIterator it )
        {
            this.in = it;
        }

        @Override
        protected Entry<TimeIntervalKey,Slice> computeNext()
        {
            if(in.hasNext()){
                if(preKey == null){
                    preKey = in.peek().getKey();
                    return in.next();
                }else{
                    TimeIntervalKey curT = in.peek().getKey();
                    if(curT.getStartKey().compareTo( preKey.getStartKey() )>=0){
                        throw new TPSNHException("key not inc! "+ preKey +" "+ curT);
                    }else{
                        preKey = curT;
                        return in.next();
                    }
                }
            }else{
                return endOfData();
            }
        }
    }
}

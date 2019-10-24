package org.act.temporalProperty.helper;

import com.google.common.collect.AbstractIterator;
import org.act.temporalProperty.exception.TPSNHException;
import org.act.temporalProperty.impl.InternalEntry;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.IntervalIterator;
import org.act.temporalProperty.impl.SearchableIterator;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.query.TimeIntervalKey;
import org.act.temporalProperty.query.TimePointL;
import org.act.temporalProperty.util.Slice;

import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

/**
 * Created by song on 2018-03-28.
 */
public class DebugIterator extends AbstractSearchableIterator
{
    private final SearchableIterator in;
    private final LinkedList<InternalKey> preKeys = new LinkedList<>();
    private InternalKey preKey = null;
    private InternalKey min = null;
    private InternalKey max = null;

    public DebugIterator( SearchableIterator in )
    {
        this.in = in;
    }

    @Override
    protected InternalEntry computeNext()
    {
        if(in.hasNext()){
            return check(in.next());
        }else{
            return endOfData();
        }
    }

    private InternalEntry check(InternalEntry curEntry)
    {
        preKeys.add(curEntry.getKey());
        if(preKeys.size()>4) preKeys.poll();
        assertKeyInc( curEntry );
        assertKeyKnown(curEntry.getKey());
        updateMinMax( curEntry );
        isKeyEqual( curEntry );
        return curEntry;
    }

    // 保证key不是连续unknown状态
    private void assertKeyKnown(InternalKey key) {
//        if(preKeys.pollLast().getValueType()==ValueType.UNKNOWN){
//
//        }
    }

    private void isKeyEqual( InternalEntry entry )
    {
        InternalKey curT = entry.getKey();
        if(curT.compareTo( new InternalKey( 3, 120048, new TimePointL(1272652202), ValueType.VALUE ) )==0){
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
                System.out.println("########### ERROR@"+this.hashCode()+" ##########");
                preKeys.forEach(System.out::println);
                System.out.println( "key not inc! " + preKey + " " + curT );
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
        String inner = in.toString().replace("\n", "").replace("|--", "").replace("{}", "");
        StringBuilder sb = new StringBuilder();
        int level = 1;
        boolean skipBlank = false;
        for(int i=0; i<inner.length(); i++){
            char c = inner.charAt(i);
            switch (c) {
                case '{':
                    sb.append(c).append('\n');
                    for(int j=0; j<level; j++){
                        sb.append("|--");
                    }
                    level++;
                    break;
                case '}':
                    level--;
                    sb.append(c);
                    break;
                case ',':
                    skipBlank = true;
                    sb.append(c);
                    break;
                case ' ':
                    if(skipBlank){
                       continue;
                    }else{
                        sb.append(c);
                        break;
                    }
                default:
                    if(skipBlank){
                        skipBlank = false;
                        sb.append('\n').append(c);
                    }else{
                        sb.append(c);
                    }
            }
        }
        return "Debug@"+hashCode()+"_"+sb.toString();
    }
}

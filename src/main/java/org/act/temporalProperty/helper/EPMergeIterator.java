package org.act.temporalProperty.helper;

import org.act.temporalProperty.exception.TPSNHException;
import org.act.temporalProperty.impl.InternalEntry;
import org.act.temporalProperty.impl.SearchableIterator;
import org.act.temporalProperty.table.TwoLevelMergeIterator;
import org.act.temporalProperty.util.Slice;

/**
 * Created by song on 2018-01-24.
 * EP开头相关的Iterator用于把其他iterator的数据流中过滤出（仅保留）指定的entityID和PropertyID的数据
 */
public class EPMergeIterator extends TwoLevelMergeIterator
{
    private final Slice id;

    public EPMergeIterator(Slice idSlice, SearchableIterator old, SearchableIterator latest) {
        super( isEP( latest ) ? latest : new EPEntryIterator( idSlice, latest ), isEP( old ) ? old : new EPEntryIterator( idSlice, old ) );
        this.id = idSlice;
    }

    private static boolean isEP( SearchableIterator iterator )
    {
        return  (iterator instanceof EPEntryIterator) ||
                (iterator instanceof EPAppendIterator) ||
                (iterator instanceof EPMergeIterator);
    }

    @Override
    protected InternalEntry computeNext() {
        InternalEntry entry = super.computeNext();
        if ( entry != null )
        {
            if ( entry.getKey().getId().equals( id ) )
            {
                return entry;
            }
            else
            {
                throw new TPSNHException( "id not equal!" );
            }
        }
        else
        { return endOfData(); }
    }

    @Override
    public String toString() {
        return "EPMergeIterator{" +
                "id=" + id +
                '}';
    }
}

package org.act.temporalProperty.impl;

import org.act.temporalProperty.helper.AbstractSearchableIterator;

/**
 * Created by song on 2018-05-09.
 * 把UNKNOWN的entry转换为Invalid的entry
 */
public class UnknownToInvalidIterator extends AbstractSearchableIterator
{

    private final SearchableIterator in;

    public UnknownToInvalidIterator( SearchableIterator in ) {this.in = in;}

    @Override
    protected InternalEntry computeNext()
    {
        if ( in.hasNext() )
        {
            InternalEntry entry = in.next();
            InternalKey key = entry.getKey();
            if ( key.getValueType() == ValueType.UNKNOWN )
            {
                return new InternalEntry( new InternalKey( key.getId(), key.getStartTime(), ValueType.INVALID ), entry.getValue() );
            }
            else
            {
                return entry;
            }
        }
        else
        { return endOfData(); }
    }

    @Override
    public void seekToFirst()
    {
        super.resetState();
        in.seekToFirst();
    }

    @Override
    public void seek( InternalKey targetKey )
    {
        super.resetState();
        in.seek( targetKey );
    }
}

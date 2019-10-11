package org.act.temporalProperty.helper;

import org.act.temporalProperty.exception.TPSRuntimeException;
import org.act.temporalProperty.impl.InternalEntry;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SearchableIterator;
import org.act.temporalProperty.util.Slice;

/**
 * Created by song on 2018-01-24.
 */
public class EPEntryIterator extends AbstractSearchableIterator {

    private final SearchableIterator iter;
    private final Slice id;

    public EPEntryIterator(Slice entityPropertyId, SearchableIterator iterator){
        this.id = entityPropertyId;
        this.iter = iterator;
        this.seekToFirst();
    }

    @Override
    public void seekToFirst() {
        InternalKey earliestKey = new InternalKey(id, 0);
        iter.seekFloor(earliestKey);
        super.resetState();
    }

    @Override
    public boolean seekFloor(InternalKey targetKey )
    {
        if(targetKey.getId().equals( id ))
        {
            this.resetState();
            if(iter.seekFloor( targetKey ) && this.hasNext()){
                return (this.peek().getKey().compareTo(targetKey) <= 0);
            }else{
                return false;
            }
        }else{
            throw new TPSRuntimeException( "id not match!" );
        }
    }

    private boolean validId(InternalEntry entry) {
        return entry.getKey().getId().equals(id);
    }

    @Override
    protected InternalEntry computeNext() {
        while(iter.hasNext()){
            InternalEntry entry = iter.next();
            if(validId(entry)){
                return entry;
            }
        }
        return endOfData();
    }

    @Override
    public String toString() {
        return "EPEntryIterator{" +
                "iter=" + iter +
                ", id=" + id +
                '}';
    }
}

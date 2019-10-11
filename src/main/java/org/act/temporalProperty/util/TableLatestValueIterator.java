package org.act.temporalProperty.util;

import org.act.temporalProperty.helper.AbstractSearchableIterator;
import org.act.temporalProperty.impl.InternalEntry;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SearchableIterator;

import java.util.NoSuchElementException;

/**
 * 在生成新的StableFile的时候，需要把上一个tableFile的每个动态属性的最新的值加入到新的文件中，这个类就是提取StableFile中每个动态属性的最近值的工具
 */
public class TableLatestValueIterator implements SearchableIterator
{
    
    private SearchableIterator iterator;
    private InternalEntry next = null;
    private InternalEntry next_next = null; 
    
    public TableLatestValueIterator(SearchableIterator iterator)
    {
        this.iterator = iterator;
    }

    @Override
    public InternalEntry peek()
    {
        if( hasNext() )
            return next;
        else
            throw new NoSuchElementException();
    }

    @Override
    public InternalEntry next()
    {
        if( hasNext() )
        {
            InternalEntry toret = this.next;
            this.next = null;
            return toret;
        }
        else
            throw new NoSuchElementException();
    }

    private void getnext()
    {
        if( this.next_next == null )
        {
            try
            {
                this.next = this.iterator.next();
                this.next_next = this.iterator.next();
                while( this.next.getKey().getId().equals( this.next_next.getKey().getId() ) )
                {
                    this.next = this.next_next;
                    this.next_next = this.iterator.next();
                }
            }
            catch( NoSuchElementException e )
            {}
        }
        else
        {
            this.next = this.next_next;
            try
            {
                this.next_next = this.iterator.next();
                while( this.next.getKey().getId().equals( this.next_next.getKey().getId() ) )
                {
                    this.next = this.next_next;
                    this.next_next = this.iterator.next();
                }
            }
            catch( NoSuchElementException e )
            {
                this.next_next = null;
            }
        }
    }
    
    @Override
    public void remove()
    {
        next();
    }

    @Override
    public boolean hasNext()
    {
        if( null != next )
            return true;
        else
        {
            getnext();
            return (null != next);
        }
            
    }

    @Override
    public void seekToFirst()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seek( InternalKey targetKey )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "TableLatestValueIterator{" +
                "iterator=" + iterator +
                ", next=" + next +
                ", next_next=" + next_next +
                '}';
    }

    /**
     * By Sjh 2018
     */
    private static class ChangeTimeIterator extends AbstractSearchableIterator{
        private final SearchableIterator input;
        private final int startTime;

        /**
         * update every entry key's startTime to `startTime`.
         * @param input
         * @param startTime
         */
        ChangeTimeIterator(SearchableIterator input, int startTime){
            this.input = input;
            this.startTime = startTime;
        }

        @Override
        protected InternalEntry computeNext() {
            if(input.hasNext()){
                InternalEntry entry = input.next();
                InternalKey key = entry.getKey();
                InternalKey newKey = new InternalKey(key.getId(), startTime, key.getValueType());
                return new InternalEntry(newKey, entry.getValue());
            }else{
                return endOfData();
            }
        }

        @Override
        public void seekToFirst() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seek(InternalKey targetKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return "ChangeTimeIterator{" +
                    "input=" + input +
                    ", startTime=" + startTime +
                    '}';
        }
    }

    public static SearchableIterator setNewStart(SearchableIterator input, int time){
        return new ChangeTimeIterator(new TableLatestValueIterator(input), time);
    }
}

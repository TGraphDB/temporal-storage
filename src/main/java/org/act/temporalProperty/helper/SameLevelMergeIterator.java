package org.act.temporalProperty.helper;

import org.act.temporalProperty.exception.TPSNHException;
import org.act.temporalProperty.impl.InternalEntry;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SearchableIterator;

import java.util.*;

/**
 * Used in merge process.
 * Note that although disk files' time ranges have no overlap, but their key (proId, entityId, time) range can overlap.
 * So we just use a PriorityQueue to manage this.
 *
 * Created by song on 2018-03-28.
 */
public class SameLevelMergeIterator extends AbstractSearchableIterator
{
    private static Comparator<SearchableIterator> cp = ( o1, o2 ) ->
    {
        if(o1.hasNext() && o2.hasNext()){
            return o1.peek().getKey().compareTo(o2.peek().getKey());
        }else{
            throw new TPSNHException("iterators which ran out should not in heap!");
        }
    };
    private PriorityQueue<SearchableIterator> heap = new PriorityQueue<>(cp);
    private Set<SearchableIterator> iterators = new HashSet<>();

    public SameLevelMergeIterator( List<SearchableIterator> iterators )
    {
        this.iterators.addAll(iterators);
        for(SearchableIterator iterator : iterators) add(iterator);
    }

    public SameLevelMergeIterator(){}

    public void add( SearchableIterator append )
    {
        this.iterators.add(append);
        if(append.hasNext()) heap.add(append);
    }

    @Override
    protected InternalEntry computeNext() {
        SearchableIterator iter = heap.poll();
        if(iter!=null){
            InternalEntry entry = iter.next();
            if (iter.hasNext()) {
                heap.add(iter);
            }
            return entry;
        }else{
            return endOfData();
        }
    }

    public int size()
    {
        return heap.size();
    }

    @Override
    public void seekToFirst() {
        super.resetState();
        heap = new PriorityQueue<>(cp);
        for(SearchableIterator i: iterators){
            i.seekToFirst();
            heap.add(i);
        }
    }

    @Override
    public boolean seekFloor(InternalKey targetKey) {
        super.resetState();
        heap = new PriorityQueue<>(cp);
        boolean flag = false;
        // if any one of the iterators seekFloor is true, then we can confirm there is one entry less or eq to targetKey.
        for(SearchableIterator i: iterators){
            if(i.seekFloor( targetKey )) flag = true;
            heap.add(i);
        }
        return flag;
    }

    @Override
    public String toString() {
        return "SameLevelMergeIterator@"+hashCode()+"{" +
                "heap=" + heap +
                ", iterators=" + iterators +
                '}';
    }
}

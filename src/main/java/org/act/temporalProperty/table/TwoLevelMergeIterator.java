package org.act.temporalProperty.table;

import org.act.temporalProperty.helper.AbstractSearchableIterator;
import org.act.temporalProperty.helper.EqualValFilterIterator;
import org.act.temporalProperty.helper.InvalidEntityFilterIterator;
import org.act.temporalProperty.impl.InternalEntry;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.PackInternalKeyIterator;
import org.act.temporalProperty.impl.SearchableIterator;
import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.util.Slice;

/**
 * 将相邻Level的数据（如某文件及其Buffer）合并，并组成统一的Iterator。
 * 注意：不会删除有delete标记的record
 * rewrite by sjh at 2018-3-27
 */
public class TwoLevelMergeIterator extends AbstractSearchableIterator
{
    private final SearchableIterator latest;
    private final SearchableIterator old;

    public TwoLevelMergeIterator(SearchableIterator latest, SearchableIterator old)
    {
//        this.latest = new DebugAssertTimeIncIterator(latest);
//        this.old = new DebugAssertTimeIncIterator(old);
        this.latest = latest;
        this.old = old;
    }

    @Override
    protected InternalEntry computeNext() {
        while (latest.hasNext() && old.hasNext()){
            InternalEntry mem = latest.peek();
            InternalEntry disk = old.peek();

            if (disk.getKey().compareTo(mem.getKey()) < 0){
                return old.next();
            } else {
                InternalKey memKey = mem.getKey();
                if(memKey.getValueType()==ValueType.UNKNOWN ){
                    latest.next();
                } else {
                    InternalEntry tmp = latest.next();
                    delOld(tmp.getKey());
                    return tmp;
                }
            }
        }
        if (latest.hasNext()){ // diskIter run out
            return latest.next();
        } else if (old.hasNext()){ // memIter run out
            return old.next();
        } else{ // both ran out
            return endOfData();
        }
    }

    private void delOld(InternalKey k) {
        if(latest.hasNext()){
            InternalKey until = latest.peek().getKey();
            while(old.hasNext()){
                InternalKey oldKey = old.peek().getKey();
                if(oldKey.getId().equals(k.getId()) && oldKey.compareTo(until)<0){
                    old.next();
                }else{
                    return;
                }
            }
        }else{
            while(old.hasNext()) {
                InternalKey oldKey = old.peek().getKey();
                if(oldKey.getId().equals(k.getId())) {
                    old.next();
                }else{
                    return;
                }
            }
        }
    }

    @Override
    public void seekToFirst()
    {
        super.resetState();
        this.latest.seekToFirst();
        this.old.seekToFirst();
    }

    @Override
    public void seek( InternalKey targetKey )
    {
        super.resetState();
        this.latest.seek( targetKey );
        this.old.seek( targetKey );
    }

    @Override
    public String toString() {
        return "TwoLevelMergeIterator{" +
                "latest=" + latest +
                ", old=" + old +
                '}';
    }

    public static TwoLevelMergeIterator merge(SearchableIterator latest, SearchableIterator old){
        return new TwoLevelMergeIterator(latest, old);
    }

    public static TwoLevelMergeIterator merge(SeekingIterator<Slice,Slice> latest, SeekingIterator<Slice,Slice> old){
        return new TwoLevelMergeIterator(
                new PackInternalKeyIterator(latest),
                new PackInternalKeyIterator(old));
    }

    public static TwoLevelMergeIterator merge(SeekingIterator<Slice,Slice> latest, SearchableIterator old){
        return new TwoLevelMergeIterator(
                new PackInternalKeyIterator(latest),
                old);
    }

    public static TwoLevelMergeIterator merge(SearchableIterator latest, SeekingIterator<Slice,Slice> old){
        return new TwoLevelMergeIterator(
                latest,
                new PackInternalKeyIterator(old));
    }

    public static SearchableIterator toDisk(SearchableIterator latest, SearchableIterator old){
        return new InvalidEntityFilterIterator(new EqualValFilterIterator(new TwoLevelMergeIterator(latest, old)));
    }
}

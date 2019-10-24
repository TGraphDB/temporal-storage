package org.act.temporalProperty.table;

import org.act.temporalProperty.helper.AbstractSearchableIterator;
import org.act.temporalProperty.helper.DebugIterator;
import org.act.temporalProperty.impl.InternalEntry;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SearchableIterator;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.query.TimePointL;
import org.act.temporalProperty.vo.EntityPropertyId;

/**
 * 将相邻Level的数据（如某文件及其Buffer）合并，并组成统一的Iterator。
 * 参考{@code TwoLevelIntervalMergeIterator}
 * rewrite by sjh at 2019-10-24
 */
public class TwoLevelMergeIterator extends AbstractSearchableIterator
{
    private final SearchableIterator latest;
    private final SearchableIterator old;
    private InternalEntry oldCurrent;

    public TwoLevelMergeIterator(SearchableIterator latest, SearchableIterator old)
    {
        this.latest = latest;
        this.old = old;
    }

    @Override
    protected InternalEntry computeNext() {
        if (latest.hasNext() && old.hasNext()){
            InternalEntry mem = latest.peek();
            InternalEntry disk = old.peek();
            InternalKey memKey = mem.getKey();
            InternalKey diskKey = disk.getKey();
            

            int r = diskKey.compareTo(memKey);
            if(r<0){
                oldCurrent = disk;
                old.next();
                return disk;
            }else if(r==0){ // disk==mem
                if(memKey.getValueType()==ValueType.UNKNOWN ){
                    oldCurrent = disk;
                    latest.next();
                    old.next();
                    return disk;
                } else {
                    oldCurrent = disk;
                    latest.next();//==mem
                    old.next();
                    return mem;
                }
            }else{ // disk > mem > oldCurrent.getKey()
                if(memKey.getValueType()==ValueType.UNKNOWN ){
                    //无需delOld因为这个是unknown所以old里是需要被返回的，所以disk也不用next
                    if(oldCurrent!=null && oldCurrent.getKey().getId().equals(memKey.getId())){
                        InternalKey tmp = new InternalKey(memKey.getId(), memKey.getStartTime(), oldCurrent.getKey().getValueType());
                        oldCurrent = new InternalEntry(tmp, oldCurrent.getValue());
                        latest.next();//==mem
                        return oldCurrent;
                    }else{
                        oldCurrent = null;
                        latest.next();//==mem
                        return mem;
                    }
                } else {
                    oldCurrent = disk;
                    latest.next();//必须先调latest的next再delOld
                    delOld(memKey);
                    return mem;
                }
            }
        }else if (latest.hasNext()){ // diskIter run out
            InternalKey memKey = latest.peek().getKey();
            if(memKey.getValueType()==ValueType.UNKNOWN ){
                //此处逻辑同上
                if(oldCurrent!=null && oldCurrent.getKey().getId().equals(memKey.getId())){
                    InternalKey tmp = new InternalKey(memKey.getId(), memKey.getStartTime(), oldCurrent.getKey().getValueType());
                    oldCurrent = new InternalEntry(tmp, oldCurrent.getValue());
                    latest.next();
                    return oldCurrent;
                }else{
                    oldCurrent = null;
                    return latest.next();//==mem
                }
            } else {//oldCurrent不用管了，也不需要delOld因为old Run out了
                return latest.next();//==mem
            }
        } else if (old.hasNext()){ // memIter run out
            return old.next();
        } else{ // both ran out
            return endOfData();
        }
    }

    //从old中移除项，直到相同ID的项
    private void delOld(InternalKey k) {
        if(latest.hasNext()){
            InternalKey until = latest.peek().getKey();
            while(old.hasNext()){
                InternalKey oldKey = old.peek().getKey();
                if(oldKey.getId().equals(k.getId()) && oldKey.compareTo(until)<0){
                    oldCurrent = old.next();
                }else{
                    return;
                }
            }
        }else{
            while(old.hasNext()) {
                InternalKey oldKey = old.peek().getKey();
                if(oldKey.getId().equals(k.getId())) {
                    oldCurrent = old.next();
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
    public boolean seekFloor(InternalKey targetKey )
    {
        this.latest.seekFloor( targetKey );
        this.old.seekFloor( targetKey );
        return super.seekFloor(targetKey);
    }

    @Override
    public String toString() {
        return "TwoLevelMergeIterator@"+hashCode()+"{" +
                "latest=" + latest +
                ", old=" + old +
                '}';
    }

    public static SearchableIterator merge(SearchableIterator latest, SearchableIterator old){
        return new DebugIterator(new TwoLevelMergeIterator(latest, old));
    }
}

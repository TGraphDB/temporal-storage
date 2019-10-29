package org.act.temporalProperty.helper;

import org.act.temporalProperty.impl.InternalEntry;
import org.act.temporalProperty.impl.InternalKey;
import org.act.temporalProperty.impl.SearchableIterator;
import org.act.temporalProperty.impl.ValueType;
import org.act.temporalProperty.query.TimePointL;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

/**
 * Created by song on 2018-03-28.
 */
public class DebugIterator extends AbstractSearchableIterator
{
    private final SearchableIterator in;
    private final InternalKeyFixedLengthFifoQueue preKeys = new InternalKeyFixedLengthFifoQueue(5);
    private InternalKey min = null;
    private InternalKey max = null;

    private DebugIterator(SearchableIterator in)
    {
        this.in = in;
    }

    public static SearchableIterator wrap(SearchableIterator iterator) {
        return iterator;
//        if(iterator instanceof DebugIterator) return iterator;
//        else return new DebugIterator(iterator);
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
        assertKeyInc( curEntry );
        assertKeyKnown(curEntry.getKey());
        updateMinMax( curEntry );
        isKeyEqual( curEntry );
        preKeys.add(curEntry.getKey());
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
        try{
            InternalKey preKey = preKeys.getLast();
            InternalKey curT = entry.getKey();
            if ( curT.compareTo( preKey ) <= 0 )
            {
                System.out.println("########### ERROR@"+this.hashCode()+" ##########");
                System.out.println( "key not inc! " + internalKeyInline(preKey) + " " + internalKeyInline(curT) );
                System.out.println(this.toString());
            }
        }catch (NoSuchElementException ignore){}
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
        preKeys.clear();
        in.seekToFirst();
    }

    @Override
    public boolean seekFloor(InternalKey targetKey )
    {
        super.resetState();
        preKeys.clear();
        return in.seekFloor(targetKey);
    }

    private String internalKeyInline(InternalKey key){
        return key.toString().replace('{','(').replace('}',')').replace(", "," ");
    }

    @Override
    public String toString() {
        boolean printPreKeys = true;

        String inner = in.toString().replace("\n", "").replace("|--", "");
        if(!printPreKeys) {
            inner = inner.replace("{}", "");
        }
        StringBuilder sb = new StringBuilder();
        int level = 0;
        boolean skipBlank = false;

        for(int i=0; i<inner.length(); i++){
            char c = inner.charAt(i);
            switch (c) {
                case '{':
                case '[':
                    sb.append(c).append('\n');
                    level++;
                    if(c=='{' && printPreKeys){
                        printPreKeys=false;
                        for(int j=0; j<level; j++){ sb.append("|--"); }
                        sb.append("preKeys=[\n");
                        level++;
                        int k=1;
                        for(InternalKey key : preKeys){
                            for(int j=0; j<level; j++){ sb.append("|--"); }
                            sb.append(internalKeyInline(key));
                            if(k<preKeys.size()) sb.append(',').append('\n');
                            k++;
                        }
                        sb.append("],\n");
                        level--;
                    }
                    for(int j=0; j<level; j++){ sb.append("|--"); }
                    break;
                case ']':
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
                        sb.append('\n');
                        for(int j=0; j<level; j++){
                            sb.append("|--");
                        }
                        sb.append(c);
                    }else{
                        sb.append(c);
                    }
            }
        }
        return "Debug@"+hashCode()+"_"+sb.toString();
    }


    private static class InternalKeyFixedLengthFifoQueue implements Iterable<InternalKey>{
        private final LinkedList<InternalKey> pool = new LinkedList<>();
        private final int len;
        private int addCount = 0;

        InternalKeyFixedLengthFifoQueue(int len) {
            this.len = len;
        }

        void add(InternalKey newest) {
            pool.add(newest);
            addCount++;
            if(addCount>len){
                pool.poll();
            }
        }

        int size(){
            return Math.min(addCount, len);
        }

        InternalKey getLast(){
            return pool.getLast();
        }

        @Override
        public Iterator<InternalKey> iterator() {
            return pool.iterator();
        }

        public void clear() {
            pool.clear();
            addCount=0;
        }
    }
}

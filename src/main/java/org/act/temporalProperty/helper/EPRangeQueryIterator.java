package org.act.temporalProperty.helper;

import org.act.temporalProperty.impl.*;
import org.act.temporalProperty.query.TimePointL;
import org.act.temporalProperty.table.TwoLevelMergeIterator;
import org.act.temporalProperty.vo.EntityPropertyId;
import org.apache.commons.lang3.tuple.Triple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EPRangeQueryIterator implements SearchableIterator{

    private final EntityPropertyId id;
    private final TimePointL end;
    private final TimePointL start;
    private final MemTable[] memTables = new MemTable[]{null, null, null};
    private final List<Triple<String, SearchableIterator, SearchableIterator>> diskIterators = new ArrayList<>();

    private final Map<SearchableIterator, String> debugIterName = new HashMap<>();

    private SearchableIterator iterator;

    public EPRangeQueryIterator(EntityPropertyId id, TimePointL start, TimePointL end){
        this.id = id;
        this.start = start;
        this.end = end;
    }

    public void addTransactionMemTable(MemTable table){
        memTables[0] = table;
    }

    public void addMemTable(MemTable table){
        memTables[1] = table;
    }

    public void addStableMemTable(MemTable table){
        memTables[2] = table;
    }

    public void appendStables(SearchableIterator fileIterator, SearchableIterator bufferIter, FileMetaData meta){
        diskIterators.add(Triple.of("st."+meta.getNumber(), fileIterator, bufferIter));
    }

    public void appendStables(SearchableIterator fileIterator, FileMetaData meta) {
        diskIterators.add(Triple.of("st."+meta.getNumber(), fileIterator, null));
    }

    public void appendUnStables(SearchableIterator fileIterator, SearchableIterator bufferIter, FileMetaData meta){
        diskIterators.add(Triple.of("un."+meta.getNumber(), fileIterator, bufferIter));
    }

    public void appendUnStables(SearchableIterator fileIterator, FileMetaData meta) {
        diskIterators.add(Triple.of("un."+meta.getNumber(), fileIterator, null));
    }

    public void build(){
        SearchableIterator memIter = ep(memTables[1].iterator(), "memTable");
        if(memTables[2]!=null) {
            memIter = epMerge(ep(memTables[2].iterator(), "stableMemTable"), memIter, "mem/stm");
        }
        if(memTables[0]!=null){
            memIter = epMerge(memIter, ep(memTables[0].iterator(), "txMemTable"), "tx/");
        }
        EPAppendIterator diskIter = new EPAppendIterator(id);
        for(Triple<String, SearchableIterator, SearchableIterator> p : diskIterators){
            SearchableIterator fileIter = ep(p.getMiddle(), p.getLeft());
            if(p.getRight()==null){
                diskIter.append(fileIter);
            }else{
                SearchableIterator bufferIter = ep(p.getRight(), p.getLeft()+".buf");
                diskIter.append(epMerge(fileIter, bufferIter, "f.b"));
            }
        }
        iterator = debug(new UnknownToInvalidIterator( epMerge(memIter, debug(diskIter, "disk"), "mem/disk") ), "unk2invalid");
    }

    private SearchableIterator ep(SearchableIterator in, String name){
        EPEntryIterator i = new EPEntryIterator(id, debug(in, name));
        return debug(i, "epEntry");
    }

    private SearchableIterator epMerge(SearchableIterator old, SearchableIterator latest, String name){
        TwoLevelMergeIterator i = new TwoLevelMergeIterator(latest, old);
//        EPMergeIterator i = new EPMergeIterator(id, old, latest);
        return debug(i, "merge("+name+")");
    }

    private SearchableIterator debug(SearchableIterator iterator, String name){
        return iterator;
//        debugIterName.put(iterator, name);
//        return new DebugIterator(iterator);
    }

//    public String iteratorTree(){
//
//    }

    public String path(InternalEntry entry){
        StringBuilder sb = new StringBuilder();
        for(SearchableIterator i : entry.path){
            sb.append(debugIterName.get(i)).append(" > ");
        }
        return sb+"(END)";
    }

    @Override
    public void seekToFirst() {
        checkBuilt();
        iterator.seekToFirst();
    }

    private void checkBuilt() {
        assert iterator!=null : "should call build() method first.";
    }

    @Override
    public boolean seekFloor(InternalKey targetKey) {
        checkBuilt();
        return iterator.seekFloor(targetKey);
    }

    @Override
    public InternalEntry peek() {
        checkBuilt();
        return iterator.peek();
    }

    @Override
    public boolean hasNext() {
        checkBuilt();
        return iterator.hasNext();
    }

    @Override
    public InternalEntry next() {
        checkBuilt();
        return iterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }


    static class DebugIterator implements SearchableIterator{

        private final SearchableIterator in;

        DebugIterator(SearchableIterator in){
            this.in = in;
        }

        @Override
        public InternalEntry peek() {
            return addInPath(in.peek());
        }

        private InternalEntry addInPath(InternalEntry entry) {
            entry.path.add(in);
            return entry;
        }

        @Override
        public boolean hasNext() {
            return in.hasNext();
        }

        @Override
        public InternalEntry next() {
            return addInPath(in.next());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seekToFirst() {
            in.seekToFirst();
        }

        @Override
        public boolean seekFloor(InternalKey targetKey) {
            return in.seekFloor(targetKey);
        }
    }
}

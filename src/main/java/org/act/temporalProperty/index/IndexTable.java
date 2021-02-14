package org.act.temporalProperty.index;

import org.act.temporalProperty.impl.SeekingIterator;
import org.act.temporalProperty.index.value.IndexQueryRegion;
import org.act.temporalProperty.index.value.IndexTableIterator;
import org.act.temporalProperty.index.value.PropertyValueInterval;
import org.act.temporalProperty.index.value.cardinality.HyperLogLog;
import org.act.temporalProperty.index.value.cardinality.RTreeCardinality;
import org.act.temporalProperty.index.value.rtree.IndexEntry;
import org.act.temporalProperty.index.value.rtree.IndexEntryOperator;
import org.act.temporalProperty.query.aggr.AggregationIndexKey;
import org.act.temporalProperty.table.MMapTable;
import org.act.temporalProperty.util.Slice;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by song on 2018-01-19.
 */
public class IndexTable {

    private final FileChannel channel;

    public IndexTable(FileChannel fileChannel) {
        this.channel = fileChannel;
    }

    public Iterator<IndexEntry> iterator(IndexQueryRegion regions) throws IOException {
        return new IndexTableIterator(this.channel, regions, extractOperator(regions));
    }

    private IndexEntryOperator extractOperator(IndexQueryRegion regions) {
        List<IndexValueType> types = new ArrayList<>();
        for(PropertyValueInterval p : regions.getPropertyValueIntervals()){
            types.add(p.getType());
        }
        return new IndexEntryOperator(types, 4096);
    }

    public SeekingIterator<Slice, Slice> aggrIterator(String filePath) throws IOException {
        return new MMapTable( filePath, channel, AggregationIndexKey.sliceComparator, false).iterator();
    }

    public HyperLogLog cardinalityEstimator(IndexQueryRegion regions) throws IOException {
        return new RTreeCardinality(this.channel, regions, extractOperator(regions)).cardinalityEstimator();
    }
}

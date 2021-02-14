package org.act.temporalProperty.index.value.cardinality;

import org.act.temporalProperty.index.value.IndexQueryRegion;
import org.act.temporalProperty.index.value.rtree.*;
import org.act.temporalProperty.util.DynamicSliceOutput;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.SliceInput;
import org.act.temporalProperty.util.SliceOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class RTreeCardinality {
    RTreeNode root;
    List<RTreeNode> firstLevel;
    // constructor used for write
    public RTreeCardinality(RTree tree) {
        this.root = tree.getRoot();
        this.firstLevel = tree.getLevels().get(tree.getLevels().size()-1);
    }

    public ByteBuffer encode() {
        DynamicSliceOutput out = new DynamicSliceOutput(4096);
        out.writeInt(0);//placeholder for size.
        root.getBound().encode(out);
        root.getCardinalityEstimator().encode(out);
        System.out.println(root.getCardinality());
        int childCnt = this.firstLevel.size();
        out.writeInt(childCnt);
        for(RTreeNode node : firstLevel){
            node.getBound().encode(out);
//            System.out.println(out.size());
            node.getCardinalityEstimator().encode(out);
            System.out.println(node.getCardinality());
        }
        Slice content = out.slice();
        content.setInt(0, content.length()-4);
        System.out.println(content.length());
        return content.toByteBuffer();
    }


    private RTreeRange queryRegion;
    private IndexEntryOperator op;
    // constructor used for read
    public RTreeCardinality(FileChannel channel, IndexQueryRegion regions, IndexEntryOperator op) throws IOException {
        this.op = op;
        this.queryRegion = op.toRTreeRange(regions);

        MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        map.order(ByteOrder.LITTLE_ENDIAN);
        int rootPos = map.getInt();
        map.position(rootPos);
        new RTreeNodeBlock(map, op);

        int len = map.getInt();
        byte[] content = new byte[len];
        map.get(content);
        SliceInput in = new Slice(content).input();

        this.root = new RTreeCardinalityNodeBlock(in);
        this.firstLevel = new ArrayList<>();
        int size = in.readInt();
        for(int i=0; i<size; i++){
            firstLevel.add(new RTreeCardinalityNodeBlock(in));
        }
    }

    public HyperLogLog cardinalityEstimator() {
        HyperLogLog result = HyperLogLog.defaultBuilder();
        for(RTreeNode node : firstLevel){
            if(node.getBound().overlap(queryRegion)) {
                result.addAll(node.getCardinalityEstimator());
            }
        }
        return result;
    }


    private class RTreeCardinalityNodeBlock extends RTreeNode{

        public RTreeCardinalityNodeBlock(SliceInput in) {
            this.setBound(RTreeRange.decode(in, op));
            this.setCardinalityEstimator(HyperLogLog.decode(in));
        }

        private void setCardinalityEstimator(HyperLogLog decode) {
            this.c = decode;
        }

        @Override
        public boolean isLeaf() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void encode(SliceOutput out) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<RTreeNode> getChildren() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<IndexEntry> getEntries() {
            throw new UnsupportedOperationException();
        }
    }
}

package org.act.temporalProperty.index.rtree;

import com.google.common.base.Preconditions;
import org.act.temporalProperty.util.Slice;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created by song on 2018-01-19.
 *
 * Sort-Tail-Recursive Tree implementation
 */
public class RTree {

    private final IndexEntryOperator op;
    private final RTreeNode root;
    private final int bData;
    private final int bIndex;
    private final List<List<RTreeNode>> levels = new ArrayList<>();

    public RTree(List<Slice> data, IndexEntryOperator op){
        this.op = op;
        this.bData = op.dataBlockCapacity();
        this.bIndex = op.indexBlockCapacity();
        this.root = packNode(packData(data));
    }

    public RTreeNode getRoot(){
        return root;
    }

    public List<List<RTreeNode>> getLevels() {
        return levels;
    }

    private List<RTreeNode> packData(List<Slice> data){
        this.sortData(data);
        List<RTreeNode> dataLevel = new ArrayList<>();
        for(int i = 0; i<data.size(); i+= bData){
            int end = (i+bData>data.size())? data.size() : i+bData;
            RTreeNode node = new RTreeLeafNode(data.subList(i, end), this.op);
            dataLevel.add(node);
        }
        this.levels.add(dataLevel);
        return dataLevel;
    }

    private RTreeNode packNode(List<RTreeNode> nodes){
        this.sortNodes(nodes);
        List<RTreeNode> upperLevelNodes = new ArrayList<>();
        for(int i = 0; i<=nodes.size(); i+= bIndex) {
            int end = (i+bIndex>nodes.size())? nodes.size() : i+bIndex;
            RTreeNode node = new RTreeIndexNode(nodes.subList(i, end), this.op);
            upperLevelNodes.add(node);
        }
//        RTree.log.info("one level packed, ({}) nodes", upperLevelNodes.size());
        if(upperLevelNodes.size()>1){
            this.levels.add(upperLevelNodes);
            return packNode(upperLevelNodes);
        }else if(upperLevelNodes.size()==1){
            return upperLevelNodes.get(0);
        }else{
            throw new RuntimeException("should not happen");
        }
    }


    //------ utils for node(RTreeNode)---------
    private void sortNodes(List<RTreeNode> data) {
        nodeRecursiveSort(data, 0, data.size(), 0, this.op.dimensionCount());
    }

    private void nodeRecursiveSort(List<RTreeNode> data, int left, int right, int corIndex, int k) {
        data.subList(left, right).sort(nodeComparator(this.op, corIndex));
        if(k>1) {
            int r = right - left;
            int p = r / bIndex + (r % bIndex == 0 ? 0 : 1);
            int s = (int) Math.round(Math.ceil(Math.pow(p, 1d / k)));
            int groupLen = s * bIndex;
            for (int i = 0; i < s; i++) {
                int start = i * groupLen;
                int end = i * (groupLen + 1) > right ? right : i * (groupLen + 1);
                nodeRecursiveSort(data, start, end, corIndex + 1, k - 1);
            }
        }
    }

    private Comparator<RTreeNode> nodeComparator(IndexEntryOperator op, int dimIndex) {
        Preconditions.checkArgument(dimIndex<op.dimensionCount());
        return (o1, o2) -> op.compareRange(o1.getBound(), o2.getBound(), dimIndex);
    }

    //------ utils for data(Slice)---------
    private void sortData(List<Slice> data) {
        dataRecursiveSort(data, 0, data.size(), 0, this.op.dimensionCount());
    }

    private void dataRecursiveSort(List<Slice> data, int left, int right, int corIndex, int k) {
        data.subList(left, right).sort(sliceComparator(this.op, corIndex));
        if(k>1) {
            int r = right - left;
            int p = r / bData + (r % bData == 0 ? 0 : 1);
            int s = (int) Math.round(Math.ceil(Math.pow(p, 1d / k)));
            int groupLen = s * bData;
            for (int i = 0; i < s; i++) {
                int start = i * groupLen;
                int end = i * (groupLen + 1) > right ? right : i * (groupLen + 1);
                dataRecursiveSort(data, start, end, corIndex + 1, k - 1);
            }
        }
    }

    private Comparator<Slice> sliceComparator(IndexEntryOperator op, int dimIndex) {
        Preconditions.checkArgument(dimIndex<op.dimensionCount());
        return (o1, o2) -> op.compare(o1, o2, dimIndex);
    }

}
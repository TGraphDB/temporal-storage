package org.act.temporalProperty;

import org.act.temporalProperty.impl.RangeQueryCallBack;
import org.act.temporalProperty.index.IndexQueryRegion;
import org.act.temporalProperty.index.IndexValueType;
import org.act.temporalProperty.index.rtree.IndexEntry;
import org.act.temporalProperty.util.Slice;

import java.util.List;

/**
 * 动态属性存储系统，对外提供其功能的接口
 *
 */
public interface TemporalPropertyStore
{
	/**
	 * 对某个动态属性进行时间点查询，返回查询的 结果
	 * @param id 动态属性所属的点/边的id
	 * @param proId 动态属性id
	 * @param time 需要查询的时间
	 * @return @{Slice} 查询的结果
	 */
    public Slice getPointValue( long id, int proId, int time );
    
    /**
	 * 对某个动态属性进行时间段查询，返回查询的 结果
	 * @param id 动态属性所属的点/边的id
	 * @param proId 动态属性id
	 * @param startTime 需要查询的时间的起始时间
	 * @param endTime 需要查询的时间的结束时间
	 * @param callback 时间段查询所采用的聚集类型
	 * @return @{Slice} 查询的结果
	 */
    public Object getRangeValue( long id, int proId, int startTime, int endTime, RangeQueryCallBack callback );
    
    /**
     * 写入某个动态属性的值
     * @param id 动态属性所属的点/边的id
     * @param proid 动态属性id
     * @param time 相应值有效的起始时间
     * @param value 值
     * @return 是否写入成功
     */
    public boolean setProperty( Slice id, byte[] value );
    
    /**
     * 删除某个动态属性在某个时间的值
     * @param id 动态属性所属的点/边的id
     * @param proid 动态属性id
     * @param time 需要删除的值的起始有效时间
     * @return 是否删除成功
     */
    public boolean delete(Slice id);

	void createValueIndex(int start, int end, List<Integer> proIds, List<IndexValueType> types);

	/**
	 * get entity id which satisfy query condition
	 * @param condition query condition of one property
	 * @return null if no index available;
	 */
	List<Long> getEntities(IndexQueryRegion condition);

	List<IndexEntry> getEntries(IndexQueryRegion condition);

	void flushMemTable2Disk();

    void flushMetaInfo2Disk();

    public void shutDown();
}

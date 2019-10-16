
package org.act.temporalProperty.impl;

import org.act.temporalProperty.util.*;

import com.google.common.base.Preconditions;
import org.act.temporalProperty.vo.EntityPropertyId;

import java.util.Objects;

import static org.act.temporalProperty.util.SizeOf.SIZE_OF_INT;
import static org.act.temporalProperty.util.SizeOf.SIZE_OF_LONG;

/**
 * InternalKey是将一个动态属性的点/边id，属性id，某个时间以及相关控制位编码为一个键值对中的键的机制。在设计文档中，我们将一个动态属性的数据分为多个record，而InternalKey就
 * 是一个record的key
 * 点/边id和属性id可联合确定一个时态值
 */
public class InternalKey implements Comparable<InternalKey>
{
    /**
     * 属性id
     */
    private final int propertyId;
    /**
     * 点/边id
     */
    private final long entityId;
    /**
     * 一个动态属性某个值的起始时间
     */
    private final int startTime;
    /**
     * 值类型：invalid or value or unknown
     */
    private final ValueType valueType;

    /**
     * 新建一个InternalKey，将相关信息传入，用于编码后生成一个Slice
     * @param Id
     * @param startTime
     * @param valueType
     */
    public InternalKey(EntityPropertyId Id, int startTime, ValueType valueType)
    {
        Preconditions.checkNotNull(Id);
        Preconditions.checkNotNull(valueType);
        Preconditions.checkArgument(startTime >= 0, "sequenceNumber is negative");

        this.propertyId = Id.getPropertyId();
        this.entityId = Id.getEntityId();
        this.startTime = startTime;
        this.valueType = valueType;
    }

    public InternalKey(int propertyId, long entityId, int startTime, ValueType valueType)
    {
        Preconditions.checkArgument(startTime >= 0, "sequenceNumber is negative");
        Preconditions.checkNotNull(valueType);

        this.propertyId = propertyId;
        this.entityId = entityId;
        this.startTime = startTime;
        this.valueType = valueType;
    }
    /**
     * 新建一个InternalKey，将相关信息传入，用于编码后生成一个Slice，通常用于查找
     * @param Id
     * @param startTime
     */
    public InternalKey(EntityPropertyId Id, int startTime)
    {
        this(Id, startTime, ValueType.VALUE);
    }

    /**
     * 新建一个InternalKey，将相关Slice传入，可用于解码相关信息。如起始时间，record类型，id等
     * @param data
     */
    public InternalKey(Slice data)
    {
        Preconditions.checkNotNull(data);
        Preconditions.checkArgument(data.length() >= SIZE_OF_LONG, "data must be at least %s bytes", SIZE_OF_LONG);
        this.propertyId = data.getInt(8);
        this.entityId = data.getLong(0);
        long packedSequenceAndType = data.getLong( data.length() - SIZE_OF_LONG );
        this.startTime = SequenceNumber.unpackTime(packedSequenceAndType);
        this.valueType = SequenceNumber.unpackValueType(packedSequenceAndType);
    }

    /**
     * 返回唯一确定某个动态属性的标识，其中其点/边id保存在返回值的前8位，属性id保存在返回值的后4位。
     * @return 唯一确定某个动态属性的标识
     */
    public EntityPropertyId getId()
    {
        return new EntityPropertyId(entityId, propertyId);
    }

    public int getPropertyId()
    {
        return propertyId;
    }

    public long getEntityId()
    {
        return entityId;
    }
    
    /**
     * @return 返回此key对应值的起始有效时间
     */
    public int getStartTime()
    {
        return startTime;
    }

    /**
     * @return 返回此key对应record的类型
     */
    public ValueType getValueType()
    {
        return valueType;
    }



    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InternalKey that = (InternalKey) o;
        return  entityId == that.entityId &&
                propertyId == that.propertyId &&
                startTime == that.startTime &&
                valueType == that.valueType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(propertyId, entityId, startTime, valueType);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("InternalKey");
        sb.append("{eid=").append(getEntityId());
        sb.append( " proId=" ).append( getPropertyId() );
        sb.append(", time=").append(getStartTime());
        sb.append(", valueType=").append(getValueType());
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int compareTo(InternalKey o) {
        int result = this.getId().compareTo(o.getId());
        if( 0 != result ) {
            return result;
        }else {
            return Long.compare(this.getStartTime(), o.getStartTime());
        }
    }

    public Slice encode()
    {
        DynamicSliceOutput out = new DynamicSliceOutput(SIZE_OF_INT + 2*SIZE_OF_LONG);
        getId().encode(out);
        out.writeLong(SequenceNumber.packTimeAndValueType( startTime, valueType ) );
        return out.slice();
    }

    public static InternalKey decode(SliceInput in)
    {
        EntityPropertyId id = EntityPropertyId.decode(in);
        long tmp = in.readLong();
        int time = SequenceNumber.unpackTime(tmp);
        ValueType valueType = SequenceNumber.unpackValueType(tmp);
        return new InternalKey(id.getPropertyId(), id.getEntityId(), time, valueType);
    }

    public static InternalKey decode(Slice in)
    {
        Preconditions.checkArgument(in.length() >= SIZE_OF_INT+2*SIZE_OF_LONG, "not a valid InternalKey slice, got len: %d", in.length());
        return decode(in.input());
    }
}

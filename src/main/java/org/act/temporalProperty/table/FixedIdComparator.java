package org.act.temporalProperty.table;

import org.act.temporalProperty.util.Slice;

import com.google.common.base.Preconditions;

/**
 * 对时态属性Id（Entity Id + Property Id）的Slice进行比较的工具类
 */
public class FixedIdComparator implements UserComparator
{

    @Override
    public int compare( Slice o1, Slice o2 )
    {
        Preconditions.checkArgument( o1.length() == 12, "FixedIdComparator only compare the fixed format of slice" );
        Preconditions.checkArgument( o2.length() == 12, "FixedIdComparator only compare the fixed format of slice" );
        int r = o1.getInt( 8 ) - o2.getInt( 8 );
        if ( r == 0 )
        {
            long rr = o1.getLong( 0 ) - o2.getLong( 0 );
            return (rr == 0) ? 0 : (rr > 0 ? 1 : -1);
        }
        else
        {
            return r;
        }
    }

    @Override
    public String name()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Slice findShortestSeparator( Slice start, Slice limit )
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Slice findShortSuccessor( Slice key )
    {
        // TODO Auto-generated method stub
        return null;
    }

    private static UserComparator self = new FixedIdComparator();

    public static int compareId( Slice o1, Slice o2 )
    {
        return self.compare( o1, o2 );
    }
}

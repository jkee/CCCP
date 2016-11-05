package ru.yandex.clickhouse.cccp.index;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLongs;

import java.util.Arrays;
import java.util.List;

/**
 * Index range for one region or other purposes
 *
 * Defined as list of unsigned long ranges.
 * For example:
 * Month index: number of month from unixepoch
 * UInt64 index: just this UInt64
 *
 * Can be a prefix so number of bounds can be lower than index range
 *
 * Created by Jkee on 29.10.2016.
 */
public class IndexRange {

    // including
    private final long[] upperBounds;

    // not including
    private final long[] lowerBounds;

    public IndexRange(long[] upperBounds, long[] lowerBounds) {
        Preconditions.checkArgument(upperBounds.length == lowerBounds.length, "Bounds length mismatch");
        for (int i = 0; i < upperBounds.length; i++) {
            Preconditions.checkArgument(UnsignedLongs.compare(upperBounds[i], lowerBounds[i]) < 0, "Upper bound should be higher than lower bound");
        }
        this.upperBounds = upperBounds;
        this.lowerBounds = lowerBounds;
    }

    public long[] getUpperBounds() {
        return upperBounds;
    }

    public long[] getLowerBounds() {
        return lowerBounds;
    }

    public List<Object> getUpperBoundsExternal(IndexConfig config) {
        return toExternal(config, lowerBounds);
    }

    public List<Object> getLowerBoundsExternal(IndexConfig config) {
        return toExternal(config, upperBounds);
    }

    private List<Object> toExternal(IndexConfig config, long[] lowerBounds) {
        List<Object> objects = Lists.newArrayListWithCapacity(lowerBounds.length);
        for (int i = 0; i < lowerBounds.length; i++) {
            IndexType<?> indexType = config.getTypes().get(i);
            Object o = indexType.fromBound(lowerBounds[i]);
            objects.add(o);
        }
        return objects;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexRange that = (IndexRange) o;

        if (!Arrays.equals(upperBounds, that.upperBounds)) return false;
        return Arrays.equals(lowerBounds, that.lowerBounds);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(upperBounds);
        result = 31 * result + Arrays.hashCode(lowerBounds);
        return result;
    }
}

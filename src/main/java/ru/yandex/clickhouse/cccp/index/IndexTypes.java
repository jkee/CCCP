package ru.yandex.clickhouse.cccp.index;

import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;

import java.time.LocalDate;
import java.util.List;

/**
 * Created by Jkee on 01.11.2016.
 */
public class IndexTypes {

    public static final IndexType<LocalDate> MONTH = new IndexType<LocalDate>() {
        @Override
        public Class<LocalDate> getClazz() {
            return LocalDate.class;
        }

        @Override
        public String getID() {
            return "month";
        }

        @Override
        public long toBound(LocalDate bound) {
            return (bound.getYear() * 12) + bound.getMonth().ordinal();
        }

        @Override
        public LocalDate fromBound(long bound) {
            int year = (int) bound / 12;
            int month = (int) bound % 12;
            return LocalDate.of(year, month + 1, 1);
        }

        @Override
        public long maxValue() {
            return toBound(LocalDate.of(2037, 1, 1));
        }

        @Override
        public long minValue() {
            return toBound(LocalDate.of(1970, 1, 1));
        }
    };

    public static final IndexType<Long> UInt64 = new IndexType<Long>() {
        @Override
        public Class<Long> getClazz() {
            return Long.class;
        }

        @Override
        public String getID() {
            return "uint64";
        }

        @Override
        public long toBound(Long bound) {
            return bound;
        }

        @Override
        public Long fromBound(long bound) {
            return bound;
        }

        @Override
        public long maxValue() {
            return UnsignedLong.MAX_VALUE.longValue();
        }

        @Override
        public long minValue() {
            return UnsignedLong.ZERO.longValue();
        }
    };

    public static final IndexType<Integer> UInt32 = new IndexType<Integer>() {
        @Override
        public Class<Integer> getClazz() {
            return Integer.class;
        }

        @Override
        public String getID() {
            return "uint32";
        }

        @Override
        public long toBound(Integer bound) {
            return bound;
        }

        @Override
        public Integer fromBound(long bound) {
            return UnsignedInteger.valueOf(bound).intValue();
        }

        @Override
        public long maxValue() {
            return UnsignedInteger.MAX_VALUE.longValue();
        }

        @Override
        public long minValue() {
            return UnsignedInteger.ZERO.longValue();
        }
    };

    private static final List<IndexType> types = Lists.newArrayList(
            MONTH,
            UInt64,
            UInt32
    );

    public static IndexType<?> fromID(String id) {
        for (IndexType type : types) {
            if (type.getID().equals(id)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Type not found: " + id);
    }

}

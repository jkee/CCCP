package ru.yandex.clickhouse.cccp.index;

import java.time.LocalDate;

/**
 * Created by Jkee on 01.11.2016.
 */
public class IndexTypes {

    public static final IndexType<LocalDate> MONTH = new IndexType<LocalDate>() {
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
    };

    public static final IndexType<Long> UInt64 = new IndexType<Long>() {
        @Override
        public String getID() {
            return "month";
        }

        @Override
        public long toBound(Long bound) {
            return bound;
        }

        @Override
        public Long fromBound(long bound) {
            return bound;
        }
    };


}

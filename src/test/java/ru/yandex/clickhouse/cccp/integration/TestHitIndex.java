package ru.yandex.clickhouse.cccp.integration;

import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedLongs;

import java.time.LocalDate;

/**
 * Created by jkee on 05/11/16.
 */
public class TestHitIndex implements Comparable<TestHitIndex> {

    private LocalDate eventMonth;
    private int indexPrefix;
    private long userIDHash;

    public TestHitIndex(LocalDate eventMonth, int indexPrefix, long userIDHash) {
        this.eventMonth = eventMonth;
        this.indexPrefix = indexPrefix;
        this.userIDHash = userIDHash;
    }

    public int getIndexPrefix() {
        return indexPrefix;
    }

    public LocalDate getEventMonth() {
        return eventMonth;
    }

    public long getUserIDHash() {
        return userIDHash;
    }

    @Override
    public int compareTo(TestHitIndex o) {
        int dateC = eventMonth.compareTo(o.eventMonth);
        if (dateC != 0) return dateC;
        int indexC = Ints.compare(indexPrefix, o.indexPrefix);
        if (indexC != 0) return indexC;
        return UnsignedLongs.compare(userIDHash, o.userIDHash);
    }
}

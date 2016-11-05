package ru.yandex.clickhouse.cccp.integration;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Created by jkee on 05/11/16.
 */
public class TestHit {

    private LocalDateTime eventTime;
    private int indexPrefix;
    private long userID;
    private String usefulData;

    public TestHit(LocalDateTime eventTime, int indexPrefix, long userID, String usefulData) {
        this.eventTime = eventTime;
        this.indexPrefix = indexPrefix;
        this.userID = userID;
        this.usefulData = usefulData;
    }

    public TestHitIndex toIndex() {
        int hash = intHash32(userID);
        return new TestHitIndex(
                LocalDate.from(eventTime).withDayOfMonth(1),
                indexPrefix,
                hash
        );
    }

    /*
    * https://github.com/yandex/ClickHouse/blob/1d836b2bf8fef378d47258957ff74ed3a4aff136/dbms/include/DB/Common/HashTable/Hash.h#L143
    * https://github.com/yandex/ClickHouse/blob/d1f11af2c2d2edaf69491bc3608e4bc52a862ca8/dbms/include/DB/Functions/FunctionsHashing.h#L147
    * */
    public static int intHash32(long key) {
        long salt = ( (long) "Баклажан".hashCode()) << 32
                    & (long) "Убийца".hashCode();

        key ^= salt;

        key = (~key) + (key << 18);
        key = key ^ ((key >> 31) | (key << 33));
        key = key * 21;
        key = key ^ ((key >> 11) | (key << 53));
        key = key + (key << 6);
        key = key ^ ((key >> 22) | (key << 42));

        return (int) key;
    }

    public LocalDateTime getEventTime() {
        return eventTime;
    }

    public int getIndexPrefix() {
        return indexPrefix;
    }

    public long getUserID() {
        return userID;
    }

    public String getUsefulData() {
        return usefulData;
    }
}

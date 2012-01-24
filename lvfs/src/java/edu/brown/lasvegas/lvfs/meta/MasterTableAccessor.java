package edu.brown.lasvegas.lvfs.meta;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;

public class MasterTableAccessor {
    private final PrimaryIndex<String, MasterTable> primaryIndex;
    public MasterTableAccessor(EntityStore masterStore) {
        primaryIndex = masterStore.getPrimaryIndex(String.class, MasterTable.class);
    }
    public Object get (String key, Object defaultValue) {
        MasterTable entry = primaryIndex.get(key);
        if (entry == null) {
            return defaultValue;
        }
        return entry.value;
    }
    public void put (String key, Object value) {
        MasterTable entry = new MasterTable();
        entry.key = key;
        entry.value = value;
        primaryIndex.putNoReturn(entry);
    }
    
    public int issueNewId (int objectTypeOrdinal) {
        return issueNewIdBlock (objectTypeOrdinal, 1);
    }
    public int issueNewIdBlock (int objectTypeOrdinal, int blockSize) {
        return issueNewIdBlock ("SEQ-" + objectTypeOrdinal, blockSize);
    }
    public synchronized int issueNewIdBlock (String idSequence, int blockSize) {
        assert (blockSize > 0);
        Integer previousId = (Integer) get(idSequence, Integer.valueOf(0));
        put(idSequence, previousId + blockSize);
        return previousId + 1; // +1 is the beginning of reserved block
    }
}
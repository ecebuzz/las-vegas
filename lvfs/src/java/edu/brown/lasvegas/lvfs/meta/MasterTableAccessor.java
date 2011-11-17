package edu.brown.lasvegas.lvfs.meta;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;

public class MasterTableAccessor {
    private final PrimaryIndex<String, MasterTable> primaryIndex;
    private final EntityStore masterStore;
    public MasterTableAccessor(EntityStore masterStore) {
        this.masterStore = masterStore;
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
    
    public EntityStore getStore () {
        return masterStore;
    }

    public int issueNewId (String idSequence) {
        Integer previousId = (Integer) get(idSequence, Integer.valueOf(0));
        put(idSequence, previousId + 1);
        return previousId + 1;
    }
}
package edu.brown.lasvegas;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Represents a <b>physical</b> scheme of a projection (partitioned replica).
 */
public class LVProjectionScheme implements Serializable {
    /** logical part of table scheme. */
    private final LVTableScheme baseTableScheme;
    /** how to compress each column file. */
    private final ArrayList<Integer> columnCompressions;
    
    public LVProjectionScheme(LVTableScheme baseTableScheme, ArrayList<Integer> columnCompressions) {
        this.baseTableScheme = baseTableScheme;
        this.columnCompressions = columnCompressions;
        assert (baseTableScheme.getColumnCount() == columnCompressions.size());
    }
    
    public LVProjectionScheme(LVTableScheme baseTableScheme, int[] columnCompressions) {
        this.baseTableScheme = baseTableScheme;
        ArrayList<Integer> list = new ArrayList<Integer>();
        for (int comp : columnCompressions) list.add(comp);
        this.columnCompressions = list;
        assert (baseTableScheme.getColumnCount() == columnCompressions.length);
    }

    public LVTableScheme getBaseTableScheme () {
        return baseTableScheme;
    }
    
    public int getColumnCount() {
        return columnCompressions.size();
    }
    
    public int getColumnCompressionType (int columnIndex) {
        return columnCompressions.get(columnIndex).intValue();
    }
    
    private static final long serialVersionUID = 1L;
}

package edu.brown.lasvegas.lvfs.imp;

import java.util.ArrayList;

import edu.brown.lasvegas.LVColumnType;

/**
 * Represents a data scheme of a table stored as a text file.
 */
public class TextFileTableScheme {
    private final ArrayList<LVColumnType> columnTypes = new ArrayList<LVColumnType>();
    
    /**
     * Returns this to allow scheme.addColumn(hoge).addColumn(foo)...
     */
    public TextFileTableScheme addColumn(LVColumnType type) {
        columnTypes.add(type);
        return this;
    }
    public void removeColumn (int index) {
        assert (index < columnTypes.size());
        columnTypes.remove(index);
    }

    public int getColumnCount () {
        return columnTypes.size();
    }
    public LVColumnType getColumnType(int index) {
        assert (index < columnTypes.size());
        return columnTypes.get(index);
    }
    
    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer(1 << 13);
        buffer.append("<table>");
        for (int i = 0; i < getColumnCount(); ++i) {
            buffer.append("<column ord='" + i + "' type='" + getColumnType(i) + "' />");
        }
        buffer.append("</table>");
        return new String(buffer);
    }
}

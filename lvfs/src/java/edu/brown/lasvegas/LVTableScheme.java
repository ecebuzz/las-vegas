package edu.brown.lasvegas;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Represents a <b>logical</b> data scheme of a table in Las-Vegas system.
 */
public class LVTableScheme implements Serializable {
    private final String tableName;
    private final ArrayList<String> columnNames = new ArrayList<String>();
    private final ArrayList<Integer> columnTypes = new ArrayList<Integer>();
    
    public LVTableScheme (String name) {
        this.tableName = name;
    }

    public String getTableName() {
        return tableName;
    }
    
    /**
     * Returns this to allow scheme.addColumn(hoge).addColumn(foo)...
     */
    public LVTableScheme addColumn(String name, int type) {
        columnNames.add(name);
        columnTypes.add(type);
        assert (columnNames.size() == columnTypes.size());
        return this;
    }
    public void removeColumn (int index) {
        assert (index < columnNames.size());
        columnNames.remove(index);
        columnTypes.remove(index);
    }

    public int getColumnCount () {
        assert (columnNames.size() == columnTypes.size());
        return columnNames.size();
    }
    public String getColumnName(int index) {
        assert (index < columnNames.size());
        return columnNames.get(index);
    }
    public int getColumnType(int index) {
        assert (index < columnTypes.size());
        return columnTypes.get(index).intValue();
    }
    
    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer(1 << 13);
        // too lazy to escape strings.
        buffer.append("<lvtable name='" + tableName + "'>");
        for (int i = 0; i < getColumnCount(); ++i) {
            buffer.append("<column ord='" + i + "' name='" + getColumnName(i) + "' type='" + getColumnType(i) + "' />");
        }
        buffer.append("</lvtable>");
        return new String(buffer);
    }
    
    private static final long serialVersionUID = 1L;
}

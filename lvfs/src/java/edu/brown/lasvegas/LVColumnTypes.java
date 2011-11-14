package edu.brown.lasvegas;

import java.sql.Types;
import java.util.HashMap;

/**
 * Defines list of column types that can be stored in LVFS.
 */
public class LVColumnTypes {
    /** this number seems not used for any JDBC/XOPEN type.*/
    public static final int INVALID = -42;

    public static final int BIGINT = Types.BIGINT;
    public static final int BOOLEAN = Types.BOOLEAN;
    public static final int DATE = Types.DATE;
    public static final int DOUBLE = Types.DOUBLE;
    public static final int FLOAT = Types.FLOAT;
    public static final int INTEGER = Types.INTEGER;
    public static final int SMALLINT = Types.SMALLINT;
    public static final int TIME = Types.TIME;
    public static final int TIMESTAMP = Types.TIMESTAMP;
    public static final int TINYINT = Types.TINYINT;
    public static final int VARCHAR = Types.VARCHAR;
    

    /** all names are uppercase. */
    private static final HashMap<String, Integer> nameToIntMap;
    /** all names are uppercase. */
    private static final HashMap<Integer, String> intToNameMap;
    
    static {
        nameToIntMap = new HashMap<String, Integer>(256);
        intToNameMap = new HashMap<Integer, String>(256);
        addType(BIGINT, "BIGINT");
        addType(BOOLEAN, "BOOLEAN");
        addType(DATE, "DATE");
        addType(DOUBLE, "DOUBLE");
        addType(FLOAT, "FLOAT");
        addType(INTEGER, "INTEGER");
        addType(SMALLINT, "SMALLINT");
        addType(TIME, "TIME");
        addType(TIMESTAMP, "TIMESTAMP");
        addType(TINYINT, "TINYINT");
        addType(VARCHAR, "VARCHAR");
    }
    private static void addType (int type, String name) {
        name = name.toUpperCase();
        assert (!nameToIntMap.containsKey(name));
        assert (!intToNameMap.containsKey(type));
        nameToIntMap.put(name, type);
        intToNameMap.put(type, name);
    }
    
    public static String toTypeName (int type) {
        String name = intToNameMap.get(type);
        if (name == null) {
            return "UNKNOWN (" + type + ")";
        }
        return name;
    }
    public static int toTypeInt (String name) {
        Integer type = nameToIntMap.get(name);
        if (type == null) {
            return INVALID;
        }
        return type.intValue();
    }

    private LVColumnTypes() {}
}

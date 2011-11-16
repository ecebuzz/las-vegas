package edu.brown.lasvegas;

import java.sql.Types;

/**
 * Defines list of column types that can be stored in LVFS.
 */
public enum LVColumnType {
    
    /** The INVALID. */
    INVALID(-42),
    
    /** The BIGINT. */
    BIGINT(Types.BIGINT),
    
    /** The BOOLEAN. */
    BOOLEAN(Types.BOOLEAN),
    
    /** The DATE. */
    DATE(Types.DATE),
    
    /** The DOUBLE. */
    DOUBLE(Types.DOUBLE),
    
    /** The FLOAT. */
    FLOAT(Types.FLOAT),
    
    /** The INTEGER. */
    INTEGER(Types.INTEGER),
    
    /** The SMALLINT. */
    SMALLINT(Types.SMALLINT),
    
    /** The TIME. */
    TIME(Types.TIME),
    
    /** The TIMESTAMP. */
    TIMESTAMP(Types.TIMESTAMP),
    
    /** The TINYINT. */
    TINYINT(Types.TINYINT),
    
    /** The VARCHAR. */
    VARCHAR(Types.VARCHAR),
    ;

    /**
     * Corresponding JDBC/XOpen type code.
     */
    private final int xopenType;
    
    /**
     * Gets the corresponding JDBC/XOpen type code.
     *
     * @return the corresponding JDBC/XOpen type code
     */
    int getXopenType() {
        return xopenType;
    }
    
    /**
     * Instantiates a new lV column type.
     *
     * @param xopenType the xopen type
     */
    LVColumnType (int xopenType) {
        this.xopenType = xopenType;
    }
    
}

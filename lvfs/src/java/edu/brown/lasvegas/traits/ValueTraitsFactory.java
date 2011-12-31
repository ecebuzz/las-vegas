package edu.brown.lasvegas.traits;


import edu.brown.lasvegas.ColumnType;

/**
 * Factory for value traits classes.
 */
public final class ValueTraitsFactory {
    /**
     * Creates an instance of value traits for the given data type.
     * This is useful when you can't specify the data type in your code.
     * Otherwise, directly instantiate your desired traits class, which improves type safety.
     */
    public static ValueTraits<?, ?> getInstance (ColumnType type) {
        switch (type) {
        case DATE:
        case TIME:
        case TIMESTAMP:
        case BIGINT:
            return new BigintValueTraits();
        case INTEGER:
            return new IntegerValueTraits();
        case SMALLINT:
            return new SmallintValueTraits();
        case BOOLEAN:
        case TINYINT:
            return new TinyintValueTraits();
        case FLOAT:
            return new FloatValueTraits();
        case DOUBLE:
            return new DoubleValueTraits();
        case VARCHAR:
            return new VarcharValueTraits();
        case VARBINARY:
            return new VarbinValueTraits();
        default:
            throw new IllegalArgumentException("unexpected type: " + type);
        }
    }

    private ValueTraitsFactory() {}
}

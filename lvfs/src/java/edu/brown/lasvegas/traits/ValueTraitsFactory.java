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

    /**
     * Creates an instance of integer value traits for the given data size.
     * This is useful when you can't specify the data type in your code.
     * Otherwise, directly instantiate your desired traits class, which improves type safety.
     */
    public static FixLenValueTraits<?, ?> getIntegerTraits (byte size) {
        if (size == 1) {
            return new TinyintValueTraits();
        } else if (size == 2) {
            return new SmallintValueTraits();
        } else if (size == 4) {
            return new IntegerValueTraits();
        } else {
            assert (size == 8);
            return new BigintValueTraits();
        }
    }

    /**
     * Creates an instance of floating point value traits for the given data size.
     * This is useful when you can't specify the data type in your code.
     * Otherwise, directly instantiate your desired traits class, which improves type safety.
     */
    public static FixLenValueTraits<?, ?> getFloatTraits (byte size) {
        if (size == 4) {
            return new FloatValueTraits();
        } else {
            assert (size == 8);
            return new DoubleValueTraits();
        }
    }

    private ValueTraitsFactory() {}
}

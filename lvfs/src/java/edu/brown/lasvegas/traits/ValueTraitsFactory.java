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
            return BIGINT_TRAITS;
        case INTEGER:
            return INTEGER_TRAITS;
        case SMALLINT:
            return SMALLINT_TRAITS;
        case BOOLEAN:
        case TINYINT:
            return TINYINT_TRAITS;
        case FLOAT:
            return FLOAT_TRAITS;
        case DOUBLE:
            return DOUBLE_TRAITS;
        case VARCHAR:
            return VARCHAR_TRAITS;
        case VARBINARY:
            return VARBIN_TRAITS;
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
            return TINYINT_TRAITS;
        } else if (size == 2) {
            return SMALLINT_TRAITS;
        } else if (size == 4) {
            return INTEGER_TRAITS;
        } else {
            assert (size == 8);
            return BIGINT_TRAITS;
        }
    }

    /**
     * Creates an instance of floating point value traits for the given data size.
     * This is useful when you can't specify the data type in your code.
     * Otherwise, directly instantiate your desired traits class, which improves type safety.
     */
    public static FixLenValueTraits<?, ?> getFloatTraits (byte size) {
        if (size == 4) {
            return FLOAT_TRAITS;
        } else {
            assert (size == 8);
            return DOUBLE_TRAITS;
        }
    }

    // reused instances. traits classes should be totally state-less. reusing has no problem.
    public static final BigintValueTraits BIGINT_TRAITS = new BigintValueTraits();
    public static final IntegerValueTraits INTEGER_TRAITS = new IntegerValueTraits();
    public static final SmallintValueTraits SMALLINT_TRAITS = new SmallintValueTraits();
    public static final TinyintValueTraits TINYINT_TRAITS = new TinyintValueTraits();
    public static final FloatValueTraits FLOAT_TRAITS = new FloatValueTraits();
    public static final DoubleValueTraits DOUBLE_TRAITS = new DoubleValueTraits();
    public static final VarcharValueTraits VARCHAR_TRAITS = new VarcharValueTraits();
    public static final VarbinValueTraits VARBIN_TRAITS = new VarbinValueTraits();

    private ValueTraitsFactory() {}
}

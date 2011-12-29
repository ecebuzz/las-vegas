package edu.brown.lasvegas.lvfs.local;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.ValueIndex;
import edu.brown.lasvegas.lvfs.ValueTraits;

/**
 * Testcases for {@link LocalValFile}.
 */
public class LocalValFileTest {
    private <T extends Comparable<T>, AT> void runTest (ColumnType type, ValueTraits<T, AT> traits, T[] values) throws Exception {
        int count = values.length;
        LocalVirtualFile file = new LocalVirtualFile("test/local/test.vdx");
        file.delete();
        List<Integer> positions = new ArrayList<Integer>();
        List<T> valueList = new ArrayList<T>();
        for (int i = 0; i < count; ++i) {
            positions.add(i * 513);
            valueList.add(values[i]);
        }
        LocalValFile<T, AT> valFile = new LocalValFile<T, AT>(valueList, positions, type);
        validateValueIndex (valFile, values);
        valFile.writeToFile(file);
        valFile = new LocalValFile<T, AT>(file, type);
        validateValueIndex (valFile, values);
    }
    @SuppressWarnings("unchecked")
    private <T extends Comparable<T>> void validateValueIndex (ValueIndex<T> index, T[] values) throws Exception {
        for (int i = 0; i < values.length; ++i) {
            int lastStrictlySmaller = -1, firstEqual = -1, lastEqual = -1;
            for (int j = 0; j < values.length; ++j) {
                if (values[j].compareTo(values[i]) < 0) {
                    lastStrictlySmaller = j;
                }
                if (values[j].compareTo(values[i]) == 0) {
                    lastEqual = j;
                    if (firstEqual == -1) {
                        firstEqual = j;
                    }
                }
            }
            assert (firstEqual != -1);
            assert (lastEqual != -1);
            boolean negInf = values[i].equals(Float.NEGATIVE_INFINITY) || values[i].equals(Double.NEGATIVE_INFINITY); // if so, decrement will not work
            boolean posInf = values[i].equals(Float.POSITIVE_INFINITY) || values[i].equals(Double.POSITIVE_INFINITY); // if so, increment will not work
            if (lastStrictlySmaller == -1) {
                assert (firstEqual == 0);
                if (!negInf) {
                    assertEquals(-1, index.searchValues((T) decrement(values[i])));
                }
                assertEquals(0, index.searchValues(values[i]));
                if (!posInf) {
                    assertEquals(lastEqual * 513, index.searchValues((T) increment(values[i])));
                }
            } else {
                if (!negInf) {
                    assertEquals(lastStrictlySmaller * 513, index.searchValues((T) decrement(values[i])));
                }
                assertEquals(lastStrictlySmaller * 513, index.searchValues(values[i]));
                if (!posInf) {
                    assertEquals(lastEqual * 513, index.searchValues((T) increment(values[i])));
                }
            }
        }
    }
    private Object increment (Object v) {
        if (v instanceof Long) {
            return ((Long) v).longValue() + 1L;
        }
        if (v instanceof Integer) {
            return ((Integer) v).intValue() + 1;
        }
        if (v instanceof Short) {
            return (short) (((Short) v).shortValue() + 1);
        }
        if (v instanceof Byte) {
            return (byte) (((Byte) v).byteValue() + 1);
        }
        if (v instanceof Float) {
            return (float) (((Float) v).floatValue() + 1);
        }
        if (v instanceof Double) {
            return (double) (((Double) v).doubleValue() + 1);
        }
        if (v instanceof String) {
            String str = (String) v;
            return str.substring(0, str.length() - 1) + (char) (str.charAt(str.length() - 1) + 1);
        }
        return null;
    }
    private Object decrement (Object v) {
        if (v instanceof Long) {
            return ((Long) v).longValue() - 1L;
        }
        if (v instanceof Integer) {
            return ((Integer) v).intValue() - 1;
        }
        if (v instanceof Short) {
            return (short) (((Short) v).shortValue() - 1);
        }
        if (v instanceof Byte) {
            return (byte) (((Byte) v).byteValue() - 1);
        }
        if (v instanceof Float) {
            return (float) (((Float) v).floatValue() - 1);
        }
        if (v instanceof Double) {
            return (double) (((Double) v).doubleValue() - 1);
        }
        if (v instanceof String) {
            String str = (String) v;
            return str.substring(0, str.length() - 1) + (char) (str.charAt(str.length() - 1) - 1);
        }
        return null;
    }
 
    @Test
    public void testTinyint() throws Exception {
        runTest (ColumnType.TINYINT, new AllValueTraits.TinyintValueTraits(),
            new Byte[]{(byte) -100, (byte) -80, (byte) -80, (byte) -40, (byte) -40, (byte) -10, (byte) 0, (byte) 80});
    }

    @Test
    public void testSmallint() throws Exception {
        runTest (ColumnType.SMALLINT, new AllValueTraits.SmallintValueTraits(),
            new Short[]{(short) -100, (short) -80, (short) -80, (short) -40, (short) -40, (short) -10, (short) 0, (short) 80});
    }

    @Test
    public void testInteger() throws Exception {
        runTest (ColumnType.INTEGER, new AllValueTraits.IntegerValueTraits(),
            new Integer[]{-12312312, -12312312, -4320, (int) -100, (int) -80, (int) -80, (int) -40, (int) -40, (int) -10, (int) 0, 0, (int) 80, 304950394});
    }

    @Test
    public void testBigint() throws Exception {
        runTest (ColumnType.BIGINT, new AllValueTraits.BigintValueTraits(),
            new Long[]{-2349283948923849234L, -9829839823L, -9829839823L, (long) -100, (long) -80, (long) -80, (long) -40, (long) -40, (long) -10, (long) 0, (long) 80, 9238409238094234L});
    }


    @Test
    public void testFloat() throws Exception {
        runTest (ColumnType.FLOAT, new AllValueTraits.FloatValueTraits(),
            new Float[]{Float.NEGATIVE_INFINITY, -3.1E5f, -3.1E5f, (float) -100, (float) -80, (float) -80, (float) -40, (float) -40, (float) -10, (float) 0, (float) 80, 3.1E5f, 3.1E5f, Float.POSITIVE_INFINITY});
    }

    @Test
    public void testDouble() throws Exception {
        runTest (ColumnType.DOUBLE, new AllValueTraits.DoubleValueTraits(),
            new Double[]{-3.1E10, -3.1E10, (double) -100, (double) -80, (double) -80, (double) -40, (double) -40, (double) -10, (double) 0, (double) 80, 3.1E10, 3.1E10, Double.POSITIVE_INFINITY});
    }


    @Test
    public void testVarchar() throws Exception {
        runTest (ColumnType.VARCHAR, new AllValueTraits.VarcharValueTraits(),
            new String[]{"str0000012", "str0000012", "str0000032", "str0000032", "str0000128", "str0000222", "str0000356", "str0000389"});
    }
}

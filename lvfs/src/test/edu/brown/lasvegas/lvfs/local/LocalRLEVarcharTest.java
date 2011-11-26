package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.lvfs.AllValueTraits;
import edu.brown.lasvegas.lvfs.VarLenValueTraits;

public class LocalRLEVarcharTest extends LocalRLETestBase<String, String[]> {
    @Override
    protected String generateValue(int index) { return "str" + (index / 4) + "sdfdf";  }
    @Override
    protected VarLenValueTraits<String> createTraits() { return new AllValueTraits.VarcharValueTraits();}
    @Override
    protected String[] createArray (int size) { return new String[size];}
    @Override
    protected void setToArray(String[] array, int index, String value){ array[index] = value; }
    @Override
    protected String getFromArray(String[] array, int index) { return array[index]; }
}

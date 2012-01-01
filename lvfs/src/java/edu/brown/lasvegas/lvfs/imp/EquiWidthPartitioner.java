package edu.brown.lasvegas.lvfs.imp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.traits.ValueTraits;
import edu.brown.lasvegas.traits.ValueTraitsFactory;
import edu.brown.lasvegas.tuple.SampleableTupleReader;
import edu.brown.lasvegas.tuple.TupleBuffer;
import edu.brown.lasvegas.util.ValueRange;

/**
 * A VERY simple, fast, and inaccurate implementation of equi-width partitioning.
 * 
 * <p>This class takes random samples of tuples from the input file and retrieves the smallest
 * and the largest partitioning attributes.</p>
 * 
 * <p>Then, it uniformly divides the ranges between the minimum and maximum
 * values of them. Of course, this is VERY inaccurate in the existence of
 * skewness. The goal of this class is not to give the definitive mean for
 * partitioning designs but to provide a very simple and fast alternative.
 * Remember, partitioning logic is plug-able, this is just a default.</p>
 * 
 * <p>Also, this implementation can't detect the exact minimal and maximal values,
 * so the global beginning and global ending values will be NULL.</p>
 */
public class EquiWidthPartitioner<T extends Comparable<T>, AT> {
    private static Logger LOG = Logger.getLogger(EquiWidthPartitioner.class);
    /**
     * Designs partition ranges by sampling the given files. See the class comment for more details.
     * @param tupleReader data files that can provide random samples.
     * @param partitioningColumnIndex the column used for partitioning
     * @param numPartitions the expected number of partitions. The actual count might be different.
     * @param sampleSize the approximate number of tuples to sample
     * @return designed partitions.
     * @throws IOException
     */
    public static ValueRange[] designPartitions (SampleableTupleReader tupleReader, int partitioningColumnIndex, int numPartitions, int sampleSize) throws IOException {
        ColumnType type = tupleReader.getColumnType(partitioningColumnIndex);
        EquiWidthPartitioner<?, ?> partitioner;
        switch(type) {
        case DATE:
        case TIME:
        case TIMESTAMP:
        case BIGINT: partitioner = new EquiWidthPartitioner<Long, long[]>(type); break;
        case DOUBLE: partitioner = new EquiWidthPartitioner<Double, double[]>(type); break;
        case FLOAT: partitioner = new EquiWidthPartitioner<Float, float[]>(type); break;
        case INTEGER: partitioner = new EquiWidthPartitioner<Integer, int[]>(type); break;
        case SMALLINT: partitioner = new EquiWidthPartitioner<Short, short[]>(type); break;
        case BOOLEAN:
        case TINYINT: partitioner = new EquiWidthPartitioner<Byte, byte[]>(type); break;
        case VARBINARY: throw new IllegalArgumentException("partitioning by VARBINARY column is not supported");
        case VARCHAR: partitioner = new EquiWidthPartitioner<String, String[]>(type); break;

        default:
            throw new IOException ("Unexpected column type:" + type);
        }
        
        return partitioner.design(tupleReader, partitioningColumnIndex, numPartitions, sampleSize);
    }
    /**
     * overload that uses default sample size.
     */
    public static ValueRange[] designPartitions (SampleableTupleReader tupleReader, int partitioningColumnIndex, int numPartitions) throws IOException {
        return designPartitions(tupleReader, partitioningColumnIndex, numPartitions, DEFAULT_SAMPLE_SIZE);
    }
    private final static int DEFAULT_SAMPLE_SIZE = 1 << 16;
    private final ValueSplitter splitter;
    private final ColumnType type;
    private final ValueTraits<T, AT> traits;
    @SuppressWarnings("unchecked")
    private EquiWidthPartitioner(ColumnType type) {
        this.type = type;
        switch(type) {
        case DATE:
        case TIME:
        case TIMESTAMP:
        case BIGINT: splitter = new LongValueSplitter(); break;
        case DOUBLE: splitter = new DoubleValueSplitter(); break;
        case FLOAT: splitter = new FloatValueSplitter(); break;
        case INTEGER: splitter = new IntegerValueSplitter(); break;
        case SMALLINT: splitter = new ShortValueSplitter(); break;
        case BOOLEAN:
        case TINYINT: splitter = new ByteValueSplitter(); break;
        case VARBINARY: throw new IllegalArgumentException("partitioning by VARBINARY column is not supported");
        case VARCHAR: splitter = new StringValueSplitter(); break;

        default:
            throw new IllegalArgumentException ("Unexpected column type:" + type);
        }
        this.traits = (ValueTraits<T, AT>) ValueTraitsFactory.getInstance(type);
    }
    
    
    private ValueRange[] design (SampleableTupleReader tupleReader, int partitioningColumnIndex, int numPartitions, int sampleSize) throws IOException {
        LOG.info("Designing " + numPartitions + " partitions");
        TupleBuffer samples = new TupleBuffer(tupleReader.getColumnTypes(), sampleSize);
        tupleReader.sample(samples);
        ValueRange minmax = getMinMax(samples, partitioningColumnIndex);
        // uniformly divide the range
        ValueRange[] ret = splitter.split(type, minmax.getStartKey(), minmax.getEndKey(), numPartitions).toArray(new ValueRange[0]);
        if (LOG.isInfoEnabled()) {
            StringBuffer msg = new StringBuffer();
            for (int i = 0; i < ret.length; ++i) {
                msg.append(ret[i] + ",");
            }
            LOG.info("divided into " + ret.length + " partitions: " + msg);
        }
        return ret;
    }
    private ValueRange getMinMax (TupleBuffer samples, int partitioningColumnIndex) throws IOException {
        T min = null, max = null;
        @SuppressWarnings("unchecked")
        AT buffer = (AT) samples.getColumnBuffer(partitioningColumnIndex);
        for (int i = 0; i < samples.getCount(); ++i) {
            T val = traits.get(buffer, i);
            if (min == null || val.compareTo(min) < 0) {
                min = val;
            }
            if (max == null || val.compareTo(max) > 0) {
                max = val;
            }
        }
        return new ValueRange(type, min, max);
    }
    private static class ByteValueSplitter implements ValueSplitter {
        @Override
        public List<ValueRange> split(ColumnType type, Comparable<?> minC, Comparable<?> maxC, int numSplits) {
            Byte min = (Byte) minC;
            Byte max = (Byte) maxC;
            List<ValueRange> list = new ArrayList<ValueRange>();
            if (numSplits == 1) {
                list.add (new ValueRange(type, null, null));
            } else {
                Byte prevEnd = null;
                for (int i = 0; i < numSplits; ++i) {
                    byte nextEnd = (byte) (min + ((double) (max - min) * i / numSplits));
                    if (prevEnd == null || nextEnd != prevEnd) {
                        list.add(new ValueRange(type, prevEnd, nextEnd));
                        prevEnd = nextEnd;
                    }
                }
                list.add(new ValueRange(type, prevEnd, null));
            }
            return list;
        }
    }

    private static class ShortValueSplitter implements ValueSplitter {
        @Override
        public List<ValueRange> split(ColumnType type, Comparable<?> minC, Comparable<?> maxC, int numSplits) {
            Short min = (Short) minC;
            Short max = (Short) maxC;
            List<ValueRange> list = new ArrayList<ValueRange>();
            if (numSplits == 1) {
                list.add (new ValueRange(type, null, null));
            } else {
                Short prevEnd = null;
                for (int i = 0; i < numSplits; ++i) {
                    short nextEnd = (short) (min + ((double) (max - min) * i / numSplits));
                    if (prevEnd == null || nextEnd != prevEnd) {
                        list.add(new ValueRange(type, prevEnd, nextEnd));
                        prevEnd = nextEnd;
                    }
                }
                list.add(new ValueRange(type, prevEnd, null));
            }
            return list;
        }
    }


    private static class IntegerValueSplitter implements ValueSplitter {
        @Override
        public List<ValueRange> split(ColumnType type, Comparable<?> minC, Comparable<?> maxC, int numSplits) {
            Integer min = (Integer) minC;
            Integer max = (Integer) maxC;
            List<ValueRange> list = new ArrayList<ValueRange>();
            if (numSplits == 1) {
                list.add (new ValueRange(type, null, null));
            } else {
                Integer prevEnd = null;
                for (int i = 0; i < numSplits; ++i) {
                    int nextEnd = (int) (min + ((double) (max - min) * i / numSplits));
                    if (prevEnd == null || nextEnd != prevEnd) {
                        list.add(new ValueRange(type, prevEnd, nextEnd));
                        prevEnd = nextEnd;
                    }
                }
                list.add(new ValueRange(type, prevEnd, null));
            }
            return list;
        }
    }

    private static class LongValueSplitter implements ValueSplitter {
        @Override
        public List<ValueRange> split(ColumnType type, Comparable<?> minC, Comparable<?> maxC, int numSplits) {
            Long min = (Long) minC;
            Long max = (Long) maxC;
            List<ValueRange> list = new ArrayList<ValueRange>();
            if (numSplits == 1) {
                list.add (new ValueRange(type, null, null));
            } else {
                Long prevEnd = null;
                for (int i = 0; i < numSplits; ++i) {
                    long nextEnd = (long) (min + ((double) (max - min) * i / numSplits));
                    if (prevEnd == null || nextEnd != prevEnd) {
                        list.add(new ValueRange(type, prevEnd, nextEnd));
                        prevEnd = nextEnd;
                    }
                }
                list.add(new ValueRange(type, prevEnd, null));
            }
            return list;
        }
    }


    private static class FloatValueSplitter implements ValueSplitter {
        @Override
        public List<ValueRange> split(ColumnType type, Comparable<?> minC, Comparable<?> maxC, int numSplits) {
            Float min = (Float) minC;
            Float max = (Float) maxC;
            List<ValueRange> list = new ArrayList<ValueRange>();
            if (numSplits == 1) {
                list.add (new ValueRange(type, null, null));
            } else {
                Float prevEnd = null;
                for (int i = 0; i < numSplits; ++i) {
                    float nextEnd = (float) (min + ((double) (max - min) * i / numSplits));
                    if (prevEnd == null || nextEnd != prevEnd) {
                        list.add(new ValueRange(type, prevEnd, nextEnd));
                        prevEnd = nextEnd;
                    }
                }
                list.add(new ValueRange(type, prevEnd, null));
            }
            return list;
        }
    }

    private static class DoubleValueSplitter implements ValueSplitter {
        @Override
        public List<ValueRange> split(ColumnType type, Comparable<?> minC, Comparable<?> maxC, int numSplits) {
            Double min = (Double) minC;
            Double max = (Double) maxC;
            List<ValueRange> list = new ArrayList<ValueRange>();
            if (numSplits == 1) {
                list.add (new ValueRange(type, null, null));
            } else {
                Double prevEnd = null;
                for (int i = 0; i < numSplits; ++i) {
                    double nextEnd = (double) (min + ((double) (max - min) * i / numSplits));
                    if (prevEnd == null || nextEnd != prevEnd) {
                        list.add(new ValueRange(type, prevEnd, nextEnd));
                        prevEnd = nextEnd;
                    }
                }
                list.add(new ValueRange(type, prevEnd, null));
            }
            return list;
        }
    }

    /** this one is tricky... */
    private static class StringValueSplitter implements ValueSplitter {
        @Override
        public List<ValueRange> split(ColumnType type, Comparable<?> minC, Comparable<?> maxC, int numSplits) {
            String min = (String) minC;
            String max = (String) maxC;
            List<ValueRange> list = new ArrayList<ValueRange>();
            if (numSplits == 1) {
                list.add (new ValueRange(type, null, null));
                return list;
            }
            // first, we rip off common leading characters (prefix).
            int prefixLen;
            for (prefixLen = 0; prefixLen < min.length() && prefixLen < max.length() && min.charAt(prefixLen) == max.charAt(prefixLen); ++prefixLen);
            String prefix = min.substring(0, prefixLen);
            assert (prefix.equals(min.substring(0, prefixLen)));
            assert (prefix.equals(max.substring(0, prefixLen)));
            
            // then, consider the first char after the prefix.
            char start = (prefixLen == min.length() ? 0 : min.charAt(prefixLen));
            char end = (prefixLen == max.length() ? 0 : max.charAt(prefixLen));

            // split it. this is far from complete, but we have no idea are what the alphabet in 2nd char and later.
            // anyway, this class is a stupid default implementation.
            Character prevEnd = null;
            for (int i = 0; i < numSplits; ++i) {
                char nextEnd = (char) (start + ((char) (end - start) * i / numSplits));
                if (prevEnd == null || nextEnd != prevEnd) {
                    list.add(new ValueRange(type, prevEnd == null ? null : prefix + prevEnd, prefix + nextEnd));
                    prevEnd = nextEnd;
                }
            }
            list.add(new ValueRange(type, prefix + prevEnd, null));
            return list;
        }
    }
}

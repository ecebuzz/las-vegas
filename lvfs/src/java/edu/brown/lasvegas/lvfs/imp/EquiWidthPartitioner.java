package edu.brown.lasvegas.lvfs.imp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.tuple.InputTableReader;
import edu.brown.lasvegas.util.ValueRange;

/**
 * A VERY simple, fast, and inaccurate implementation of equi-width partitioning.
 * 
 * <p>This class looks at the first 10MB of the first file
 * and last 10MB of the last file, retrieving the smallest
 * and the largest partitioning attributes.</p>
 * 
 * <p>Then, it uniformly divides the ranges between the minimum and maximum
 * values of them. Of course, this is VERY inaccurate in the existence of
 * skewness and/or non-sorted input. The goal of this class is not to give the definitive mean for
 * partitioning designs but to provide a very simple and fast alternative.
 * Remember, partitioning logic is plug-able, this is just a default.</p>
 * 
 * <p>Also, this implementation can't detect the exact minimal and maximal values,
 * so the global beginning and global ending values will be NULL. The data import code
 * should modify the beginning value of the first partition, and the ending value of the
 * last partition while importing.</p>
 */
public class EquiWidthPartitioner<T extends Comparable<T>> {
    private static Logger LOG = Logger.getLogger(EquiWidthPartitioner.class);
    /**
     * Designs partition ranges by sampling the given files. See the class comment for more details.
     * @param firstSplit The first file to import. If the partitioning column is not correlated with
     * tuple position, any file would work fine.
     * @param lastSplit The last file to import. If the partitioning column is not correlated with
     * tuple position, any file would work fine. Could be same as firstSplit if there is only one file to import.
     * @param partitioningColumnIndex the column used for partitioning
     * @param numPartitions the expected number of partitions. The actual count might be different.
     * @param sampleByteSize
     * @return designed partitions.
     * @throws IOException
     */
    public static ValueRange[] designPartitions (InputTableReader firstSplit, InputTableReader lastSplit, int partitioningColumnIndex, int numPartitions, int sampleByteSize) throws IOException {
        ColumnType type = firstSplit.getColumnType(partitioningColumnIndex);
        EquiWidthPartitioner<?> partitioner;
        switch(type) {
        case DATE:
        case TIME:
        case TIMESTAMP:
        case BIGINT: partitioner = new EquiWidthPartitioner<Long>(type); break;
        case DOUBLE: partitioner = new EquiWidthPartitioner<Double>(type); break;
        case FLOAT: partitioner = new EquiWidthPartitioner<Float>(type); break;
        case INTEGER: partitioner = new EquiWidthPartitioner<Integer>(type); break;
        case SMALLINT: partitioner = new EquiWidthPartitioner<Short>(type); break;
        case BOOLEAN:
        case TINYINT: partitioner = new EquiWidthPartitioner<Byte>(type); break;
        case VARBINARY: throw new IllegalArgumentException("partitioning by VARBINARY column is not supported");
        case VARCHAR: partitioner = new EquiWidthPartitioner<String>(type); break;

        default:
            throw new IOException ("Unexpected column type:" + type);
        }
        
        return partitioner.design(firstSplit, lastSplit, partitioningColumnIndex, numPartitions, sampleByteSize);
    }
    /**
     * overload that uses default sample size.
     * @see #designPartitions(InputTableReader, InputTableReader, int, int, int)
     */
    public static ValueRange[] designPartitions (InputTableReader firstSplit, InputTableReader lastSplit, int partitioningColumnIndex, int numPartitions) throws IOException {
        return designPartitions(firstSplit, lastSplit, partitioningColumnIndex, numPartitions, DEFAULT_SAMPLE_BYTE_SIZE);
    }
    private final static int DEFAULT_SAMPLE_BYTE_SIZE = 1 << 20;
    private final ValueSplitter splitter;
    private final ColumnType type;
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
    }
    
    
    @SuppressWarnings("unchecked")
    private ValueRange[] design (InputTableReader firstSplit, InputTableReader lastSplit, int partitioningColumnIndex, int numPartitions, int sampleByteSize) throws IOException {
        LOG.info("Designing " + numPartitions + " partitions from first-file:" + firstSplit + " and last-file:" + lastSplit);
        //first SAMPLE_BYTE_SIZE from first-file
        firstSplit.reset();
        ValueRange minmax1 = getMinMax(firstSplit, partitioningColumnIndex, sampleByteSize);
        //last SAMPLE_BYTE_SIZE from last-file
        long lastSplitLen = lastSplit.length();
        long seekpos = lastSplitLen - sampleByteSize;
        if (seekpos < 0) seekpos = 0;
        lastSplit.seekApproximate(seekpos);
        ValueRange minmax2 = getMinMax(lastSplit, partitioningColumnIndex, sampleByteSize);
        ValueRange minmax = new ValueRange(type,
            ((T) minmax1.getStartKey()).compareTo((T) minmax2.getStartKey()) < 0 ? minmax1.getStartKey() : minmax2.getStartKey(),
            ((T) minmax1.getEndKey()).compareTo((T) minmax2.getEndKey()) > 0 ? minmax1.getEndKey() : minmax2.getEndKey());
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
    private ValueRange getMinMax (InputTableReader file, int partitioningColumnIndex, int sampleByteSize) throws IOException {
        T min = null, max = null;
        for (int totalRead = 0; totalRead < sampleByteSize;) {
            if (!file.next()) {
                break;
            }
            @SuppressWarnings("unchecked")
            T val = (T) file.getObject(partitioningColumnIndex);
            if (min == null || val.compareTo(min) < 0) {
                min = val;
            }
            if (max == null || val.compareTo(max) > 0) {
                max = val;
            }
            totalRead += file.getCurrentTupleByteSize();
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

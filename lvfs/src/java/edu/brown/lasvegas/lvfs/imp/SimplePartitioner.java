package edu.brown.lasvegas.lvfs.imp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.ColumnType;
import edu.brown.lasvegas.InputTableReader;
import edu.brown.lasvegas.util.ValueRange;

/**
 * A VERY simple, fast, and inaccurate implementation of partitioning logic.
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
public class SimplePartitioner<T extends Comparable<T>> {
    private static Logger LOG = Logger.getLogger(SimplePartitioner.class);
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
    public static ValueRange<?>[] designPartitions (InputTableReader firstSplit, InputTableReader lastSplit, int partitioningColumnIndex, int numPartitions, int sampleByteSize) throws IOException {
        ColumnType type = firstSplit.getColumnType(partitioningColumnIndex);
        SimplePartitioner<?> partitioner;
        switch(type) {
        case BIGINT: partitioner = new SimplePartitioner<Long>(new LongValueSplitter()); break;
        case BOOLEAN: partitioner = new SimplePartitioner<Boolean>(new BooleanValueSplitter()); break;
        case DOUBLE: partitioner = new SimplePartitioner<Double>(new DoubleValueSplitter()); break;
        case FLOAT: partitioner = new SimplePartitioner<Float>(new FloatValueSplitter()); break;
        case INTEGER: partitioner = new SimplePartitioner<Integer>(new IntegerValueSplitter()); break;
        case SMALLINT: partitioner = new SimplePartitioner<Short>(new ShortValueSplitter()); break;
        case TINYINT: partitioner = new SimplePartitioner<Byte>(new ByteValueSplitter()); break;
        case VARBINARY: throw new IllegalArgumentException("partitioning by VARBINARY column is not supported");
        case VARCHAR: partitioner = new SimplePartitioner<String>(new StringValueSplitter()); break;

        case DATE: partitioner = new SimplePartitioner<Long>(new LongValueSplitter()); break;
        case TIME: partitioner = new SimplePartitioner<Long>(new LongValueSplitter()); break;
        case TIMESTAMP: partitioner = new SimplePartitioner<Long>(new LongValueSplitter()); break;
        default:
            throw new IOException ("Unexpected column type:" + type);
        }
        
        return partitioner.design(firstSplit, lastSplit, partitioningColumnIndex, numPartitions, sampleByteSize);
    }
    /**
     * overload that uses default sample size.
     * @see #designPartitions(InputTableReader, InputTableReader, int, int, int)
     */
    public static ValueRange<?>[] designPartitions (InputTableReader firstSplit, InputTableReader lastSplit, int partitioningColumnIndex, int numPartitions) throws IOException {
        return designPartitions(firstSplit, lastSplit, partitioningColumnIndex, numPartitions, DEFAULT_SAMPLE_BYTE_SIZE);
    }
    private final static int DEFAULT_SAMPLE_BYTE_SIZE = 1 << 20;
    private final ValueSplitter<T> splitter;
    private SimplePartitioner(ValueSplitter<T> splitter) {
        this.splitter = splitter;
    }
    
    
    private ValueRange<?>[] design (InputTableReader firstSplit, InputTableReader lastSplit, int partitioningColumnIndex, int numPartitions, int sampleByteSize) throws IOException {
        LOG.info("Designing " + numPartitions + " partitions from first-file:" + firstSplit + " and last-file:" + lastSplit);
        //first SAMPLE_BYTE_SIZE from first-file
        firstSplit.reset();
        ValueRange<T> minmax1 = getMinMax(firstSplit, partitioningColumnIndex, sampleByteSize);
        //last SAMPLE_BYTE_SIZE from last-file
        long lastSplitLen = lastSplit.length();
        long seekpos = lastSplitLen - sampleByteSize;
        if (seekpos < 0) seekpos = 0;
        lastSplit.seekApproximate(seekpos);
        ValueRange<T> minmax2 = getMinMax(lastSplit, partitioningColumnIndex, sampleByteSize);
        ValueRange<T> minmax = new ValueRange<T>(minmax1.getStartKey().compareTo(minmax2.getStartKey()) < 0 ? minmax1.getStartKey() : minmax2.getStartKey(),
                    minmax1.getEndKey().compareTo(minmax2.getEndKey()) > 0 ? minmax1.getEndKey() : minmax2.getEndKey());
        // uniformly divide the range
        ValueRange<?>[] ret = splitter.split(minmax.getStartKey(), minmax.getEndKey(), numPartitions).toArray(new ValueRange<?>[0]);
        if (LOG.isInfoEnabled()) {
            StringBuffer msg = new StringBuffer();
            for (int i = 0; i < ret.length; ++i) {
                msg.append(ret[i] + ",");
            }
            LOG.info("divided into " + ret.length + " partitions: " + msg);
        }
        return ret;
    }
    private ValueRange<T> getMinMax (InputTableReader file, int partitioningColumnIndex, int sampleByteSize) throws IOException {
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
        return new ValueRange<T>(min, max);
    }
    private interface ValueSplitter<T extends Comparable<T>> {
        /** uniformly split the range into numSplits partitions. */
        List<ValueRange<T>> split (T min, T max, int numSplits);
    }
    private static class BooleanValueSplitter implements ValueSplitter<Boolean> {
        @Override
        public List<ValueRange<Boolean>> split(Boolean min, Boolean max, int numSplits) {
            // at most only 2 partitions possible.
            List<ValueRange<Boolean>> list = new ArrayList<ValueRange<Boolean>>();
            if (numSplits == 1 || min.booleanValue() || !max.booleanValue()) {
                list.add(new ValueRange<Boolean>(null, null));
            } else {
                list.add(new ValueRange<Boolean>(null, Boolean.TRUE));
                list.add(new ValueRange<Boolean>(Boolean.TRUE, null));
            }
            return list;
        }
    }
    private static class ByteValueSplitter implements ValueSplitter<Byte> {
        @Override
        public List<ValueRange<Byte>> split(Byte min, Byte max, int numSplits) {
            List<ValueRange<Byte>> list = new ArrayList<ValueRange<Byte>>();
            if (numSplits == 1) {
                list.add (new ValueRange<Byte>(null, null));
            } else {
                Byte prevEnd = null;
                for (int i = 0; i < numSplits; ++i) {
                    byte nextEnd = (byte) (min + ((double) (max - min) * i / numSplits));
                    if (prevEnd == null || nextEnd != prevEnd) {
                        list.add(new ValueRange<Byte>(prevEnd, nextEnd));
                        prevEnd = nextEnd;
                    }
                }
                list.add(new ValueRange<Byte>(prevEnd, null));
            }
            return list;
        }
    }

    private static class ShortValueSplitter implements ValueSplitter<Short> {
        @Override
        public List<ValueRange<Short>> split(Short min, Short max, int numSplits) {
            List<ValueRange<Short>> list = new ArrayList<ValueRange<Short>>();
            if (numSplits == 1) {
                list.add (new ValueRange<Short>(null, null));
            } else {
                Short prevEnd = null;
                for (int i = 0; i < numSplits; ++i) {
                    short nextEnd = (short) (min + ((double) (max - min) * i / numSplits));
                    if (prevEnd == null || nextEnd != prevEnd) {
                        list.add(new ValueRange<Short>(prevEnd, nextEnd));
                        prevEnd = nextEnd;
                    }
                }
                list.add(new ValueRange<Short>(prevEnd, null));
            }
            return list;
        }
    }


    private static class IntegerValueSplitter implements ValueSplitter<Integer> {
        @Override
        public List<ValueRange<Integer>> split(Integer min, Integer max, int numSplits) {
            List<ValueRange<Integer>> list = new ArrayList<ValueRange<Integer>>();
            if (numSplits == 1) {
                list.add (new ValueRange<Integer>(null, null));
            } else {
                Integer prevEnd = null;
                for (int i = 0; i < numSplits; ++i) {
                    int nextEnd = (int) (min + ((double) (max - min) * i / numSplits));
                    if (prevEnd == null || nextEnd != prevEnd) {
                        list.add(new ValueRange<Integer>(prevEnd, nextEnd));
                        prevEnd = nextEnd;
                    }
                }
                list.add(new ValueRange<Integer>(prevEnd, null));
            }
            return list;
        }
    }

    private static class LongValueSplitter implements ValueSplitter<Long> {
        @Override
        public List<ValueRange<Long>> split(Long min, Long max, int numSplits) {
            List<ValueRange<Long>> list = new ArrayList<ValueRange<Long>>();
            if (numSplits == 1) {
                list.add (new ValueRange<Long>(null, null));
            } else {
                Long prevEnd = null;
                for (int i = 0; i < numSplits; ++i) {
                    long nextEnd = (long) (min + ((double) (max - min) * i / numSplits));
                    if (prevEnd == null || nextEnd != prevEnd) {
                        list.add(new ValueRange<Long>(prevEnd, nextEnd));
                        prevEnd = nextEnd;
                    }
                }
                list.add(new ValueRange<Long>(prevEnd, null));
            }
            return list;
        }
    }


    private static class FloatValueSplitter implements ValueSplitter<Float> {
        @Override
        public List<ValueRange<Float>> split(Float min, Float max, int numSplits) {
            List<ValueRange<Float>> list = new ArrayList<ValueRange<Float>>();
            if (numSplits == 1) {
                list.add (new ValueRange<Float>(null, null));
            } else {
                Float prevEnd = null;
                for (int i = 0; i < numSplits; ++i) {
                    float nextEnd = (float) (min + ((double) (max - min) * i / numSplits));
                    if (prevEnd == null || nextEnd != prevEnd) {
                        list.add(new ValueRange<Float>(prevEnd, nextEnd));
                        prevEnd = nextEnd;
                    }
                }
                list.add(new ValueRange<Float>(prevEnd, null));
            }
            return list;
        }
    }

    private static class DoubleValueSplitter implements ValueSplitter<Double> {
        @Override
        public List<ValueRange<Double>> split(Double min, Double max, int numSplits) {
            List<ValueRange<Double>> list = new ArrayList<ValueRange<Double>>();
            if (numSplits == 1) {
                list.add (new ValueRange<Double>(null, null));
            } else {
                Double prevEnd = null;
                for (int i = 0; i < numSplits; ++i) {
                    double nextEnd = (double) (min + ((double) (max - min) * i / numSplits));
                    if (prevEnd == null || nextEnd != prevEnd) {
                        list.add(new ValueRange<Double>(prevEnd, nextEnd));
                        prevEnd = nextEnd;
                    }
                }
                list.add(new ValueRange<Double>(prevEnd, null));
            }
            return list;
        }
    }

    /** this one is tricky... */
    private static class StringValueSplitter implements ValueSplitter<String> {
        @Override
        public List<ValueRange<String>> split(String min, String max, int numSplits) {
            List<ValueRange<String>> list = new ArrayList<ValueRange<String>>();
            if (numSplits == 1) {
                list.add (new ValueRange<String>(null, null));
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
                    list.add(new ValueRange<String>(prevEnd == null ? null : prefix + prevEnd, prefix + nextEnd));
                    prevEnd = nextEnd;
                }
            }
            list.add(new ValueRange<String>(prefix + prevEnd, null));
            return list;
        }
    }
}

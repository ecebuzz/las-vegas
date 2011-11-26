package edu.brown.lasvegas.lvfs.local;

import java.io.File;
import java.util.Random;

import edu.brown.lasvegas.CompressionType;

/**
 * Tests the performance of bulk reading and writing with block compression.
 * This is not a testcase.
 * This benchmark tests the pure overhead of compression/decompression.
 * So, it uses little-compressible (random) values.
 */
public class ReaderWriterBlockCompressionBenchmark {
    public static void main(String[] args) throws Exception {
        CompressionType compType;
        boolean write = false;
        boolean write_sync = false;
        boolean read = false;
        if (args.length < 2) {
            System.out.println("args: <snappy/gzip> <writeonly/writeonly_sync/readonly/all>");
            System.out.println("default: snappy all");
            compType = CompressionType.SNAPPY;
            write = true;
            write_sync = true;
            read = true;
        } else {
            if (args[0].equalsIgnoreCase("snappy")) {
                compType = CompressionType.SNAPPY;
            } else if (args[0].equalsIgnoreCase("gzip")) {
                compType = CompressionType.GZIP_BEST_COMPRESSION;
            } else {
                System.err.println("compression type??:" + args[0]);
                return;
            }
            
            if (args[1].equalsIgnoreCase("writeonly")) {
                write = true;
            } else if (args[1].equalsIgnoreCase("writeonly_sync")) {
                write = true;
                write_sync = true;
            } else if (args[1].equalsIgnoreCase("readonly")) {
                read = true;
            } else if (args[1].equalsIgnoreCase("all")) {
                write = true;
                write_sync = true;
                read = true;
            } else {
                System.err.println("<writeonly/writeonly_sync/readonly/all>");
                return;
            }
        }
        
        
        File file = new File("test/local/bench.bin");
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + file.getParentFile().getAbsolutePath());
        }
        boolean longRun = false;
        if (args.length >= 3) {
            longRun = args[2].equalsIgnoreCase("longrun");
        }
        System.out.println("long run=" + longRun);
        short[] buf = new short[1 << 17];
        Random random = new Random(123456L) ; // fixed seed
        for (int j = 0; j < buf.length; ++j) {
            if (longRun) {
                buf[j] = (j % 128 == 0 ? (short) random.nextInt() : buf[j - 1]);
            } else {
                buf[j] = (short) random.nextInt();
            }
        }
        {
            // JVM warm-up
            File dummy = new File("test/local/dummy.bin");
            for (int rep = 0; rep < 3; ++rep) {
                {
                    System.currentTimeMillis();
                    LocalBlockCompressionFixLenWriter<Short, short[]> writer = LocalBlockCompressionFixLenWriter.getInstanceSmallint(dummy, compType);
                    for (int i = 0; i < 20; ++i) {
                        writer.writeValues(buf, 0, buf.length);
                    }
                    writer.writeFileFooter();
                    writer.flush();
                    writer.close();
                }
                {
                    LocalBlockCompressionFixLenReader<Short, short[]> reader = LocalBlockCompressionFixLenReader.getInstanceSmallint(dummy, compType);
                    for (int i = 0; i < 20; ++i) {
                        reader.readValues(buf, 0, buf.length);
                    }
                    reader.close();        
                }
            }
        }
        if (write) {
            long startTime = System.currentTimeMillis();
            LocalBlockCompressionFixLenWriter<Short, short[]> writer = LocalBlockCompressionFixLenWriter.getInstanceSmallint(file, compType);
            for (int i = 0; i < 1024; ++i) {
                writer.writeValues(buf, 0, buf.length);
            }
            writer.writeFileFooter();
            writer.flush(write_sync);
            writer.close();
            long endTime = System.currentTimeMillis();
            System.out.println("wrote " + (file.length() >> 20) + "MB in " + (endTime - startTime) + "ms");
        }

        if (read) {
            long startTime = System.currentTimeMillis();
            LocalBlockCompressionFixLenReader<Short, short[]> reader = LocalBlockCompressionFixLenReader.getInstanceSmallint(file, compType);
            for (int i = 0; i < 1024; ++i) {
                reader.readValues(buf, 0, buf.length);
            }
            reader.close();        
            long endTime = System.currentTimeMillis();
            System.out.println("read " + (file.length() >> 20) + "MB in " + (endTime - startTime) + "ms");
        }
    }
}

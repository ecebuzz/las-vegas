package edu.brown.lasvegas.lvfs.local;

import java.io.File;

/**
 * Tests the performance of bulk reading and writing for RLE.
 * This is not a testcase.
 * This benchmark tests the pure overhead of compression/decompression.
 * So, it uses no-compressible (no run length) values.
 */
public class ReaderWriterRLEBenchmark {
    public static void main(String[] args) throws Exception {
        File file = new File("test/local/bench.bin");
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + file.getParentFile().getAbsolutePath());
        }
        boolean longRun = false;
        if (args.length >= 2) {
            longRun = args[1].equalsIgnoreCase("longrun");
        }
        System.out.println("long run=" + longRun);
        int[] buf = new int[1 << 16];
        for (int j = 0; j < buf.length; ++j) {
            if (longRun) {
                buf[j] = j / 128; // long run
            } else {
                buf[j] = (int) j; // no runs
            }
        }
        {
            // JVM warm-up
            File dummy = new File("test/local/dummy.bin");
            for (int rep = 0; rep < 3; ++rep) {
                {
                    System.currentTimeMillis();
                    LocalRLEWriter<Integer, int[]> writer = LocalRLEWriter.getInstanceInteger(dummy);
                    for (int i = 0; i < 20; ++i) {
                        writer.writeValues(buf, 0, buf.length);
                    }
                    writer.writeFileFooter();
                    writer.flush();
                    writer.close();
                }
                {
                    LocalRLEReader<Integer, int[]> reader = LocalRLEReader.getInstanceInteger(dummy);
                    for (int i = 0; i < 20; ++i) {
                        reader.readValues(buf, 0, buf.length);
                    }
                    reader.close();        
                }
            }
        }
        if (args.length == 0 || args[0].equalsIgnoreCase("writeonly") || args[0].equalsIgnoreCase("writeonly_sync")) {
            long startTime = System.currentTimeMillis();
            LocalRLEWriter<Integer, int[]> writer = LocalRLEWriter.getInstanceInteger(file);
            for (int i = 0; i < 512; ++i) {
                writer.writeValues(buf, 0, buf.length);
            }
            writer.writeFileFooter();
            writer.flush(args.length == 0 || args[0].equalsIgnoreCase("writeonly_sync"));
            writer.close();
            long endTime = System.currentTimeMillis();
            System.out.println("wrote " + (file.length() >> 20) + "MB in " + (endTime - startTime) + "ms");
        }

        if (args.length == 0 || args[0].equalsIgnoreCase("readonly")) {
            long startTime = System.currentTimeMillis();
            LocalRLEReader<Integer, int[]> reader = LocalRLEReader.getInstanceInteger(file);
            for (int i = 0; i < 512; ++i) {
                reader.readValues(buf, 0, buf.length);
            }
            reader.close();        
            long endTime = System.currentTimeMillis();
            System.out.println("read " + (file.length() >> 20) + "MB in " + (endTime - startTime) + "ms");
        }
    }
}

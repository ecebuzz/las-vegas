package edu.brown.lasvegas.lvfs.local;

import java.io.File;

/**
 * Tests the performance of bulk reading and writing for variable-length string.
 * This is not a testcase.
 */
public class ReaderWriterStringBenchmark {
    public static void main(String[] args) throws Exception {
        File file = new File("test/local/string");
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new Exception ("Couldn't create test directory " + file.getParentFile().getAbsolutePath());
        }
        //1 byte length-size, 1 byte length, 14 bytes string = 16 bytes per value
        String[] buf = new String[1 << 16]; // 64K * 16 = 1M bytes
        for (int j = 0; j < buf.length; ++j) {
            buf[j] = "string--" + String.format("%06d", j);
        }
        {
            // JVM warm-up
            File dummy = new File("test/local/string.bin");
            for (int rep = 0; rep < 3; ++rep) {
                {
                    System.currentTimeMillis();
                    LocalVarLenWriter<String> writer = LocalVarLenWriter.getInstanceVarchar(dummy, 1 << 10);
                    for (int i = 0; i < 20; ++i) {
                        writer.writeValues(buf, 0, buf.length);
                    }
                    writer.writeFileFooter();
                    writer.flush();
                    writer.close();
                }
                {
                    LocalVarLenReader<String> reader = LocalVarLenReader.getInstanceVarchar(dummy);
                    for (int i = 0; i < 20; ++i) {
                        reader.readValues(buf, 0, buf.length);
                    }
                    reader.close();        
                }
            }
        }
        if (args.length == 0 || args[0].equalsIgnoreCase("writeonly") || args[0].equalsIgnoreCase("writeonly_sync")) {
            long startTime = System.currentTimeMillis();
            LocalVarLenWriter<String> writer = LocalVarLenWriter.getInstanceVarchar(file, 1 << 10);
            for (int i = 0; i < 256; ++i) {
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
            LocalVarLenReader<String> reader = LocalVarLenReader.getInstanceVarchar(file);
            for (int i = 0; i < 256; ++i) {
                reader.readValues(buf, 0, buf.length);
            }
            reader.close();        
            long endTime = System.currentTimeMillis();
            System.out.println("read " + (file.length() >> 20) + "MB in " + (endTime - startTime) + "ms");
        }
    }
}

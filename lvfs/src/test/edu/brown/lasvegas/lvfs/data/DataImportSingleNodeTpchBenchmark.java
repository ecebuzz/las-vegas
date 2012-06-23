package edu.brown.lasvegas.lvfs.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.log4j.Logger;

/**
 * TPCH version.
 * This is NOT a testcase.
 */
public class DataImportSingleNodeTpchBenchmark extends DataImportTpchBenchmark {
    private SingleNodeBenchmarkResources resources;
    private static final Logger LOG = Logger.getLogger(DataImportSingleNodeTpchBenchmark.class);

    private static File[] createFileArray (int count, String basename) {
        File[] files = new File[count];
        for (int i = 0; i < count; ++i) {
            files[i] = new File (basename + "." + (i + 1));
        }
        return files;
    }
    /*
    private static final File[] lineitemFiles = new File[]{new File ("../tpch-dbgen/s2/lineitem.tbl.1"), new File ("../tpch-dbgen/s2/lineitem.tbl.2")};
    private static final File[] partFiles = new File[]{new File ("../tpch-dbgen/s2/part.tbl.1"), new File ("../tpch-dbgen/s2/part.tbl.2")};
    private static final File[] supplierFiles = new File[]{new File ("../tpch-dbgen/s2/supplier.tbl.1"), new File ("../tpch-dbgen/s2/supplier.tbl.2")};
    private static final File[] ordersFiles = new File[]{new File ("../tpch-dbgen/s2/orders.tbl.1"), new File ("../tpch-dbgen/s2/orders.tbl.2")};
    private static final File[] customerFiles = new File[]{new File ("../tpch-dbgen/s2/customer.tbl.1"), new File ("../tpch-dbgen/s2/customer.tbl.2")};
    private static final int partitionCount = 2;
    private static final int fractureCount = 2;
    */

    // for i in {1..10}; do ./dbgen -T o -s 1 -C 10 -S $i; done
    // for i in {1..10}; do ./dbgen -T c -s 1 -C 10 -S $i; done
    // for i in {1..10}; do ./dbgen -T P -s 1 -C 10 -S $i; done
    // for i in {1..10}; do ./dbgen -T s -s 1 -C 10 -S $i; done
    private static final int partitionCount = 10;
    private static final int fractureCount = 10;
    private static final int fileCount = 10; // file count must be multiply of fractureCount
    private static final String tblFolder = "../tpch-dbgen/s1_10";

    private static final File[] lineitemFiles;
    private static final File[] partFiles;
    private static final File[] supplierFiles;
    private static final File[] ordersFiles;
    private static final File[] customerFiles;
    static {
        lineitemFiles = createFileArray(fileCount, tblFolder + "/lineitem.tbl");
        partFiles = createFileArray(fileCount, tblFolder + "/part.tbl");
        supplierFiles = createFileArray(fileCount, tblFolder + "/supplier.tbl");
        ordersFiles = createFileArray(fileCount, tblFolder + "/orders.tbl");
        customerFiles = createFileArray(fileCount, tblFolder + "/customer.tbl");
    }

    /*
    private static final File[] lineitemFiles = new File[]{new File ("../tpch-dbgen/lineitem.tbl")};
    private static final File[] partFiles = new File[]{new File ("../tpch-dbgen/part.tbl")};
    private static final File[] supplierFiles = new File[]{new File ("../tpch-dbgen/supplier.tbl")};
    private static final File[] ordersFiles = new File[]{new File ("../tpch-dbgen/orders.tbl")};
    private static final File[] customerFiles = new File[]{new File ("../tpch-dbgen/customer.tbl")};
    private static final int partitionCount = 2;
    private static final int fractureCount = 2;
    */
    
/*
    // just for testing
    private static final File[] lineitemFiles = new File[]{new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_lineitem.tbl")};
    private static final File[] partFiles = new File[]{new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_part.tbl")};
    private static final File[] ordersFiles = new File[]{new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_orders.tbl")};
    private static final File[] customerFiles = new File[]{new File ("src/test/edu/brown/lasvegas/lvfs/data/mini_tpch_customer.tbl")};
    private static final int partitionCount = 1;
*/
    DataImportSingleNodeTpchBenchmark (SingleNodeBenchmarkResources resources) throws IOException {
        super(resources.metaRepo, partitionCount, fractureCount);
        this.resources = resources;
    }
    protected void tearDown () throws IOException {
        resources.tearDown();
    }
    void exec () throws Exception {
        InputFile lineitemInputFile = new InputFile("lineitem", lineitemFiles);
        InputFile partInputFile = new InputFile("part", partFiles);
        InputFile supplierInputFile = new InputFile("supplier", supplierFiles);
        InputFile customerInputFile = new InputFile("customer", customerFiles);
        InputFile ordersInputFile = new InputFile("orders", ordersFiles);
        super.exec(lineitemInputFile.path(), partInputFile.path(), supplierInputFile.path(), customerInputFile.path(), ordersInputFile.path());
        lineitemInputFile.delete();
        partInputFile.delete();
        customerInputFile.delete();
        ordersInputFile.delete();
    }
    private class InputFile {
        InputFile (String name, File[] files) throws IOException {
            this.inputFile = new File (SingleNodeBenchmarkResources.LVFS_ROOT, name + "_list.txt");
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(inputFile), "UTF-8"));
            for (File file : files) {
                writer.write(resources.nodes[0][0].getName() + "\t" + file.getAbsolutePath());
                writer.newLine();
            }
            writer.flush();
            writer.close();
        }
        void delete () throws IOException {
            if (inputFile.exists()) {
                inputFile.delete();
            }
        }
        String path () { return inputFile.getAbsolutePath(); }
        @Override
        protected void finalize() throws Throwable {
            delete ();
        }
        private final File inputFile;
    }
    
    public static void main (String[] args) throws Exception {
        LOG.info("running a single node experiment..");
        for (File[] files : new File[][]{lineitemFiles, partFiles, ordersFiles, customerFiles}) {
            for (File file : files) {
                if (!file.exists()) {
                    throw new FileNotFoundException(file.getAbsolutePath() + " doesn't exist. Have you generated the data?");
                }
            }
        }

        SingleNodeBenchmarkResources resources = new SingleNodeBenchmarkResources(1, 1, 0); // 0 databases because the benchmark creates one itself
        DataImportSingleNodeTpchBenchmark program = new DataImportSingleNodeTpchBenchmark(resources);
        program.setUp();
        program.exec();
    }
}

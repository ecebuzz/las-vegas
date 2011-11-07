package edu.brown.lasvegas.toy;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

/**
 * Sorts a tbl file and also pads the end of lines to make sure no line spans
 * two HDFS blocks (64MB).
 */
public class SortTblFile {
    private static Logger LOG = Logger.getLogger(SortTblFile.class);
    /*
     * ddl.append("CREATE TABLE lineorder(lo_orderkey INT NOT NULL,"); //0
     * ddl.append("lo_linenumber SMALLINT NOT NULL,");//1
     * ddl.append("lo_custkey INT NOT NULL,");//2
     * ddl.append("lo_partkey INT NOT NULL,");//3
     * ddl.append("lo_suppkey INT NOT NULL,");//4
     * ddl.append("lo_orderdate INT NOT NULL,");//5
     * ddl.append("lo_orderpriority CHAR(15) NOT NULL,");//6
     * ddl.append("lo_shippriority CHAR(1) NOT NULL,");//7
     * ddl.append("lo_quantity SMALLINT NOT NULL,");//8
     * ddl.append("lo_extendedprice BIGINT NOT NULL,");//9
     * ddl.append("lo_ordertotalprice INT NOT NULL,");//10
     * ddl.append("lo_discount TINYINT NOT NULL,");//11
     * ddl.append("lo_revenue BIGINT NOT NULL,");//12
     * ddl.append("lo_supplycost INT NOT NULL,");//13
     * ddl.append("lo_tax TINYINT NOT NULL,");//14
     * ddl.append("lo_commitdate INT NOT NULL,");//15
     * ddl.append("lo_shipmode CHAR(10) NOT NULL");//16 ddl.append(")");
     */
    private static int SORT_COL = 5;
    private static int SORT_BUCKET = 1; // >1 to bucket each sorted run (2-level sorting).
    private static File ORG_FILE = new File("/home/hkimura/samba/lineorder.tbl");
    private static File SORT_TMP_DIR = new File("/media/datavol/sort-tmp");
    private static File OUT_FILE = new File(SORT_TMP_DIR, "lineorder_sorted_" + SORT_COL + ".tbl");

    public static void main(String[] args) throws Exception {
        LOG.info("starting sort-col=" + SORT_COL + ".");
        if (!ORG_FILE.exists()) {
            LOG.error(ORG_FILE.getAbsolutePath() + " doesn't exist.");
            return;
        }
        if (!SORT_TMP_DIR.exists()) {
            LOG.error(SORT_TMP_DIR.getAbsolutePath() + " doesn't exist.");
            return;
        }
        if (OUT_FILE.exists()) {
            LOG.info("Deleted existing file:" + OUT_FILE.getAbsolutePath());
            OUT_FILE.delete();
        }

        // first, output each block
        SortedMap<Integer, TmpBlock> blocks = outputBucketFiles(ORG_FILE);
        if (SORT_BUCKET > 1) {
            LOG.info("2-level sort: sorting bucket files...");
            // if it's two-level sorting, sort in each bucket
            for (TmpBlock block : blocks.values()) {
                sortBucketFile(block);
                if (block.key % 20 == 0) {
                    LOG.info("sorted " + block.key);
                }
            }
        }

        // then, merge all blocks into one file.
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(OUT_FILE), 1 << 24);
        LOG.info("merging all bucket files...");
        HdfsBlock hdfsBlock = new HdfsBlock();
        for (TmpBlock block : blocks.values()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(block.file), "UTF-8"), 1 << 24);
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                String[] data = line.split("\\|");
                int key = Integer.parseInt(data[SORT_COL]);
                hdfsBlock.append(line, key);
                if (hdfsBlock.shouldFlush()) {
                    hdfsBlock.flushBlock(out);
                }
            }
            reader.close();
            block.file.delete();
            if (block.key % 20 == 0) {
                LOG.info("merged " + block.key);
            }
        }
        hdfsBlock.finalFlushBlock (out);
        out.flush();
        out.close();
        LOG.info("merging all done!.");
    }
    
    private static SortedMap<Integer, TmpBlock> outputBucketFiles(File inFile) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inFile), "UTF-8"), 1 << 24);
        LOG.info("Bucketing the input file...");
        SortedMap<Integer, TmpBlock> blocks = new TreeMap<Integer, TmpBlock>();
        int cnt = 0, next_cnt = 100000;
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            if (line.endsWith("|")) {
                line = line.substring(0, line.length() - 1); // trim the last | (seen in TPCH, not SSB)
            }
            String[] data = line.split("\\|");
            int key = Integer.parseInt(data[SORT_COL]) / SORT_BUCKET;
            TmpBlock block = blocks.get(key);
            if (block == null) {
                block = new TmpBlock(key);
                blocks.put(key, block);
            }
            block.writer.write(line);
            block.writer.newLine();
            if (++cnt >= next_cnt) {
                LOG.info("processed " + cnt + "lines");
                next_cnt *= 2;
            }
        }
        reader.close();
        for (TmpBlock block : blocks.values()) {
            block.closeWriter();
        }
        LOG.info("written " + blocks.size() + " temp files.");
        return blocks;
    }
    private static void sortBucketFile(TmpBlock block) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(block.file), "UTF-8"), 1 << 24);
        SortedMap<Integer, ArrayList<String>> sorted = new TreeMap<Integer, ArrayList<String>>();
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            String[] data = line.split("|");
            int key = Integer.parseInt(data[SORT_COL]);
            ArrayList<String> lines = sorted.get(key);
            if (lines == null) {
                lines = new ArrayList<String>();
                sorted.put(key, lines);
            }
            lines.add(line);
        }
        reader.close();
        block.file.delete();
        BufferedWriter writer = new BufferedWriter(new FileWriter(block.file), 1 << 22);
        for (ArrayList<String> lines : sorted.values()) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        }
        writer.flush();
        writer.close();
    }

    private static class TmpBlock {
        private TmpBlock(int key) throws IOException {
            this.key = key;
            file = new File(SORT_TMP_DIR, "/temp_" + key + ".tbl");
            if (file.exists())
                file.delete();
            if (key % 40 == 0) {
                LVUtil.outputMemory();
            }
            writer = new BufferedWriter(new FileWriter(file), 2 << 17);
        }

        private void closeWriter() throws IOException {
            writer.flush();
            writer.close();
            writer = null;
        }

        final int key;
        File file;
        BufferedWriter writer;
    }

    private static class HdfsBlock {
        static final byte[] LF = "\r\n".getBytes();
        static final int LF_BYTES = LF.length;
        static final int BLOCK_SIZE = 64 << 20;

        private void updateMinMax(int new_key) {
            max_key = Math.max(max_key, new_key);
            min_key = Math.min(min_key, new_key);
            ++count;
        }
        
        private void append (String line, int key) {
            if (cur_bytes != 0) {
                System.arraycopy(LF, 0, buffer, cur_bytes, LF_BYTES);
                cur_bytes += LF_BYTES;
            }
            byte[] bytes = line.getBytes();
            System.arraycopy(bytes, 0, buffer, cur_bytes, bytes.length);
            cur_bytes += line.length();
            updateMinMax(key);
        }
        private boolean shouldFlush () {
            return (BLOCK_SIZE - cur_bytes < 300);
        }
        private byte[] makeHeader() {
            String header = "#" + SORT_COL + "|"+min_key+"|"+max_key+"|"+count;
            return header.getBytes();
        }
        private void flushBlock (OutputStream out) throws IOException {
            byte[] header_bytes = makeHeader();
            out.write(header_bytes);
            out.write(LF);
            out.write(buffer, 0, cur_bytes);
            
            int pad_bytes = BLOCK_SIZE - header_bytes.length - LF_BYTES - cur_bytes - LF_BYTES;
            LOG.info("end of HDFS block. padded " + pad_bytes + " bytes");
            char[] padding = new char[pad_bytes];
            Arrays.fill(padding, ' ');
            byte[] padding_bytes = new String(padding).getBytes();
            assert(padding_bytes.length == pad_bytes);
            out.write(padding_bytes);
            out.write(LF);

            cur_bytes = 0;
            count = 0;
            max_key = Integer.MIN_VALUE;
            min_key = Integer.MAX_VALUE;
        }
        private void finalFlushBlock (OutputStream out) throws IOException {
            if (cur_bytes > 0) {
                byte[] header_bytes = makeHeader();
                out.write(header_bytes);
                out.write(LF);
                out.write(buffer, 0, cur_bytes);
                out.write(LF);
                cur_bytes = 0;
            }
        }

        // max/min/count key (not bucketed) in this block
        int max_key = Integer.MIN_VALUE;
        int min_key = Integer.MAX_VALUE;
        int count = 0;
        int cur_bytes = 0;
        byte[] buffer = new byte[BLOCK_SIZE];
    }
}

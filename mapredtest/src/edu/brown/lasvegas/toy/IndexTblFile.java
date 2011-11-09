package edu.brown.lasvegas.toy;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;

import org.apache.log4j.Logger;

/**
 * Scan a sorted tbl file and create an index file
 * which maintains some number of pointers to locations in the file. 
 * The format of an index file is:
 * key1 pointer1 (e.g., 1 20)  // this is always first row (doesn't start at 0 because of header)
 * key2 pointer2 (e.g., 10 122)
 * key3 pointer3 (e.g., 30 333)
 * ...
 * So far assumes int key.
 */
public class IndexTblFile {
    private static Logger LOG = Logger.getLogger(IndexTblFile.class);
    private static File SORT_TMP_DIR = SortTblFile.SORT_TMP_DIR;
    private static File SORTED_FILE = new File(SORT_TMP_DIR, "part_pk.tbl");
    private static File INDEX_FILE = new File(SORT_TMP_DIR, SORTED_FILE.getName() + ".idx");
    private static int SORT_COL = SortTblFile.SORT_COL;
    
    /** number of pointers per block. */
    private static final int NUM_POINTERS = 256;
    
    private static final int BLOCK_SIZE = SortTblFile.BLOCK_SIZE;
    
    private static final byte LF = "\n".getBytes()[0]; // \r\n -> \n is the end. \n -> \n is the end too. 
    private static final byte CR = "\r".getBytes()[0]; 
    public static void main(String[] args) throws Exception {
        byte[] buffer = new byte[BLOCK_SIZE];
        FileInputStream in = new FileInputStream(SORTED_FILE);
        BufferedWriter writer = new BufferedWriter(new FileWriter(INDEX_FILE), 16 << 20);

        long location = 0;
        while (true) {
            LOG.info("Reading a block...");
            int readBytes = in.read(buffer);
            if (readBytes <= 0) break;
            LOG.info("block size=" + readBytes);
            int headerEndsAt;
            for (headerEndsAt = 0; headerEndsAt < readBytes; ++headerEndsAt) {
                if (buffer[headerEndsAt] == LF) break;
            }
            String header = extractLine(buffer, 0, headerEndsAt);
            LOG.info("header=" + header);
            assert(header.startsWith("#"));
            String[] headerData = header.split("\\|");
            int blockRowCount = Integer.parseInt(headerData[headerData.length - 1]);
            int rowsPerPointer = (blockRowCount / NUM_POINTERS) + 1;
            LOG.info("blockRowCount=" + blockRowCount + ", rows-per-pointer=" + rowsPerPointer);

            int prevKey = Integer.MIN_VALUE;
            int rowCounter = 0;
            for (int pos = headerEndsAt + 1; pos < readBytes;) {
                int nextPos;
                for (nextPos = pos; buffer[nextPos] != LF; ++nextPos);
                if (rowCounter % rowsPerPointer == 0) {
                    String line = extractLine(buffer, pos, nextPos);
                    String[] lineData = line.split("\\|");
                    int key = Integer.parseInt(lineData[SORT_COL]);
                    assert (key >= prevKey);
                    if (key > prevKey) {
                        prevKey = key;
                        LOG.debug(key + " " + (location + pos));
                        writer.write(key + " " + (location + pos));
                        writer.newLine();
                    }
                }
                ++rowCounter;
                pos = nextPos + 1;
            }
            
            location += readBytes;
        }
        writer.flush();
        writer.close();
        LOG.info("Finished.");
    }
    private static boolean endsWithCrlf(byte[] buffer, int lfPos) {
        return lfPos >= 1 && buffer[lfPos - 1] == CR;
    }
    private static String extractLine(byte[] buffer, int start, int end) {
        int lineEnd = endsWithCrlf(buffer, end) ? end - 1 : end;
        return new String(buffer, start, lineEnd - start);
    }
}

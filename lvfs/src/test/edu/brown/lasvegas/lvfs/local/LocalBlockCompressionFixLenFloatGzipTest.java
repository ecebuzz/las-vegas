package edu.brown.lasvegas.lvfs.local;

import edu.brown.lasvegas.CompressionType;

public class LocalBlockCompressionFixLenFloatGzipTest extends LocalBlockCompressionFixLenFloatTest {
    @Override
    protected CompressionType getType() {
        return CompressionType.GZIP_BEST_COMPRESSION;
    }
}

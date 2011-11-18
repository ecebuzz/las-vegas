package edu.brown.lasvegas.util;

import org.apache.log4j.Logger;

/**
 * JVM memory-related utility methods.
 */
public final class MemoryUtil {
    private static Logger LOG = Logger.getLogger(MemoryUtil.class);

    public static void outputMemory() {
        long maxMemory = Runtime.getRuntime().maxMemory();
        long allocatedMemory = Runtime.getRuntime().totalMemory();
        long freeMemory = Runtime.getRuntime().freeMemory();
        LOG.info("Memory " + (maxMemory / 1000000L) + " allocated MB, " + (allocatedMemory / 1000000L) + " max MB, "
                        + ((freeMemory + (maxMemory - allocatedMemory)) / 1000000L) + " free MB");
    }
}

package edu.brown.lasvegas.lvfs.data.task;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.lvfs.data.DataTaskRunner;

/**
 * Flush the OS's disk cache at each data node. Used while benchmarks.
 * This might internally use /proc/sys/vm/drop_caches (which requires root permission) or just read large files.
 */
public final class DiskCacheFlushTaskRunner extends DataTaskRunner<DiskCacheFlushTaskParameters> {
    private static Logger LOG = Logger.getLogger(DiskCacheFlushTaskRunner.class);

    /** consume error or stdout from the process. */
    private static class PipeThread extends Thread {
    	PipeThread (InputStream in, String label) throws IOException {
    		this.reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
    		this.label = label;
    	}
    	@Override
    	public void run() {
    		try {
	    		try {
	    			while (true) {
	    				String line = reader.readLine();
	    				if (line == null) {
	    					break;
	    				}
		    			LOG.info("drop_cache process:" + label + ":" + line);
	    			}
	    		} finally {
	    			LOG.info("finished reading:" + label);
    				reader.close();
	    		}
    		} catch (IOException ex) {
    			LOG.info("exception in reader:" + label, ex);
			}
    	}
    	private final BufferedReader reader;
    	private final String label;
    }
    
	@Override
	protected String[] runDataTask() throws Exception {
		if (parameters.isUseDropCaches()) {
			LOG.info("flushing disk cache with drop_caches...");
			Process proc = Runtime.getRuntime().exec("sudo /sbin/sysctl vm.drop_caches=3"); // same as 'echo 3 > /proc/sys/vm/drop_caches'
			PipeThread stderrThread = new PipeThread(proc.getErrorStream(), "stderr");
			PipeThread stdoutThread = new PipeThread(proc.getInputStream(), "stdout");
			stderrThread.start();
			stdoutThread.start();
			for (int i = 0; i < 100; ++i) {
				Thread.sleep(100);
				try {
					int e = proc.exitValue();
					LOG.info("drop_cache process has terminated. exit_value=" + e);
					break;
				} catch (IllegalThreadStateException ex) {
				}
			}
			proc.destroy();
			LOG.info("flushed disk cache with drop_caches.");
			return new String[0];
		}

		File file = new File(parameters.getPath());
		if (!file.exists()) {
			throw new FileNotFoundException(file.getAbsolutePath());
		}
		byte[] buffer = new byte[1 << 24];
		LOG.info("reading " + file.getAbsolutePath() + " (" + file.length() + " bytes) to flood disk cache");
		FileInputStream in = new FileInputStream(file);
		try {
			long totalRead = 0;
			for (int cnt = 0;; ++cnt) {
				int read = in.read(buffer);
				if (read < 0) {
					break;
				}
				totalRead += read;
				if (cnt % 10 == 0) {
					LOG.info("read " + totalRead + " bytes.");
				}
			}
			LOG.info("done.");
		} finally {
			in.close();
		}
		return new String[0];
	}
}

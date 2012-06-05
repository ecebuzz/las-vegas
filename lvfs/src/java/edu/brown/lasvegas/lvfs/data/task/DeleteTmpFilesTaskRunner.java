package edu.brown.lasvegas.lvfs.data.task;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.lvfs.data.DataTaskRunner;

/**
 * Sub task of a few jobs.
 * Physically delete temporary files/folders in the node.
 */
public final class DeleteTmpFilesTaskRunner extends DataTaskRunner<DeleteTmpFilesTaskParameters> {
    private static Logger LOG = Logger.getLogger(DeleteTmpFilesTaskRunner.class);
    @Override
    protected String[] runDataTask() throws Exception {
        LOG.info("deleting " + parameters.getPaths().length + " files/folders..");
        for (String path : parameters.getPaths()) {
            deleteAndLog(path);
        }
        LOG.info("done!");
        return new String[0];
    }
    private void deleteAndLog (String path) throws IOException {
        if (path == null) {
            return;
        }
        File file = new File(path);
        if (!file.exists()) {
            LOG.warn("This file doesn't exist in this node:" + file.getAbsolutePath() + ".");
            return;
        }
        if (!file.getAbsolutePath().startsWith(context.localLvfsTmpDir.getAbsolutePath())) {
            LOG.warn("This file (" + file.getAbsolutePath() + ") doesn't seem in the temporary folder("
                + context.localLvfsTmpDir.getAbsolutePath() + "). Ignored. This task never deletes any files outside temporary folders.");
        }
        boolean deleted;
        if (file.isDirectory()) {
            deleted = deleteFileRecursive(file);
        } else {
            deleted = file.delete();
        }
        if (!deleted) {
            LOG.warn("couldn't delete this file/folder:" + file.getAbsolutePath() + ".");
        }
    }
    private boolean deleteFileRecursive (File dir) throws IOException {
        if (!dir.isDirectory()) {
            return dir.delete();
        }
        for (File child : dir.listFiles()) {
            if (!deleteFileRecursive(child)) {
                return false;
            }
        }
        return dir.delete();
    }
}

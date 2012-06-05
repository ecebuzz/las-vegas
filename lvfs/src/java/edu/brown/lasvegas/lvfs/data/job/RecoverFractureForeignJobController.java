package edu.brown.lasvegas.lvfs.data.job;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.AbstractJobController;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * A job to recover all files of a replica scheme from another replica scheme
 * in a different group, which requires re-partitioning.
 * 
 * This type of recovery goes as follows.
 * <ul>
 * <li>Repartition an intact replica scheme. </li>
 * <li>In the damaged replica scheme, collect the corresponding repartitioned files and save it in each node. </li>
 * <li>Merge and sort the repartitioned files to recover the damaged partitions. </li>
 * </ul>
 */ 
public class RecoverFractureForeignJobController extends AbstractJobController<RecoverFractureForeignJobParameters> {
    public RecoverFractureForeignJobController(LVMetadataProtocol metaRepo) throws IOException {
        super(metaRepo);
    }
    public RecoverFractureForeignJobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }
    private static Logger LOG = Logger.getLogger(RecoverFractureForeignJobController.class);
    @Override
    protected void initDerived() throws IOException {
        // TODO Auto-generated method stub
        
    }
    @Override
    protected void runDerived() throws IOException {
        // TODO Auto-generated method stub
        
    }
}

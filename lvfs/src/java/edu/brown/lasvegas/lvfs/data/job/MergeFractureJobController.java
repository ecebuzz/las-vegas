package edu.brown.lasvegas.lvfs.data.job;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.AbstractJobController;
import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVFracture;
import edu.brown.lasvegas.LVTable;
import edu.brown.lasvegas.lvfs.placement.PlacementEventHandlerImpl;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * The job to merge multiple fractures into one.
 */
public class MergeFractureJobController extends AbstractJobController<MergeFractureJobParameters> {
    private static Logger LOG = Logger.getLogger(MergeFractureJobController.class);
    /** fractures to be merged. */
    private LVFracture[] fractures;
    /** new fracture after merging. */
    private LVFracture newFracture;
    private LVTable table;

    public MergeFractureJobController(LVMetadataProtocol metaRepo) throws IOException {
        super(metaRepo);
    }
    public MergeFractureJobController (LVMetadataProtocol metaRepo, long stopMaxWaitMilliseconds, long taskJoinIntervalMilliseconds, long taskJoinIntervalOnErrorMilliseconds) throws IOException {
        super(metaRepo, stopMaxWaitMilliseconds, taskJoinIntervalMilliseconds, taskJoinIntervalOnErrorMilliseconds);
    }
    
    @Override
    protected void initDerived() throws IOException {
        // TODO Auto-generated method stub
        assert (param.getFractureIds() != null);
        assert (param.getFractureIds().length >= 2);
        
        String msg = "[";
        LOG.info("merging ");
        fractures = new LVFracture[param.getFractureIds().length];
        for (int i = 0; i < fractures.length; ++i) {
            fractures[i] = metaRepo.getFracture(param.getFractureIds()[i]);
            if (fractures[i] == null) {
                throw new IOException ("this fracture ID doesn't exist: " + param.getFractureIds()[i]);
            }
            if (table == null) {
                table = metaRepo.getTable(fractures[i].getTableId());
            } else {
                if (table.getTableId() != fractures[i].getTableId()) {
                    throw new IOException ("this fracture belongs to a different table:" + fractures[i]);
                }
            }
            msg += param.getFractureIds()[i] + ",";
        }
        msg += "]";
        LOG.info("merging fractures:" + msg);
        this.jobId = metaRepo.createNewJobIdOnlyReturn("merge fractures " + msg, JobType.MERGE_FRACTURE, null);
        
        this.newFracture = metaRepo.createNewFracture(table);
        assert (newFracture != null);
        new PlacementEventHandlerImpl(metaRepo).onNewFracture(newFracture);
    }
    @Override
    protected void runDerived() throws IOException {
        // TODO Auto-generated method stub
        
    }
}

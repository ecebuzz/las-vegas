package edu.brown.lasvegas.lvfs.imp;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.JobType;
import edu.brown.lasvegas.LVJob;
import edu.brown.lasvegas.lvfs.data.PartitionRawTextFilesTaskParameters;
import edu.brown.lasvegas.protocol.LVMetadataProtocol;

/**
 * The class to control data import to LVFS.
 * <p>This class merely registers a job and a bunch of sub-tasks
 * to the metadata repository and waits for data nodes to
 * complete the actual work.</p>
 * 
 * <p>
 * For example, you can use this class as following:
 * <pre>
 * LVMetadataClient metaClient = new LVMetadataClient(new Configuration());
 * LVMetadataProtocol metaRepo = metaClient.getChannel ();
 * DataImportParameters param = new DataImportParameters(123);
 * param.getNodeFilePathMap().put(11, new String[]{"/home/user/test1.txt"});
 * param.getNodeFilePathMap().put(12, new String[]{"/home/user/test2.txt"});
 * new DataImportController(metaRepo).execute(param);
 * </pre>
 * </p>
 */
public class DataImportController {
    private static Logger LOG = Logger.getLogger(DataImportController.class);

    /**
     * Metadata repository.
     */
    private LVMetadataProtocol metaRepo;
    
    public DataImportController (LVMetadataProtocol metaRepo) {
        this.metaRepo = metaRepo;
    }
    
    /**
     * Start a data import.
     * @return ID of the Job ({@link LVJob}) object created for this data import.
     */
    public int execute (DataImportParameters param) throws IOException {
        // First, create a job object for the import.
        LOG.info("importing Fracture-" + param.getFractureId());
        int jobId = metaRepo.createNewJobIdOnlyReturn("data import Fracture-" + param.getFractureId(), JobType.IMPORT_FRACTURE, null);
        
        PartitionRawTextFilesTaskParameters taskParam = new PartitionRawTextFilesTaskParameters();
        
        return jobId;
    }
}

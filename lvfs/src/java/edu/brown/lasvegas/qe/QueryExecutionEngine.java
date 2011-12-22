package edu.brown.lasvegas.qe;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.ipc.ProtocolSignature;

import edu.brown.lasvegas.lvfs.meta.MasterMetadataRepository;
import edu.brown.lasvegas.protocol.LVQueryProtocol;

/**
 * The query execution engine which runs on the central node
 * to compile, optimize, execute and monitor queries.
 */
public final class QueryExecutionEngine implements LVQueryProtocol {
    /**
     * metadata repository. Notice this is a Master repository, not an RPC proxy.
     * The query execution engine needs to access metadata in an extremely high rate,
     * so we need to avoid RPC. In future, we might try separating metadata node
     * and query optimizer node for better scalability, but it wouldn't be easy. 
     */ 
    private final MasterMetadataRepository metadata;

    /**
     * Holds information of all queries since the startup.
     */
    private final Map<Integer, QueryRepo> queries;
    private final AtomicInteger lastQueryId = new AtomicInteger(0);

    public QueryExecutionEngine (MasterMetadataRepository metadata) {
        this.metadata = metadata;
        this.queries = Collections.synchronizedMap(new HashMap<Integer, QueryRepo>());
    }
    

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion, clientMethodsHash);
    }
    
    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        if (protocol.equals(LVQueryProtocol.class.getName())) {
            return LVQueryProtocol.versionID;
        } else {
            throw new IOException("This protocol is not supported: " + protocol);
        }
    }

    @Override
    public int compile(int databaseId, String sql) throws IOException {
        int queryId = lastQueryId.incrementAndGet();
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int compile(String sql) throws IOException {
        return compile (0, sql);
    }

    @Override
    public ParsedQuery getParsedQuery(int queryId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int optimize(int queryId) throws IOException {
        return optimize (queryId, new QueryHint[0]);
    }

    @Override
    public int optimize(int queryId, QueryHint[] hints) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public QueryPlan getQueryPlan(int queryId, int planId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int execute(int queryId, int planId, TaskLogLevel logLevel) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int execute(int queryId, int planId) throws IOException {
        return execute(queryId, planId, TaskLogLevel.INFO);
    }

    @Override
    public TaskProgress joinTask(int queryId, int taskId, long millisecondsToWait) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TaskProgress getTaskProgress(int queryId, int taskId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getTaskLogLength(int queryId, int taskId) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getTaskLog(int queryId, int taskId, int offset, int len) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TaskProgress cancelTask(int queryId, int taskId) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void releaseQuery(int queryId) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        
    }
}

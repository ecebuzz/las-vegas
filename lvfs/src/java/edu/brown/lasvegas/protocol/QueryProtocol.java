package edu.brown.lasvegas.protocol;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

import edu.brown.lasvegas.qe.ParsedQuery;
import edu.brown.lasvegas.qe.QueryHint;
import edu.brown.lasvegas.qe.QueryPlan;
import edu.brown.lasvegas.qe.TaskLogLevel;
import edu.brown.lasvegas.qe.TaskProgress;

/**
 * Defines a protocol to optimize, execute and monitor queries in Las-Vegas.
 * 
 * <p>All queries are first sent to the central node to compile and optimize,
 * then tasks are distributed to each Map-Reduce node, and finally
 * results are collected from each node.</p>
 * 
 * <p>
 * Query execution consists of the following steps. You can skip some step
 * (or repeat more than once) except compilation and release.
 * <ol>
 * <li>Compile Query: {@link #compile(int, String)} to get QueryID</li>
 * <li>Optimize Plan: {@link #optimize(int)} to get PlanID</li>
 * <li>Execute Task: {@link #execute(int, int)} to get TaskID</li>
 * <li>Monitor Task Progress/Log: {@link #getTaskProgress(int, int)}, {@link #getTaskLog(int, int, int, int)}, etc.</li>
 * <li>(optional) Cancel Task: {@link #cancelTask(int, int)} to terminate a running query.</li>
 * <li>Receive Query Results: {@link #joinTask(int, int, long)} or {@link #getTaskProgress(int, int)}</li>
 * <li>Release all resources: {@link #releaseQuery(int)}</li>
 * </ol>
 * </p>
 * 
 * <p>All query plans and related objects are kept in the central node
 * until the client explicitly calls {@link #releaseQuery(int)} to revoke all resources for
 * the query. So, never forget calling it when you are done!</p>
 */
public interface QueryProtocol extends VersionedProtocol {
    /**
     * Syntactically parses the given SQL and creates a query object for it.
     * @param databaseId the context database. 0 if no context database (some SQL doesn't
     * need context database, such as CREATE DATABASE, DROP DATABASE).
     * @param sql The SQL string to parse
     * @return ID of the created query object. Use it for subsequent method calls.
     * @throws IOException if failed to parse the SQL
     */
    int compile (int databaseId, String sql) throws IOException;
    /**
     * Overload for the case where you don't need a context database.
     * @see #compile(int, String)
     */
    int compile (String sql) throws IOException;

    /**
     * Returns the syntax tree of a compiled query.
     * @param queryId ID of the compiled query
     * @return query syntax tree
     * @throws IOException
     */
    ParsedQuery getParsedQuery (int queryId) throws IOException;
    
    /**
     * Generates a query plan for the query.
     * @param queryId the query to optimize
     * @return ID of the created query plan. Use it for subsequent method calls.
     * @throws IOException
     */
    int optimize (int queryId) throws IOException;
    /**
     * Generates a query plan for the query with the given query hints.
     * @param queryId the query to optimize
     * @param hints query hints to help query planning
     * @return ID of the created query plan. Use it for subsequent method calls.
     * @throws IOException
     */
    int optimize (int queryId, QueryHint[] hints) throws IOException;

    /**
     * Returns the optimized query execution plan.
     * @param planId the plan to obtain
     * @return Details of the query plan
     * @throws IOException
     */
    QueryPlan getQueryPlan (int queryId, int planId) throws IOException;

    /**
     * Starts running the specified query with the given plan. This method immediately returns.
     * Use monitoring functions to check the progress of the query.
     * @param queryId the query to run
     * @param planId the query plan to use
     * @param logLevel level of logging for this query execution. the logged messages can be
     * obtained by {@link #getTaskLog(int, long, long)}.
     * @return ID of the query execution task. Use it for subsequent method calls.
     * @throws IOException
     */
    int execute (int queryId, int planId, TaskLogLevel logLevel) throws IOException;
    /**
     * Overload using INFO loggin level.
     * @see #execute(int, int, TaskLogLevel)
     */
    int execute (int queryId, int planId) throws IOException;
    
    /**
     * Blocks until the specified task finishes or the given time elapses. 
     * @param queryId ID of the query
     * @param taskId the task to join
     * @param millisecondsToWait the maximum time to wait
     * @return progress of the task as of returning from this method
     * @throws IOException
     */
    TaskProgress joinTask (int queryId, int taskId, long millisecondsToWait) throws IOException;

    /**
     * Returns the current progress of the specified task.
     */
    TaskProgress getTaskProgress (int queryId, int taskId) throws IOException;

    /**
     * Returns the number of unicode characters logged for the task.
     */
    int getTaskLogLength (int queryId, int taskId) throws IOException;

    /**
     * Returns the log message for the specified task.
     * @param queryId ID of the query
     * @param taskId ID of the task
     * @param offset offset in unicode characters
     * @param len maximum number of unicode characters to return
     * @return log message.
     */
    String getTaskLog (int queryId, int taskId, int offset, int len) throws IOException;
    
    /**
     * Forcibly terminates the specified task.
     * @param queryId ID of the query
     * @param taskId the task to terminate
     * @return progress as of termination
     * @throws IOException
     */
    TaskProgress cancelTask (int queryId, int taskId) throws IOException;

    /**
     * Release all objects (such as query results and query plans) for the specified query.
     * @param queryId the query to revoke
     * @throws IOException
     */
    void releaseQuery (int queryId) throws IOException;
    
    /** Releases all resources. */
    void close() throws IOException;

    public static final long versionID = 1L;
}

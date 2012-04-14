package edu.brown.lasvegas.costmodels.recovery.sim;

/**
 * Parameters for HDFS data layout.
 * It's just the replication factor. 
 */
public class HdfsPlacementParameters {
	public HdfsPlacementParameters(int replicationFactor, boolean secondReplicaInSameRack) {
		this.replicationFactor = replicationFactor;
		this.secondReplicaInSameRack = secondReplicaInSameRack;
	}
	public final int replicationFactor;
	/**
	 * Whether to put the second replica in the same rack as the first replica.
	 * The actual HDFS placement policy does it, but it's better to turn this off
	 * in terms of recoverability. Set false to this to simulate it.
	 */
	public final boolean secondReplicaInSameRack;
}

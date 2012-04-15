package edu.brown.lasvegas.costmodels.recovery.sim;

/**
 * Parameters for HDFS data layout.
 * It's just the replication factor. 
 */
public class HdfsPlacementParameters {
	public HdfsPlacementParameters(int replicationFactor) {
		this (replicationFactor, true, false);
	}
	public HdfsPlacementParameters(int replicationFactor, boolean secondReplicaInSameRack) {
		this (replicationFactor, secondReplicaInSameRack, false);
	}
	public HdfsPlacementParameters(int replicationFactor, boolean secondReplicaInSameRack, boolean stripeChunking) {
		this.replicationFactor = replicationFactor;
		this.secondReplicaInSameRack = secondReplicaInSameRack;
		this.stripeChunking = stripeChunking;
	}
	public final int replicationFactor;
	/**
	 * Whether to put the second replica in the same rack as the first replica.
	 * The actual HDFS placement policy does it, but it's better to turn this off
	 * in terms of recoverability. Set false to this to simulate it.
	 */
	public final boolean secondReplicaInSameRack;
	
	/**
	 * Turns on the stripe placement discussed in Lian et al. ICSCS'05.
	 * This is, if chunk size is very large, equivalent to our node coupling.
	 * When this flag is turned on, we increase the size of HDFS blocks to simulate its behavior.
	 */
	public final boolean stripeChunking;
	
	@Override
	public String toString() {
		return "replicationFactor=" + replicationFactor + ", secondReplicaInSameRack=" + secondReplicaInSameRack + ", stripeChunking=" + stripeChunking;
	}
}

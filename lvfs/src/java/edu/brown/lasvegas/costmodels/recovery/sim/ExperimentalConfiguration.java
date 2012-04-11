package edu.brown.lasvegas.costmodels.recovery.sim;

/**
 * Specifies the hardware configuration to simulate.
 * Immutable, thus all properties are public final.
 */
public class ExperimentalConfiguration {
	
	/**
	 * Instantiates a new experimental configuration.
	 */
	public ExperimentalConfiguration() {
		this (100, 100, 100, 100,
				4.3d * 30 * 24 * 60, 10.2d * 365 * 24 * 60,
				50.0d, 3000.0d,
				3650.0d * 24 * 60);
		// numbers are from: http://www.cs.cornell.edu/projects/ladis2009/talks/dean-keynote-ladis2009.pdf
		// and OSDI'10
	}

	/**
	 * Instantiates a new experimental configuration.
	 *
	 * @param racks the racks
	 * @param nodesPerRack the nodes per rack
	 * @param gigabytesPerNode the gigabytes per node
	 * @param tables the tables
	 * @param nodeMeanTimeToFail the node mean time to fail
	 * @param rackMeanTimeToFail the rack mean time to fail
	 * @param nodeRecoveryRateGBs the node recovery rate gb/s
	 * @param backboneNetworkGBs the backbone network gb/s
	 * @param maxSimulationPeriod the max simulation period
	 */
	public ExperimentalConfiguration(int racks, int nodesPerRack, int gigabytesPerNode, int tables,
			double nodeMeanTimeToFail, double rackMeanTimeToFail,
			double nodeRecoveryRateGBs, double backboneNetworkGBs,
			double maxSimulationPeriod) {
		this.racks = racks;
		this.nodesPerRack = nodesPerRack;
		this.nodes = racks * nodesPerRack;
		this.gigabytesPerNode = gigabytesPerNode;
		this.tables = tables;
		this.nodeMeanTimeToFail = nodeMeanTimeToFail;
		this.rackMeanTimeToFail = rackMeanTimeToFail;
		this.nodeRecoveryRateGBs = nodeRecoveryRateGBs;
		this.backboneNetworkGBs = backboneNetworkGBs;
		this.maxSimulationPeriod = maxSimulationPeriod;
	}
	
	/** number of racks in the system. */
	public final int racks;
	/** number of machines in each rack. */
	public final int nodesPerRack;
	/** total number of machines . */
	public final int nodes;
	/** amount of original data to store per node _without replication_. */
	public final int gigabytesPerNode;
	/** number of tables (files) stored in the system. */
	public final int tables;
	
	/** MTTF of a node in minutes. */
	public final double nodeMeanTimeToFail;
	
	/** MTTF of a rack in minutes. */
	public final double rackMeanTimeToFail;
	
	/** disk/network combined throughput to recover a node GB/s. */
	public final double nodeRecoveryRateGBs;
	/** network throughput (particularly inter-rack I/O) GB/s. */
	public final double backboneNetworkGBs;
	
	/** maximum length of simulation in minutes. */
	public final double maxSimulationPeriod;
	
	public int rackIdFromNodeId (int nodeId) {
		assert (nodeId >= 0);
		assert (nodeId < nodes);
		return nodeId / nodesPerRack;
	}
}

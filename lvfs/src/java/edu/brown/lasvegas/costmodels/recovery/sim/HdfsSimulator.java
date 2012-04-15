package edu.brown.lasvegas.costmodels.recovery.sim;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.costmodels.recovery.sim.FailureSchedule.FailureEvent;

/**
 * Simulator to estimate Data loss probability for HDFS.
 */
public class HdfsSimulator extends Simulator {
    private static Logger LOG = Logger.getLogger(HdfsSimulator.class);
	private final HdfsPlacementParameters policy;
	private static final double DEFAULT_BLOCK_SIZE = 1.0d / 16.0d;
	private final double BLOCK_SIZE;
	
	public HdfsSimulator(ExperimentalConfiguration config, HdfsPlacementParameters policy, long firstRandomSeed) {
		super (config, firstRandomSeed);
		this.policy = policy;
		if (policy.stripeChunking) {
			BLOCK_SIZE = config.gigabytesPerNode / (config.backboneNetwork / config.localDisk);
			// following the conclusion in Lian et al. ICSCS'05, we assign B/b stripes per node
			LOG.info ("HDFS stripe placement size=" + BLOCK_SIZE + "GB");
		} else {
			BLOCK_SIZE = DEFAULT_BLOCK_SIZE;
		}
	}

	@Override
	public void decidePlacement () {
		LOG.info("deciding data placement in HDFS...");
		// use a fixed random seed to determine the placement.
		// should be okay. we are iterating over various schedules, not placement.
		Random random = new Random(123456L);
		hdfsBlocks = (int) Math.ceil(config.gigabytesTotal / BLOCK_SIZE);
		redundancies = new byte[hdfsBlocks];
		blocksInNodes = new int[config.nodes][];
		blocksInNodesStored = new int[config.nodes];
		for (int i = 0; i < config.nodes; ++i) {
			// *1.5 to make expansion rare. but, it might happen!
			blocksInNodes[i] = new int[(hdfsBlocks / config.nodes) * 3 / 2 * policy.replicationFactor];
		}
		Arrays.fill(blocksInNodesStored, 0);
		
		boolean[] rackPicked = new boolean[config.racks]; // to avoid the same rack
		for (int block = 0; block < hdfsBlocks; ++block) {
			int firstReplicaNodeId = 0;
			int firstReplicaRackId = 0;
			Arrays.fill(rackPicked, false);
			for (int rep = 0; rep < policy.replicationFactor; ++rep) {
				int nodeId;
				if (rep == 0) {
					nodeId = random.nextInt(config.nodes);
					firstReplicaNodeId = nodeId;
					firstReplicaRackId = config.rackIdFromNodeId(firstReplicaNodeId);
				} else if (rep == 1 && policy.secondReplicaInSameRack) {
					// following the HDFS implementation, we place the second replica in the same rack
					// and the 3rd, 4th.. in other racks.
					// this is not quite optimal for recoverability, but that's what HDFS does.
					int nodeInRack = random.nextInt(config.nodesPerRack - 1); // -1 to avoid the same node
					nodeId = config.firstNodeIdFromRackId(firstReplicaRackId) + nodeInRack;
					if (nodeId >= firstReplicaNodeId) {
						++nodeId;
					}
					assert (nodeId != firstReplicaNodeId);
					assert (config.rackIdFromNodeId(nodeId) == config.rackIdFromNodeId(firstReplicaNodeId));
				} else {
					int rack;
					while (true) {
						rack = random.nextInt(config.racks);
						if (!rackPicked[rack]) {
							break;
						}
					}
					int nodeInRack = random.nextInt(config.nodesPerRack);
					nodeId = config.firstNodeIdFromRackId(rack) + nodeInRack;
				}

				rackPicked[config.rackIdFromNodeId(nodeId)] = true;
				if (blocksInNodesStored[nodeId] == blocksInNodes[nodeId].length) {
					// unluckily, we need to extend the array.
					int[] newArray = new int[blocksInNodesStored[nodeId] * 2];
					System.arraycopy(blocksInNodes[nodeId], 0, newArray, 0, blocksInNodesStored[nodeId]);
					blocksInNodes[nodeId] = newArray;
				}
				assert (blocksInNodesStored[nodeId] < blocksInNodes[nodeId].length);
				blocksInNodes[nodeId][blocksInNodesStored[nodeId]] = block;
				++blocksInNodesStored[nodeId];
			}
		}
		LOG.info("decided data placement");
	}
	private int hdfsBlocks;

	/** number of replica blocks stored in each node. */
	private int[] blocksInNodesStored;
	/** Block ID of replica blocks stored in each node. blocksInNodesStored tells the size. Array's size might be larger than that. */
	private int[][] blocksInNodes;
	
	/** ongoing recovery task. */
	private static class Recovery {
		public Recovery(int id, double remainingGigabytes) {
			this.id = id;
			this.remainingGigabytes = remainingGigabytes;
		}
		/** ID of node. */
		int id;
		double remainingGigabytes;
	}
	/** ongoing recoveries. */
	private ArrayList<Recovery> recoveries = new ArrayList<Recovery>();
	/** how many replicas (including the original) are available for the block. zero means the block is permanently lost.*/
	private byte[] redundancies;

	private int getFirstToBeCompleted() {
		int ret = -1;
		double minGb = 0;
		for (int i = 0; i < recoveries.size(); ++i) {
			double gb = recoveries.get(i).remainingGigabytes;
			if (ret == -1 || gb < minGb) {
				ret = i;
				minGb = gb;
			}
		}
		return ret;
	}
	
	private void doRecoveries (double elapsed) {
		while (!recoveries.isEmpty()) {
			Recovery first = recoveries.get(getFirstToBeCompleted ());
			double combinedRate = config.getNetworkRate(recoveries.size());
			if (first.remainingGigabytes / combinedRate > elapsed) {
				// nothing changes except each recovery task's remaining time is reduced.
				for (Recovery recovery : recoveries) {
					assert (recovery.remainingGigabytes / combinedRate >= elapsed);
					recovery.remainingGigabytes -= combinedRate * elapsed;
				}
				break;
			} else {
				// finish one recovery and recalculate the rate
				double spent = first.remainingGigabytes / combinedRate;
				for (int i = 0; i < recoveries.size(); ++i) {
					Recovery recovery = recoveries.get(i);
					recovery.remainingGigabytes -= combinedRate * spent;
					if (recovery.remainingGigabytes <= 0.5d) {
						// then consider it's done.
						onNodeRecovered (recovery.id);
						recoveries.remove(i);
						--i; // to keep checking the following (otherwise the next entry will be skipped).
					}
				}
				
				combinedRate = config.getNetworkRate(recoveries.size());
				elapsed -= spent;
			}			
		}
	}
	private void onNodeRecovered (int nodeId) {
		assert (nodeId >= 0);
		assert (nodeId < config.nodes);
		final int count = blocksInNodesStored[nodeId];
		for (int i = 0; i < count; ++i) {
			int block = blocksInNodes[nodeId][i];
			++redundancies[block];
		}
	}
	private void onNodeFailure (int nodeId) throws DataLostException {
		assert (nodeId >= 0);
		assert (nodeId < config.nodes);
		final int count = blocksInNodesStored[nodeId];
		for (int i = 0; i < count; ++i) {
			int block = blocksInNodes[nodeId][i];
			--redundancies[block];
			assert (redundancies[block] >= 0);
			if (redundancies[block] == 0) {
				throw new DataLostException("block " + block + " has been permanently lost because of a failure in node " + nodeId + "!");
			}
		}
	}
	
	private void addNodeFailure (int nodeId) throws DataLostException {
		// check if the node is already being recovered
		boolean exists = false;
		double gbToRecover = blocksInNodesStored[nodeId] * BLOCK_SIZE;
		for (Recovery recovery : recoveries) {
			if (recovery.id == nodeId) {
				recovery.remainingGigabytes = gbToRecover;
				exists = true;
				break;
			}
		}
		if (!exists) {
			onNodeFailure(nodeId);
			recoveries.add(new Recovery(nodeId, gbToRecover));
		}
	}
	
	@Override
	public double simulateTimeToFail (FailureSchedule schedule) {
		LOG.debug("running one schedule...");
		Arrays.fill(redundancies, (byte) policy.replicationFactor);
		recoveries.clear();
		Runtime.getRuntime().gc();

		// remaining times for recovery as of the previous event. 0 means the node is available
		double[] nodeRecoveryTimes = new double[config.nodes];
		Arrays.fill(nodeRecoveryTimes, 0.0d);
		
		assert (schedule.getNow() == 0.0d);
		try {
			while (true) {
				FailureEvent event = schedule.getNextEvent();
				if (event == null) {
					break;
				}
				// since the previous event, some recovery might be completed.
				doRecoveries(event.interval);
				
				if (!event.rackFailure) {
					addNodeFailure (event.failedNode);
				} else {
					for (int i = 0; i < config.nodesPerRack; ++i) {
						int nodeId = config.firstNodeIdFromRackId(event.failedNode) + i;
						addNodeFailure (nodeId);
					}
				}
			}
		} catch (DataLostException ex) {
			LOG.debug("Simulated a data loss! now=" + schedule.getNow() + ". " + ex.getMessage());
			return schedule.getNow();
		}
		LOG.debug("ran schedule without data loss.");
		return Double.POSITIVE_INFINITY;
	}
	@Override
	protected String summarizeParameters() {
		return "HDFS:" + policy;
	}
}

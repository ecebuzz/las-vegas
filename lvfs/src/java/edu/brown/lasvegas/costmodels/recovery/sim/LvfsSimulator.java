package edu.brown.lasvegas.costmodels.recovery.sim;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.costmodels.recovery.sim.FailureSchedule.FailureEvent;

/**
 * Simulator to estimate Data loss probability for LVFS.
 * <p>The biggest differences from {@link HdfsSimulator} are
 * attributed to the recovery between replica groups.</p>
 * 
 * <p>We have two recovery tasks, CopyTask and RepartitionTask.
 * CopyTask is just same as HdfsSimulator. It's done when recovering
 * from buddy partitions or re-partitioned blocks.
 * RepartitionTask is used when recovering from another replica group
 * and produces re-partitioned blocks which are then used in CopyTask.</p>
 * 
 * <p>We need to track redundancy in each replica group to tell if we
 * need to do the latter.</p>
 */
public class LvfsSimulator extends Simulator {
	/** 1GB per partition (with 20 columns, 50MB per file). */
	private static final double DEFAULT_PARTITION_SIZE = 1.0d;
	/** this might not be the default value if racksPerGroup is too large. */
	private final double PARTITION_SIZE;
	/** keep the repartitioned files in stable storage for 5 hours. */
	private static final double REPARTITIONED_FILES_HOLD = 5 * 60;
    private static Logger LOG = Logger.getLogger(LvfsSimulator.class);
	private final LvfsPlacementParameters policy;
	public LvfsSimulator(ExperimentalConfiguration config, LvfsPlacementParameters policy, long firstRandomSeed) {
		super (config, firstRandomSeed);
		this.policy = policy;
		double fractureSize = (double) config.gigabytesTotal / (policy.fracturesPerTable * config.tables);
		double partitionsIdeal = fractureSize / DEFAULT_PARTITION_SIZE;
		int minSchemes = Integer.MAX_VALUE;
		for (int schemes : policy.replicaSchemes) {
			minSchemes = Math.min(minSchemes, schemes);
		}
		if (policy.racksPerGroup * config.nodesPerRack / minSchemes > partitionsIdeal) {
			LOG.warn("WARNING!: racksPerGroup=" + policy.racksPerGroup + " is too large to assign at least one partition for each node. reducing the size of partition."
					+ "Consider using smaller racksPerGroup (unless you're testing it on purpose).");
			PARTITION_SIZE = (fractureSize / (policy.racksPerGroup * config.nodesPerRack / minSchemes)) - 0.00001d;
			partitionsIdeal = fractureSize / PARTITION_SIZE;
			assert (policy.racksPerGroup * config.nodesPerRack / minSchemes <= partitionsIdeal);
		} else {
			PARTITION_SIZE = DEFAULT_PARTITION_SIZE;
		}
		this.partitions = (int) Math.ceil(partitionsIdeal);
		LOG.info("number of partitions in each fracture=" + partitions + " (" + partitionsIdeal + ")");
	}
	/** number of partitions in each fracture. */
	private final int partitions;

	private static int NEXT_BLOCK_GROUP_ID = 0; 
	private static int NEXT_STATUS_ID = 0; 

	private class NodeStatus {
		BlockGroup[] blockGroups;
		ArrayList<BlockGroup> tmpBlockGroupsList = new ArrayList<BlockGroup>();
		/**
		 * The time to simply recover this node if it's the only failing node.
		 * Used to speed up simulation.
		 */
		double simpleRecoveryTime = Double.POSITIVE_INFINITY;
		
		void finalizeBlockGroups() {
			blockGroups = tmpBlockGroupsList.toArray(new BlockGroup[tmpBlockGroupsList.size()]);
			tmpBlockGroupsList = null;
			simpleRecoveryTime = 0;
			double singleRecoveryRate = config.getCombinedRate(config.getNetworkRate(1));
			for (BlockGroup bg : blockGroups) {
				bg.finalizePartitions();
				if (policy.replicaSchemes[bg.group] == 1 || !policy.buddyExclusion) {
					simpleRecoveryTime = Double.POSITIVE_INFINITY; // then can't simply calculate.
				} else if (simpleRecoveryTime != Double.POSITIVE_INFINITY) {
					simpleRecoveryTime += bg.partitions.length * PARTITION_SIZE / singleRecoveryRate;
				}
			}
		}
		void resetStatus() {
			blockGroupsBeingRecovered = 0;
			blockGroupsActivelyBeingRecovered.clear();
			for (BlockGroup bg : blockGroups) {
				bg.resetStatus();
			}
		}
		
		/** for debugging. */
		void validate () {
			int recoveringGroups = 0;
			for (BlockGroup bg : blockGroups) {
				if (bg.beingRecovered) {
					++recoveringGroups;
					if (bg.beingActivelyRecovered) {
						assert (blockGroupsActivelyBeingRecovered.contains(bg));
					}
				} else {
					assert (bg.remainingGigabytes == 0);
					assert (!bg.beingActivelyRecovered);
					assert (!blockGroupsActivelyBeingRecovered.contains(bg));
				}
			}
			assert (blockGroupsBeingRecovered == recoveringGroups);
		}
		
		/**
		 * Total number of BlockGroup that are being recovered at this node.
		 */
		int blockGroupsBeingRecovered = 0;
		
		/**
		 * BlockGroup that are being recovered without pending (due to wait for repartitioning) at this node.
		 */
		HashSet<BlockGroup> blockGroupsActivelyBeingRecovered = new HashSet<BlockGroup>();
	}
	private static class BlockGroup {
		BlockGroup (int table, int fracture, int group, int nodeId) {
			this.id = NEXT_BLOCK_GROUP_ID++;
			this.table = table;
			this.fracture = fracture;
			this.group = group;
			this.nodeId = nodeId;
		}
		final int id;
		final int table;
		final int fracture;
		final int group;
		final int nodeId;
		int[] partitions;
		ArrayList<Integer> tmpPartitionsList = new ArrayList<Integer>();
		// final ArrayList<Integer> schemes = new ArrayList<Integer>();
		void finalizePartitions() {
			partitions = new int[tmpPartitionsList.size()];
			for (int i = 0; i < partitions.length; ++i) {
				partitions[i] = tmpPartitionsList.get(i);
			}
			tmpPartitionsList = null;
		}
		void resetStatus() {
			beingRecovered = false;
			beingActivelyRecovered = false;
			remainingGigabytes = 0;
		}
		@Override
		public int hashCode() {
			return id;
		}

		// properties for recovery
		boolean beingRecovered = false;
		boolean beingActivelyRecovered = false;
		double remainingGigabytes = 0;
	}
	
	
	private class ReplicaGroupFractureStatus {
		ReplicaGroupFractureStatus (int table, int fracture, int group, int[] assignedRacks) {
			this.id = NEXT_STATUS_ID++;
			this.table = table;
			this.fracture = fracture;
			this.group = group;
			this.buddySchemes = policy.replicaSchemes[group];
			this.redundancies = new byte[partitions];
			this.redundancyStats = new int[128];
			this.totalGigabytes = partitions * PARTITION_SIZE;
			this.totalRepartitionRate = config.localRepartition * assignedRacks.length * config.nodesPerRack;
			resetStatus();
		}
		final int id;
		final int table;
		final int fracture;
		final int group;
		final int buddySchemes;
		
		/** number of available replicas for each partition in this replica group in this fracture. */
		byte[] redundancies;
		
		/** number of replicas with the redundancy. eg redundancyStats[1]=number of entries in redundancies whose value=1.*/
		int[] redundancyStats;

		/** total amount of original (without replication) data. referred for re-partitioning.*/
		final double totalGigabytes;
		/** total (all nodes involved) combined (disk read/write, partition) throughput of re-partitioning.*/
		final double totalRepartitionRate;
		
		@Override
		public int hashCode() {
			return id;
		}
		void resetStatus () {
			Arrays.fill(redundancies, (byte) buddySchemes);
			Arrays.fill(redundancyStats, 0);
			redundancyStats[buddySchemes] = partitions;
			waitingForRepartition.clear();
			remainingGigabytes = 0;
			repartitionedBlocksAvailableUntil = 0;
		}
		
		/** for debugging. */
		void validate () {
			if (remainingGigabytes == 0) {
				assert (waitingForRepartition.size() == 0);
			} else {
				assert (remainingGigabytes > 0);
				assert (waitingForRepartition.size() > 0);
				for (BlockGroup bg : waitingForRepartition.values()) {
					assert (bg.table == table);
					assert (bg.fracture == fracture);
					assert (bg.group == group);
					assert (bg.beingRecovered);
					assert (!bg.beingActivelyRecovered);
					assert (bg.remainingGigabytes > 0);
				}
			}
			int[] redundancyStatsCorrect = new int[redundancyStats.length];
			Arrays.fill(redundancyStatsCorrect, 0);
			for (int i = 0; i < redundancies.length; ++i) {
				assert (redundancies[i] >= 0);
				++redundancyStatsCorrect[redundancies[i]];
			}
			for (int i = 0; i < redundancyStats.length; ++i) {
				assert (redundancyStats[i] == redundancyStatsCorrect[i]);
			}
		}
		
		/**
		 * Block groups that are being recovered, but pending because of this repartitioning.
		 * key=nodeId.
		 */
		HashMap<Integer, BlockGroup> waitingForRepartition = new HashMap<Integer, BlockGroup>();

		/** until the completion of repartitioning. */
		double remainingGigabytes = 0;
		
		boolean isRepartitionedBlocksAvailable (double now) {
			return now <= repartitionedBlocksAvailableUntil;
		}
		/**
		 * Repartitioned blocks are kept for a while, and then deleted on this time.
		 */
		double repartitionedBlocksAvailableUntil = 0;

	}
	
	private void addRepartitionTask (ReplicaGroupFractureStatus target) {
		assert (!repartitionTasks.contains(target));
		repartitionTasks.add(target);
		target.remainingGigabytes = target.totalGigabytes;
	}

	private NodeStatus[] nodeStatuses;
	/** status of each replica group in each fracture. [table][fracture][group].*/
	private ReplicaGroupFractureStatus[][][] groupStatuses;
	/** ongoing copy tasks (nodeID to recover). */
	private HashSet<Integer> copyTasks;
	/** ongoing repartition tasks (the replica group _to be recovered_ (not the source)). */
	private HashSet<ReplicaGroupFractureStatus> repartitionTasks;

	private void assignBlock (int nodeId, int table, int fracture, int group, int scheme, int partition) {
		ArrayList<BlockGroup> blocks = nodeStatuses[nodeId].tmpBlockGroupsList;
		BlockGroup blockGroup = null;
		for (BlockGroup bg : blocks) {
			if (bg.table == table && bg.fracture == fracture && bg.group == group) {
				blockGroup = bg;
				break;
			}
		}
		if (blockGroup == null) {
			blockGroup = new BlockGroup (table, fracture, group, nodeId);
			blocks.add(blockGroup);
		}
		blockGroup.tmpPartitionsList.add(partition);
	}

	@Override
	public void decidePlacement() {
		LOG.info("deciding data placement in LVFS... parameters=" + policy);
		// use a fixed random seed to determine the placement.
		// should be okay. we are iterating over various schedules, not placement.
		Random random = new Random(123456L);
		
		nodeStatuses = new NodeStatus[config.nodes];
		for (int i = 0; i < config.nodes; ++i) {
			nodeStatuses[i] = new NodeStatus();
		}
		groupStatuses = new ReplicaGroupFractureStatus[config.tables][policy.fracturesPerTable][policy.replicaSchemes.length];
		
		int currentRack = 0; // to assign racks in RR fashion
		for (int table = 0; table < config.tables; ++table) {
			for (int fracture = 0; fracture < policy.fracturesPerTable; ++fracture) {
				for (int group = 0; group < policy.replicaSchemes.length; ++group) {
					// listup nodes assigned to this group
					int[] assignedRacks = new int[config.nodesPerRack];
					int[] assignedNodes = new int[config.nodesPerRack * policy.racksPerGroup];
					for (int i = 0; i < policy.racksPerGroup; ++i) {
						int firstNodeId = config.firstNodeIdFromRackId(currentRack);
						/*
						// randomly shuffle in each rack.
						// this shuffling increases the number of possible couples among different table/fracture/group.
						// not quite ideal, so commented out.
						ArrayList<Integer> remaining = new ArrayList<Integer>();
						for (int j = 0; j < config.nodesPerRack; ++j) {
							remaining.add(firstNodeId + j);
						}
						for (int j = 0; j < config.nodesPerRack; ++j) {
							assignedNodes[i * config.nodesPerRack + j] = remaining.remove(random.nextInt(remaining.size()));
						}
						*/
						for (int j = 0; j < config.nodesPerRack; ++j) {
							assignedNodes[i * config.nodesPerRack + j] = firstNodeId + j;
						}
						assignedRacks[i] = currentRack;
						++currentRack;
						if (currentRack == config.racks) {
							currentRack = 0;
						}
					}
					int buddySchemes = policy.replicaSchemes[group];

					groupStatuses[table][fracture][group] = new ReplicaGroupFractureStatus(table, fracture, group, assignedRacks);

					if (!policy.buddyExclusion) {
						// no techniques. just randomly scatter them.
						for (int partition = 0; partition < partitions; ++partition) {
							for (int scheme = 0; scheme < buddySchemes; ++scheme) {
								int nodeId = assignedNodes[random.nextInt(assignedNodes.length)];
								assignBlock(nodeId, table, fracture, group, scheme, partition);
							}
						}
					} else if (!policy.nodeCoupling) {
						// only buddy exclusion
						assert (!policy.buddySwapping);
						int[] buffer = new int[buddySchemes];
						for (int partition = 0; partition < partitions; ++partition) {
							generateRandomSet(random, buddySchemes, 0, assignedNodes.length, buffer);
							for (int scheme = 0; scheme < buddySchemes; ++scheme) {
								assignBlock(assignedNodes[buffer[scheme]], table, fracture, group, scheme, partition);
							}
						}
					} else {
						// with node coupling.
						int couples = assignedNodes.length / buddySchemes;
						int placedPartitions = 0;
						int swaps = 0; // for buddy swapping
						assert (partitions >= couples);
						// when partitions < couples, the following algorithm might not assign any
						// partitions to some nodes (e.g., 30 couples, 10 partitions=partitions only in 3rd, 6th, 9th... node)
						// this's why we reduce the size of partition in constructor.
						for (int couple = 0; couple < couples; ++couple) {
							int partitionsEnd = (int) (partitions * (couple + 1) / couples);
							assert (couple != couples - 1 || partitionsEnd == partitions);
							for (int partition = placedPartitions; partition < partitionsEnd; ++partition) {
								for (int scheme = 0; scheme < buddySchemes; ++scheme) {
									// by this way, we most likely couple nodes from different racks
									int swappedScheme = (scheme + swaps) % buddySchemes;
									int nodeId = assignedNodes[swappedScheme * couples + couple];
									assignBlock(nodeId, table, fracture, group, scheme, partition);
								}
								if (policy.buddySwapping) {
									++swaps;
								}
							}
							placedPartitions = partitionsEnd;
						}
					}
				}
			}
		}
		int sumBlocks = 0, maxBlocks = 0, maxNode = 0;
		for (int i = 0; i < config.nodes; ++i) {
			nodeStatuses[i].finalizeBlockGroups();
			sumBlocks += nodeStatuses[i].blockGroups.length;
			if (nodeStatuses[i].blockGroups.length > maxBlocks) {
				maxBlocks = nodeStatuses[i].blockGroups.length;
				maxNode = i;
			}
		}
		LOG.info("decided data placement. average blockGroups-per-node=" + ((double) sumBlocks / config.nodes) + ", max=" + maxBlocks + "(node=" + maxNode + ")");
		for (int i = 0; i < config.nodes; ++i) {
			if (nodeStatuses[i].blockGroups.length > (double) sumBlocks * 3 / config.nodes) {
				LOG.warn("node-" + i + " has as many as " + nodeStatuses[i].blockGroups.length + " blockGroups.. wtf??");
			}
		}
	}
	/** pick the given number of values from the given domain without duplicates. */
	private static void generateRandomSet (Random random, int setSize, int domainFrom, int domainTo, int[] buffer) {
		assert (domainFrom + setSize <= domainTo);
		for (int i = 0; i < setSize; ++i) {
			int value;
			while (true) {
				value = domainFrom + random.nextInt (domainTo - domainFrom);
				boolean restart = false;
				for (int j = 0; j < i; ++j) {
					if (buffer[j] == value) {
						restart = true;
						break;
					}
				}
				if (!restart) {
					break;
				}
			}
			buffer[i] = value;
		}
	}

	private double getNetworkRate (double now) {
		int activeCopyTaskCount = 0;
		for (Integer nodeId : copyTasks) {
			if (!nodeStatuses[nodeId].blockGroupsActivelyBeingRecovered.isEmpty()) {
				++activeCopyTaskCount;
			}
		}
		assert (activeCopyTaskCount <= copyTasks.size());
		return config.getNetworkRate(activeCopyTaskCount);
	}
	private double getNextTimeToSpend(double now, double networkRate) {
		double minTime = Double.MAX_VALUE;
		double combinedNodeRecoveryRate = config.getCombinedRate(networkRate);
		for (Integer nodeId : copyTasks) {
			NodeStatus nodeStatus = nodeStatuses[nodeId];
			assert (nodeStatus.blockGroupsBeingRecovered > 0);
			if (nodeStatus.blockGroupsActivelyBeingRecovered.isEmpty()) {
				continue;
			}
			double timeTotal = 0;
			for (BlockGroup bg : nodeStatus.blockGroupsActivelyBeingRecovered) {
				assert (bg.beingActivelyRecovered);
				timeTotal += bg.remainingGigabytes / combinedNodeRecoveryRate;
			}
			// consider the active copy tasks as one composite task.
			if (timeTotal < minTime) {
				minTime = timeTotal;
			}
		}
		for (ReplicaGroupFractureStatus status : repartitionTasks) {
			double time = status.remainingGigabytes / status.totalRepartitionRate;
			if (time < minTime) {
				minTime = time;
			}
		}
		return minTime;
	}

	private void doRecoveries (double now, double elapsed) {
		while (!copyTasks.isEmpty() || !repartitionTasks.isEmpty()) {
			double networkRate = getNetworkRate (now);
			double combinedNodeRecoveryRate = config.getCombinedRate(networkRate);
			double spent = getNextTimeToSpend (now, networkRate);
			if (spent > elapsed) {
				spent = elapsed; 
			}

			for (Integer nodeId : copyTasks.toArray(new Integer[0])) { // copy to avoid 'deletion in loop'
				// complete tasks one by one
				NodeStatus nodeStatus = nodeStatuses[nodeId];
				assert (nodeStatus.blockGroupsBeingRecovered > 0);
				if (nodeStatuses[nodeId].blockGroupsActivelyBeingRecovered.isEmpty()) {
					continue;
				}
				double budget = spent;
				for (BlockGroup bg : nodeStatus.blockGroupsActivelyBeingRecovered.toArray(new BlockGroup[0])) {// copy to avoid 'deletion in loop'
					assert (bg.beingActivelyRecovered);
					double required = bg.remainingGigabytes / combinedNodeRecoveryRate;
					if (budget >= required) {
						onBlockGroupRecovered(nodeStatus, bg);
						budget -= required;
					} else {
						bg.remainingGigabytes -= combinedNodeRecoveryRate * budget;
						break;
					}
				}

				if (nodeStatus.blockGroupsBeingRecovered == 0) {
					copyTasks.remove(nodeId);
				}
			}
			for (ReplicaGroupFractureStatus status : repartitionTasks.toArray(new ReplicaGroupFractureStatus[0])) { // copy to avoid 'deletion in loop'
				assert (status.remainingGigabytes > 0);
				double progressedGigabytes = status.totalRepartitionRate * spent;
				if (progressedGigabytes < status.remainingGigabytes) {
					status.remainingGigabytes -= progressedGigabytes;
				} else {
					onRepartitionCompleted(now, status);
					repartitionTasks.remove(status);
				}
			}

			now += spent;
			elapsed -= spent;
			if (elapsed <= 0) {
				break;
			}
		}
	}
	private void onBlockGroupRecovered (NodeStatus nodeStatus, BlockGroup bg) {
		assert (bg.beingRecovered);
		assert (bg.beingActivelyRecovered);
		bg.remainingGigabytes = 0;
		ReplicaGroupFractureStatus status = groupStatuses[bg.table][bg.fracture][bg.group];
		for (int partition : bg.partitions) {
			assert (status.redundancies[partition] < policy.replicaSchemes[bg.group]);
			--status.redundancyStats[status.redundancies[partition]];
			++status.redundancyStats[status.redundancies[partition] + 1];
			++status.redundancies[partition];
		}
		bg.beingRecovered = false;
		bg.beingActivelyRecovered = false;
		assert (nodeStatus.blockGroupsActivelyBeingRecovered.contains(bg));
		nodeStatus.blockGroupsActivelyBeingRecovered.remove(bg);
		--nodeStatus.blockGroupsBeingRecovered;
		assert (nodeStatus.blockGroupsBeingRecovered >= 0);
	}
	private void onRepartitionCompleted(double now, ReplicaGroupFractureStatus status) {
		status.remainingGigabytes = 0;
		status.repartitionedBlocksAvailableUntil = now + REPARTITIONED_FILES_HOLD;
		for (BlockGroup bg : status.waitingForRepartition.values()) {
			NodeStatus nodeStatus = nodeStatuses[bg.nodeId];
			assert (bg.beingRecovered);
			assert (!bg.beingActivelyRecovered);
			assert (!nodeStatus.blockGroupsActivelyBeingRecovered.contains(bg));
			assert (bg.table == status.table);
			assert (bg.fracture == status.fracture);
			assert (bg.group == status.group);
			bg.beingActivelyRecovered = true;
			nodeStatus.blockGroupsActivelyBeingRecovered.add(bg);
		}
		status.waitingForRepartition.clear();
	}

	private void addNodeFailure (double now, int nodeId) throws DataLostException {
		assert (nodeId >= 0);
		assert (nodeId < config.nodes);
		NodeStatus nodeStatus = nodeStatuses[nodeId];
		if (nodeStatus.blockGroups.length == 0) {
			return; // empty node.
		}
		if (!copyTasks.contains(nodeId)) {
			copyTasks.add(nodeId);
		}
		
		for (BlockGroup bg : nodeStatus.blockGroups) {
			// invalidate blocks unless the blocks were already being recovered
			if (!bg.beingRecovered) {
				boolean otherGroupAvailable = false;
				for (int group = 0; group < policy.replicaSchemes.length; ++group) {
					if (group == bg.group) {
						continue;
					}
					if (groupStatuses[bg.table][bg.fracture][group].redundancyStats[0] == 0
							|| groupStatuses[bg.table][bg.fracture][group].isRepartitionedBlocksAvailable(now)) {
						// some other replica group is active. we can recover from it
						otherGroupAvailable = true;
						break;
					}
				}
				ReplicaGroupFractureStatus status = groupStatuses[bg.table][bg.fracture][bg.group];
				boolean recoverableFromRepartitionedFiles = status.isRepartitionedBlocksAvailable(now);

				for (int partition : bg.partitions) {
					assert (status.redundancies[partition] > 0);
					--status.redundancyStats[status.redundancies[partition]];
					++status.redundancyStats[status.redundancies[partition] - 1];
					--status.redundancies[partition];
					assert (status.redundancies[partition] >= 0);
					if (status.redundancies[partition] == 0) {
						// we have lost this partition! no recovery possible in this group
						if (recoverableFromRepartitionedFiles) {
							continue; // okay, we can recover from the repartitioned files
						}
						if (!otherGroupAvailable) {
							// no other group is available. fail
							throw new DataLostException("lost a partition " + partition
									+ " in table " + bg.table + ", fracture " + bg.fracture
									+ " permanently! triggered by a failure in node " + nodeId);
						}
						// okay, some other group is available, but we need to repartition
						if (status.remainingGigabytes == 0) {
							addRepartitionTask (status);
							assert (status.remainingGigabytes > 0);
						}
					}
				}
				assert (!bg.beingActivelyRecovered);
				bg.beingRecovered = true;
				++nodeStatus.blockGroupsBeingRecovered;
				assert (nodeStatus.blockGroupsBeingRecovered <= nodeStatus.blockGroups.length);
				if (status.remainingGigabytes == 0) {
					bg.beingActivelyRecovered = true;
					assert (!nodeStatus.blockGroupsActivelyBeingRecovered.contains(bg));
					nodeStatus.blockGroupsActivelyBeingRecovered.add(bg);
				} else {
					assert (!status.waitingForRepartition.containsKey(nodeId));
					status.waitingForRepartition.put(nodeId, bg);
				}
			}
			bg.remainingGigabytes = bg.partitions.length * PARTITION_SIZE;
		}
		assert (nodeStatus.blockGroupsBeingRecovered > 0);
	}
	private void addRackFailure (double now, int rackId) throws DataLostException {
		for (int i = 0; i < config.nodesPerRack; ++i) {
			int nodeId = config.firstNodeIdFromRackId(rackId) + i;
			addNodeFailure (now, nodeId);
		}
	}
	
	
	private void resetStatus () {
		for (NodeStatus nodeStatus : nodeStatuses) {
			nodeStatus.resetStatus();
		}
		for (int table = 0; table < config.tables; ++table) {
			for (int fracture = 0; fracture < policy.fracturesPerTable; ++fracture) {
				for (int group = 0; group < policy.replicaSchemes.length; ++group) {
					groupStatuses[table][fracture][group].resetStatus();
				}
			}
		}
		copyTasks = new HashSet<Integer>();
		repartitionTasks = new HashSet<ReplicaGroupFractureStatus>();
		Runtime.getRuntime().gc();
	}
	/** for debugging. */
	private boolean validateAll () {
		for (NodeStatus nodeStatus : nodeStatuses) {
			nodeStatus.validate();
		}
		for (int table = 0; table < config.tables; ++table) {
			for (int fracture = 0; fracture < policy.fracturesPerTable; ++fracture) {
				for (int group = 0; group < policy.replicaSchemes.length; ++group) {
					groupStatuses[table][fracture][group].validate();
				}
			}
		}
		return true;
	}
	@Override
	protected double simulateTimeToFail(FailureSchedule schedule) {
		LOG.debug("running one schedule...");
		resetStatus ();
		int skippedEvents = 0;
		assert (schedule.getNow() == 0.0d);
		try {
			while (true) {
				double prevNow = schedule.getNow();
				FailureEvent event = schedule.getNextEvent();
				if (event == null) {
					break;
				}
				// since the previous event, some recovery might be completed.
				doRecoveries(prevNow, event.interval);

				if (!event.rackFailure) {
					if (copyTasks.isEmpty() && repartitionTasks.isEmpty()) {
						FailureEvent nextEvent = schedule.peekNextEvent();
						if (nextEvent.interval >= nodeStatuses[event.failedNode].simpleRecoveryTime) {
							// all blocks will be recovered by the next event.
							// then we can ignore this event.
							// hey, but this tuning gave almost nothing because the situation is rare (about one failure per 20 minutes, simpleRecoverytime is like 100 min)..
							// still, this might be useful for some experiments. (e.g., fewer machines=less failures)
							++skippedEvents;
							continue;
						}
					}
					addNodeFailure (schedule.getNow(), event.failedNode);
				} else {
					// LOG.info(schedule.getNow() + ": rack-" + event.failedNode);
					addRackFailure (schedule.getNow(), event.failedNode);
				}
			}
		} catch (DataLostException ex) {
			LOG.debug("Simulated a data loss! now=" + schedule.getNow() + ". " + ex.getMessage() + ". #events:#skipped_events=" + schedule.getEventCount() + ":" + skippedEvents);
			assert(validateAll());
			return schedule.getNow();
		}
		
		assert(validateAll());
		LOG.debug("ran schedule without data loss. #events:#skipped_events=" + schedule.getEventCount() + ":" + skippedEvents);
		return Double.POSITIVE_INFINITY;
	}
	@Override
	protected String summarizeParameters() {
		return "LVFS:" + policy;
	}
}

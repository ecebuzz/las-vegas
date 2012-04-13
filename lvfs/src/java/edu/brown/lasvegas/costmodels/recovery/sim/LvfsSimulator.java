package edu.brown.lasvegas.costmodels.recovery.sim;

import java.util.ArrayList;
import java.util.Arrays;
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
	/** assume 20 columns per table */
	private static final int COLUMNS_PER_TABLE = 20;
	/** 50MB per columnar file */
	private static final double COLUMN_FILE_SIZE = 0.05d;
    private static Logger LOG = Logger.getLogger(LvfsSimulator.class);
	private final LvfsPlacementParameters policy;
	public LvfsSimulator(ExperimentalConfiguration config, LvfsPlacementParameters policy, long firstRandomSeed) {
		super (config, firstRandomSeed);
		this.policy = policy;
		double partitionsIdeal = (double) config.gigabytesTotal
			/ (policy.fracturesPerTable * config.tables)
			/ COLUMNS_PER_TABLE
			/ COLUMN_FILE_SIZE
			;
		this.partitions = (int) Math.ceil(partitionsIdeal);
		LOG.info("number of partitions in each fracture=" + partitions + " (" + partitionsIdeal + ")");
	}
	/** number of partitions in each fracture. */
	private final int partitions;

	private ArrayList<ArrayList<BlockGroup>> blocksInNodes;
	private static class BlockGroup {
		BlockGroup (int table, int fracture, int group) {
			this.table = table;
			this.fracture = fracture;
			this.group = group;
		}
		final int table;
		final int fracture;
		final int group;
		final ArrayList<Integer> partitions = new ArrayList<Integer>();
		// final ArrayList<Integer> schemes = new ArrayList<Integer>();

		// properties for recovery
		boolean beingRecovered = false;
		double remainingGigabytes = 0;
	}
	
	
	private static class ReplicaGroupFractureStatus {
		@SuppressWarnings("unused")
		int table;
		@SuppressWarnings("unused")
		int fracture;
		@SuppressWarnings("unused")
		int group;
		@SuppressWarnings("unused")
		int[] assignedRacks;
		
		/** number of available replicas for each partition in this replica group in this fracture. */
		byte[] redundancies;
		
		/** number of replicas with the redundancy. eg redundancyStats[1]=number of entries in redundancies whose value=1.*/
		int[] redundancyStats;

		/** total amount of original (without replication) data. referred for re-partitioning.*/
		double totalGigabytes;
		/** total amount of disk read/write throughput available while re-partitioning.*/
		double totalDiskRate;
		
		/**
		 * The recovery task to repartition another replica group's data
		 * when this replica group is lost (redundancyStats[0] > 0).
		 * Once done, this property is set to NULL.
		 */
		Repartition repartitionTask;

		/** until the completion of repartitioning. */
		double remainingGigabytes;
		
		boolean isRepartitionedBlocksAvailable (double now) {
			return now <= repartitionedBlocksAvailableUntil;
		}
		/**
		 * Repartitioned blocks are kept for a while, and then deleted on this time.
		 */
		double repartitionedBlocksAvailableUntil = 0;
	}
	
	private static class Recovery {
	}
	/**
	 * Recovery task to copy lost partitions from other nodes.
	 * One task per one node (because it uses the same disk bandwidth).
	 */
	private static class Copy extends Recovery {
		public Copy(int nodeId) {
			this.nodeId = nodeId;
		}
		int nodeId;
	}
	/**
	 * Recovery task to create partition files from other replica group.
	 * One task per one ReplicaGroupFractureStatus.
	 */
	private static class Repartition extends Recovery {
		public Repartition(ReplicaGroupFractureStatus target) {
			assert (target.repartitionTask == null);
			this.target = target;
			target.repartitionTask = this;
			target.remainingGigabytes = target.totalGigabytes;
		}
		/** the replica group _to be recovered_ (not the source). */
		final ReplicaGroupFractureStatus target;
	}

	/** status of each replica group in each fracture. [table][fracture][group].*/
	private ReplicaGroupFractureStatus[][][] allStatus;
	/** ongoing recoveries. */
	private ArrayList<Recovery> recoveries;

	private void assignBlock (int nodeId, int table, int fracture, int group, int scheme, int partition) {
		ArrayList<BlockGroup> blocks = blocksInNodes.get(nodeId);
		BlockGroup blockGroup = null;
		for (BlockGroup bg : blocks) {
			if (bg.table == table && bg.fracture == fracture && bg.group == group) {
				blockGroup = bg;
				break;
			}
		}
		if (blockGroup == null) {
			blockGroup = new BlockGroup (table, fracture, group);
			blocks.add(blockGroup);
		}
		blockGroup.partitions.add(partition);
	}

	@Override
	public void decidePlacement() {
		LOG.info("deciding data placement in LVFS... parameters=" + policy);
		// use a fixed random seed to determine the placement.
		// should be okay. we are iterating over various schedules, not placement.
		Random random = new Random(123456L);
		
		blocksInNodes = new ArrayList<ArrayList<BlockGroup>>();
		for (int i = 0; i < config.nodes; ++i) {
			blocksInNodes.add (new ArrayList<BlockGroup>());
		}
		allStatus = new ReplicaGroupFractureStatus[config.tables][policy.fracturesPerTable][policy.replicaSchemes.length];
		
		int currentRack = 0; // to assign racks in RR fashion
		for (int table = 0; table < config.tables; ++table) {
			for (int fracture = 0; fracture < policy.fracturesPerTable; ++fracture) {
				for (int group = 0; group < policy.replicaSchemes.length; ++group) {
					// listup nodes assigned to this group
					int[] assignedRacks = new int[config.nodesPerRack];
					int[] assignedNodes = new int[config.nodesPerRack * policy.racksPerGroup];
					for (int i = 0; i < policy.racksPerGroup; ++i) {
						int firstNodeId = config.firstNodeIdFromRackId(currentRack);
						for (int j = 0; j < config.nodesPerRack; ++j) {
							assignedNodes[i * policy.racksPerGroup + j] = firstNodeId + j;
						}
						assignedRacks[i] = currentRack;
						++currentRack;
						if (currentRack == config.racks) {
							currentRack = 0;
						}
					}
					int buddySchemes = policy.replicaSchemes[group];

					ReplicaGroupFractureStatus status = new ReplicaGroupFractureStatus();
					status.table = table;
					status.fracture = fracture;
					status.group = group;
					status.assignedRacks = assignedRacks;
					status.redundancies = new byte[partitions];
					Arrays.fill(status.redundancies, (byte) buddySchemes);
					status.redundancyStats = new int[128];
					Arrays.fill(status.redundancyStats, 0);
					status.redundancyStats[buddySchemes] = partitions;
					allStatus[table][fracture][group] = status;
					status.totalGigabytes = partitions * COLUMN_FILE_SIZE * COLUMNS_PER_TABLE;
					status.totalDiskRate = config.localDisk * assignedNodes.length;

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
								assignBlock(buffer[scheme], table, fracture, group, scheme, partition);
							}
						}
					} else {
						// with node coupling.
						int couples = assignedNodes.length / buddySchemes;
						int placedPartitions = 0;
						int swaps = 0; // for buddy swapping
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

		LOG.info("decided data placement");
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
		for (Recovery task : recoveries) {
			if (task instanceof Copy) {
				int nodeId = ((Copy) task).nodeId;
				boolean activeTask = false;
				for (BlockGroup bg : blocksInNodes.get(nodeId)) {
					if (!bg.beingRecovered) {
						continue;
					}
					ReplicaGroupFractureStatus status = allStatus[bg.table][bg.fracture][bg.group];
					if (status.redundancyStats[0] > 0 && !status.isRepartitionedBlocksAvailable(now)) {
						// we need to wait for the completion of repartitioning. skip it.
						continue;
					}
					activeTask = true;
					break;
				}
				if (activeTask) {
					++activeCopyTaskCount;
				}
			}
		}
		assert (activeCopyTaskCount <= recoveries.size());
		return config.getNetworkRate(activeCopyTaskCount);
	}
	private double getNextTimeToSpend(double now, double networkRate) {
		double minTime = Double.MAX_VALUE;
		for (Recovery task : recoveries) {
			if (task instanceof Copy) {
				int nodeId = ((Copy) task).nodeId;
				for (BlockGroup bg : blocksInNodes.get(nodeId)) {
					if (!bg.beingRecovered) {
						continue;
					}
					ReplicaGroupFractureStatus status = allStatus[bg.table][bg.fracture][bg.group];
					if (status.redundancyStats[0] > 0 && !status.isRepartitionedBlocksAvailable(now)) {
						// we need to wait for the completion of repartitioning. skip it.
						continue;
					}
					double time = bg.remainingGigabytes / config.getCombinedRate(networkRate);
					if (time < minTime) {
						minTime = time;
					}
					break; // only take a look at the first active task. we can copy blocks in arbitrary order, but let's simplify
				}
			} else {
				ReplicaGroupFractureStatus status = ((Repartition) task).target;
				double time = status.remainingGigabytes / status.totalDiskRate;
				if (time < minTime) {
					minTime = time;
				}
			}
		}
		return minTime;
	}

	private void doRecoveries (double now, double elapsed) {
		while (!recoveries.isEmpty()) {
			double networkRate = getNetworkRate (now);
			double spent = getNextTimeToSpend (now, networkRate);
			if (spent > elapsed) {
				spent = elapsed; 
			}

			for (int i = 0; i < recoveries.size(); ++i) {
				Recovery task = recoveries.get(i);

				boolean somethingRemaining = false;
				if (task instanceof Copy) {
					// complete tasks one by one
					int nodeId = ((Copy) task).nodeId;
					for (BlockGroup bg : blocksInNodes.get(nodeId)) {
						if (!bg.beingRecovered) {
							continue;
						}
						ReplicaGroupFractureStatus status = allStatus[bg.table][bg.fracture][bg.group];
						if (status.redundancyStats[0] > 0 && !status.isRepartitionedBlocksAvailable(now)) {
							// we need to wait for the completion of repartitioning. skip it.
							continue;
						}
						double progressedGigabytes = config.getCombinedRate(networkRate) * spent;
						if (progressedGigabytes < bg.remainingGigabytes) {
							bg.remainingGigabytes -= progressedGigabytes;
						} else {
							onBlockGroupRecovered(bg);
						}
						break; // only process the first active task
					}
					for (BlockGroup bg : blocksInNodes.get(nodeId)) {
						if (bg.beingRecovered) {
							somethingRemaining = true;
							break;
						}
					}
				} else {
					ReplicaGroupFractureStatus status = ((Repartition) task).target;
					double progressedGigabytes = status.totalDiskRate * spent;
					if (progressedGigabytes < status.remainingGigabytes) {
						status.remainingGigabytes -= progressedGigabytes;
						somethingRemaining = false;
					} else {
						onRepartitionCompleted(now, status);
					}
				}

				if (!somethingRemaining) {
					recoveries.remove(i);
					--i; // to keep checking the following (otherwise the next entry will be skipped).
				}
			}

			now += spent;
			elapsed -= spent;
			if (elapsed <= 0) {
				break;
			}
		}
	}
	private void onBlockGroupRecovered (BlockGroup bg) {
		assert (bg.beingRecovered);
		bg.remainingGigabytes = 0;
		ReplicaGroupFractureStatus status = allStatus[bg.table][bg.fracture][bg.group];
		for (Integer partition : bg.partitions) {
			assert (status.redundancies[partition] < policy.replicaSchemes[bg.group]);
			--status.redundancyStats[status.redundancies[partition]];
			++status.redundancyStats[status.redundancies[partition] + 1];
			++status.redundancies[partition];
		}
		bg.beingRecovered = false;
	}
	private void onRepartitionCompleted(double now, ReplicaGroupFractureStatus status) {
		status.remainingGigabytes = 0;
		status.repartitionTask = null;
		status.repartitionedBlocksAvailableUntil = now + 1440; // keep the repartitioned data for 24 hours
	}

	private void addNodeFailure (double now, int nodeId) throws DataLostException {
		assert (nodeId >= 0);
		assert (nodeId < config.nodes);
		boolean exists = false;
		for (Recovery recovery : recoveries) {
			if (!(recovery instanceof Copy)) {
				continue;
			}
			Copy copy = (Copy) recovery;
			if (copy.nodeId != nodeId) {
				continue;
			}
			exists = true;
			break;
		}
		if (!exists) {
			recoveries.add(new Copy (nodeId));
		}
		
		for (BlockGroup bg : blocksInNodes.get(nodeId)) {
			// invalidate blocks unless the blocks were already being recovered
			if (!bg.beingRecovered) {
				bg.beingRecovered = true;
				ReplicaGroupFractureStatus status = allStatus[bg.table][bg.fracture][bg.group];
				for (Integer partition : bg.partitions) {
					assert (status.redundancies[partition] > 0);
					--status.redundancyStats[status.redundancies[partition]];
					++status.redundancyStats[status.redundancies[partition] - 1];
					--status.redundancies[partition];
					if (status.repartitionTask == null && !status.isRepartitionedBlocksAvailable(now)) {
						if (status.redundancies[partition] == 0 && status.redundancyStats[0] == 1) {
							// we have just lost a partition, and there is no repartitioning going on.
							boolean recoverable = false;
							for (int group = 0; group < policy.replicaSchemes.length; ++group) {
								if (allStatus[bg.table][bg.fracture][group].redundancyStats[0] == 0
										|| allStatus[bg.table][bg.fracture][group].isRepartitionedBlocksAvailable(now)) {
									// some other replica group is active. let's recover from it
									recoverable = true;
									break;
								}
							}
							if (!recoverable) {
								throw new DataLostException("lost a partition " + partition
										+ " in table " + bg.table + ", fracture " + bg.fracture
										+ " permanently! triggered by a failure in node " + nodeId);
							}
							recoveries.add(new Repartition(status));
						}
					}
				}
			}
			bg.remainingGigabytes = bg.partitions.size() * COLUMN_FILE_SIZE * COLUMNS_PER_TABLE;
		}
	}
	
	
	private void resetStatus () {
		for (ArrayList<BlockGroup> blocks : blocksInNodes) {
			for (BlockGroup bg : blocks) {
				bg.beingRecovered = false;
				bg.remainingGigabytes = 0;
			}
		}
		for (int table = 0; table < config.tables; ++table) {
			for (int fracture = 0; fracture < policy.fracturesPerTable; ++fracture) {
				for (int group = 0; group < policy.replicaSchemes.length; ++group) {
					ReplicaGroupFractureStatus status = allStatus[table][fracture][group];
					int buddySchemes = policy.replicaSchemes[group];
					Arrays.fill(status.redundancies, (byte) buddySchemes);
					Arrays.fill(status.redundancyStats, 0);
					status.redundancyStats[buddySchemes] = partitions;
				}
			}
		}
		
		recoveries = new ArrayList<Recovery>();
	}
	@Override
	protected double simulateTimeToFail(FailureSchedule schedule) {
		LOG.debug("running one schedule...");
		resetStatus ();
		assert (schedule.getNow() == 0.0d);
		try {
			while (true) {
				double prevNow = schedule.getNow();
				FailureEvent event = schedule.generateNextEvent();
				if (event == null) {
					break;
				}
				// since the previous event, some recovery might be completed.
				doRecoveries(prevNow, event.interval);

				if (!event.rackFailure) {
					addNodeFailure (schedule.getNow(), event.failedNode);
				} else {
					for (int i = 0; i < config.nodesPerRack; ++i) {
						int nodeId = config.firstNodeIdFromRackId(event.failedNode) + i;
						addNodeFailure (schedule.getNow(), nodeId);
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
}

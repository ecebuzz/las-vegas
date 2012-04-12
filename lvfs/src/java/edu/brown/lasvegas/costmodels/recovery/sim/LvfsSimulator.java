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
			/ policy.fracturesPerTable * config.tables
			/ COLUMNS_PER_TABLE
			/ COLUMN_FILE_SIZE
			;
		this.partitions = (int) Math.ceil(partitionsIdeal);
		LOG.info("number of partitions in each fracture=" + partitions + " (" + partitionsIdeal + ")");
	}
	/** number of partitions in each fracture. */
	private final int partitions;

	private ArrayList<ArrayList<BlockId>> blocksInNodes;
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

		boolean beingRecovered = false;
		double remainingGigabytes = 0;
	}
	private static class BlockId {
		BlockId (int table, int fracture, int group, int scheme, int partition) {
			this.table = table;
			this.fracture = fracture;
			this.group = group;
			this.scheme = scheme;
			this.partition = partition;
		}
		final int table;
		final int fracture;
		final int group;
		final int scheme;
		final int partition;
	}
	private void assignBlock (int nodeId, int table, int fracture, int group, int scheme, int partition) {
		ArrayList<BlockId> blocks = blocksInNodes.get(nodeId);
		blocks.add (new BlockId(table, fracture, group, scheme, partition));
	}
	
	private static class ReplicaGroupFractureStatus {
		int table;
		int fracture;
		int group;
		int[] assignedRacks;
		
		/** number of available replicas for each partition in this replica group in this fracture. */
		byte[] redundancies;
		
		/** number of replicas with the redundancy. eg redundancyStats[1]=number of entries in redundancies whose value=1.*/
		int[] redundancyStats;
		
		/**
		 * The recovery task to repartition another replica group's data
		 * when this replica group is lost (redundancyStats[0] > 0).
		 * Once done, this property is set to NULL.
		 */
		Repartition repartitionTask;
		
		boolean repartitionedBlocksAvailable = false;
	}
	private static class RackStatus {
		/** references to ReplicaGroupFractureStatus this rack houses. */
		ArrayList<ReplicaGroupFractureStatus> residents = new ArrayList<ReplicaGroupFractureStatus>();
	}
	
	private static class Recovery {
		public Recovery(double gigabytes) {
			this.gigabytes = gigabytes;
			this.remainingGigabytes = gigabytes;
		}
		double gigabytes;
		double remainingGigabytes;
	}
	/**
	 * Recovery task to copy lost partitions from other nodes.
	 * One task per one node (because it uses the same disk bandwidth).
	 */
	private static class Copy extends Recovery {
		public Copy(int nodeId, double gigabytes) {
			super (gigabytes);
			this.nodeId = nodeId;
		}
		int nodeId;
	}
	/**
	 * Recovery task to create partition files from other replica group.
	 * One task per one ReplicaGroupFractureStatus.
	 */
	private static class Repartition extends Recovery {
		public Repartition(double gigabytes) {
			super (gigabytes);
		}
		/** the replica group _to be recovered_ (not the source). */
		ReplicaGroupFractureStatus target;
	}

	/** status of each replica group in each fracture. [table][fracture][group].*/
	private ReplicaGroupFractureStatus[][][] allStatus;
	/** ongoing recoveries. */
	private ArrayList<Recovery> recoveries;

	@Override
	public void decidePlacement() {
		LOG.info("deciding data placement in LVFS...");
		// use a fixed random seed to determine the placement.
		// should be okay. we are iterating over various schedules, not placement.
		Random random = new Random(123456L);
		
		blocksInNodes = new ArrayList<ArrayList<BlockId>>();
		for (int i = 0; i < config.nodes; ++i) {
			blocksInNodes.add (new ArrayList<BlockId>());
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
							int partitionsEnd = (int) (partitions * (couple + 1) / couples) - placedPartitions;
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
	
	private void doRecoveries (double elapsed) {
		
	}

	private void addNodeFailure (int nodeId) throws DataLostException {
		assert (nodeId >= 0);
		assert (nodeId < config.nodes);
		// check if the node is already being recovered
		boolean exists = false;
		ArrayList<BlockId> blocks = blocksInNodes.get(nodeId);
		double gbToRecover = blocks.size() * COLUMN_FILE_SIZE * COLUMNS_PER_TABLE;
		for (Recovery recovery : recoveries) {
			if (recovery instanceof Copy) {
				Copy copy = (Copy) recovery;
				if (copy.nodeId == nodeId) {
					recovery.remainingGigabytes = gbToRecover;
					exists = true;
					break;
				}
			}
		}
		if (!exists) {
			// this node newly crashes
			for (BlockId block : blocks) {
				ReplicaGroupFractureStatus status = allStatus[block.table][block.fracture][block.group];
				--status.redundancies[block.partition];
				assert (status.redundancies[block.partition] >= 0);
				if (status.redundancies[block.partition] == 0) {
					// oops, this partition is lost from this replica group!
					if (status.repartitionTask != null) {
						// 
						continue;
					}
				}
				throw new DataLostException("block " + block + " has been permanently lost because of a failure in node " + nodeId + "!");
			}
		}
	}
	
	@Override
	protected double simulateTimeToFail(FailureSchedule schedule) {
		LOG.debug("running one schedule...");
		recoveries = new ArrayList<Recovery>();
		assert (schedule.getNow() == 0.0d);
		try {
			while (true) {
				FailureEvent event = schedule.generateNextEvent();
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
			LOG.info("Simulated a data loss! now=" + schedule.getNow() + ". " + ex.getMessage());
			return schedule.getNow();
		}

		LOG.debug("ran schedule without data loss.");
		return Double.POSITIVE_INFINITY;
	}
}

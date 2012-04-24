package edu.brown.lasvegas.costmodels.recovery;

import org.apache.log4j.Logger;

import edu.brown.lasvegas.costmodels.recovery.sim.ExperimentalConfiguration;
import edu.brown.lasvegas.costmodels.recovery.sim.LvfsPlacementParameters;

/**
 * LVFS's analytic model for recoverability.
 */
public class LvfsRecoverabilityEstimator {
    private static Logger LOG = Logger.getLogger(LvfsRecoverabilityEstimator.class);
	private final ExperimentalConfiguration config;
	private final LvfsPlacementParameters policy;
	public LvfsRecoverabilityEstimator(ExperimentalConfiguration config, LvfsPlacementParameters policy) {
		this.config = config;
		this.policy = policy;
		init ();
	}

	/** expected time to recover a failed node, ignoring repartitioning. */
	private double nodeMeanTimeToRecover;
	/** expected time to recover a failed rack, ignoring repartitioning. . */
	private double rackMeanTimeToRecover;
	/** total number of replicas. */
	private int replicationFactor;
	
	private void init () {
		replicationFactor = 0;
		for (int schemes : policy.replicaSchemes) {
			replicationFactor += schemes;
		}
		
		double nodeStorage = config.gigabytesPerNode * replicationFactor;

		// calculate nodeMeanTimeToRecover. this is an iterative approximation
		double averageNodesFailing = 0d;
		for (int i = 0; i < 20; ++i) {
			double networkRateOnNodeFailure = config.getNetworkRate((int) Math.round(1.0d + averageNodesFailing));
			double recoveryRateOnNodeFailure = config.getCombinedRate(networkRateOnNodeFailure);
			nodeMeanTimeToRecover = nodeStorage / recoveryRateOnNodeFailure;

			double networkRateOnRackFailure = config.getNetworkRate((int) Math.round(config.nodesPerRack + averageNodesFailing));
			double recoveryRateOnRackFailure = config.getCombinedRate(networkRateOnRackFailure);
			rackMeanTimeToRecover = nodeStorage / recoveryRateOnRackFailure; // if network bandwidth is enough, this is same as nodeMeanTimeToRecover

			double newAverageNodesFailing = config.nodes * nodeMeanTimeToRecover / (config.nodeMeanTimeToFail + nodeMeanTimeToRecover)
				+ config.nodesPerRack * config.racks * rackMeanTimeToRecover / (config.rackMeanTimeToFail + rackMeanTimeToRecover);
			if (newAverageNodesFailing == averageNodesFailing) {
				break; // converged
			}
			averageNodesFailing = newAverageNodesFailing;
		}
		LOG.debug("nodeMeanTimeToRecove=" + nodeMeanTimeToRecover + " minutes, rackMeanTimeToRecover=" + rackMeanTimeToRecover + " minutes");
		
		assert (policy.buddyExclusion);
		assert (policy.nodeCoupling);
		// we assume node coupling and buddy exclusion.
	}
	
	/**
	 * Estimate the number of permanent data loss events during maxSimulationPeriod. 
	 */
	public double estimateFailureCount () {
		// how many times the first replica group in some fracture will fail?
		double replicaGroupFailures = calculateGroupFailureCountPerFracture (policy.replicaSchemes[0]);

		// consider the event where all other replica groups also fail during repartitioning
		double fractureSize = config.gigabytesTotal / config.tables / policy.fracturesPerTable;
		double repartitioningRate = config.localRepartition * policy.racksPerGroup * config.nodesPerRack;
		double repartitioningTime = fractureSize / repartitioningRate;
		LOG.debug("fractureSize=" + fractureSize + " GB, repartitioningTime=" + repartitioningTime + " minutes");
		
		for (int other = 1; other < policy.replicaSchemes.length; ++other) {
			double otherReplicaGroupFailures = calculateGroupFailureCountPerFracture (policy.replicaSchemes[other]);
			double groupFailureMttf = config.maxSimulationPeriod / otherReplicaGroupFailures;
			replicaGroupFailures *= integrateExponentialDistribution(repartitioningTime, groupFailureMttf);
		}
		return replicaGroupFailures * config.tables * policy.fracturesPerTable;
	}
	
	private double calculateGroupFailureCountPerFracture (int buddySchemes) {
		if (buddySchemes == 1) {
			// then ANY node/rack failure will cause data loss! 
			return config.maxSimulationPeriod / config.nodeMeanTimeToFail * policy.racksPerGroup * config.nodesPerRack
				+ config.maxSimulationPeriod / config.rackMeanTimeToFail * policy.racksPerGroup;
		}
		
		// consider one couple (we consider other couples later) of racks assigned to replica schemes in this group. 
		// how many times we see dataloss among them?
		double datalossCount = 0;
		
		// let's calculate the occurrences of events where the first node/rack fails and also its coupled nodes/racks
		// fail during its recovery time.
		{
			// we start with the node failure count.
			double firstNodeFailures = config.maxSimulationPeriod / config.nodeMeanTimeToFail * config.nodesPerRack;
			
			// probability that a particular node (or the rack containing the rack) fails during the first node recovery
			double combinedMttf = 1.0d / ((1.0d / config.nodeMeanTimeToFail) + (1.0d / config.rackMeanTimeToFail));
			assert (combinedMttf < config.nodeMeanTimeToFail);
			assert (combinedMttf < config.rackMeanTimeToFail);
			double nodeFailureProb = integrateExponentialDistribution (nodeMeanTimeToRecover, combinedMttf);
			
			datalossCount += firstNodeFailures * Math.pow(nodeFailureProb, buddySchemes - 1);
		}
		{
			// what's missing above is the case where all racks had concurrent rack failures 
			double firstRackFailures = config.maxSimulationPeriod / config.rackMeanTimeToFail;
			
			// probability that a particular rack fails during the first rack recovery
			double rackFailureProb = integrateExponentialDistribution (rackMeanTimeToRecover, config.rackMeanTimeToFail);
			
			datalossCount += firstRackFailures * Math.pow(rackFailureProb, buddySchemes - 1);
		}
		// if the group has 6 racks assigned and has 3 buddy schemes,
		// we have 6/3=2 such rack couples. So, we multiply by that.
		datalossCount *= (double) policy.racksPerGroup / buddySchemes;

		return datalossCount;
	}
	/** returns the probability to fail during the duration, assuming the mean. */
	private static double integrateExponentialDistribution (double duration, double mttf) {
		// f(y) = exp(-y/mttf)/mttf
		// int[f(y)]_0^t = [-exp(-y/mttf)]_0^t = 1 - exp (-t/mttf)
		return 1.0d - Math.exp(-duration / mttf);
	}
}

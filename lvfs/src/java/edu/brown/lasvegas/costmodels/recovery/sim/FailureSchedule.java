package edu.brown.lasvegas.costmodels.recovery.sim;

import java.util.Random;

import org.apache.log4j.Logger;

/**
 * A pre-determined schedule of which racks and nodes will fail when.
 * To be fair, the same schedule is used to compare the data loss risks of
 * two data placement policies.
 */
public class FailureSchedule {
	public FailureSchedule (ExperimentalConfiguration config, long randomSeed) {
		this.config = config;
		this.random = new Random(randomSeed);

		// assuming independence between each node failure and rack failure,
		// and also ignoring the reduction of contribution from the currently failing nodes/racks,
		// we apply Matthiessen's rule: 1/MTTF_combined = 1/MTTF_1 + 1/MTTF_2 + ...
		this.nodeFailureContribution = (double) config.nodes / config.nodeMeanTimeToFail;
		this.rackFailureContribution = (double) config.racks / config.rackMeanTimeToFail;
		this.meanTimeToFailCombined = 1.0d / (nodeFailureContribution + rackFailureContribution);
		this.nodeFailureFraction = nodeFailureContribution / (nodeFailureContribution + rackFailureContribution);
		LOG.info("generating failure schedule for " + config.maxSimulationPeriod + " minutes..");
	}
	
    private static Logger LOG = Logger.getLogger(FailureSchedule.class);
	public final ExperimentalConfiguration config;
	private final Random random;
	private int eventCount = 0, nodeEventCount = 0;
	private double now = 0;
	
	private final double nodeFailureContribution;
	private final double rackFailureContribution;
	private final double meanTimeToFailCombined;
	private final double nodeFailureFraction;

	public static class FailureEvent {
		public FailureEvent (boolean rackFailure, int failedNode, double interval) {
			this.rackFailure = rackFailure;
			this.failedNode = failedNode;
			this.interval = interval;
		}
		/** whether the event is a rack failure or not (=node failure).*/
		public final boolean rackFailure;
		/** ID of the failed node or rack. */
		public final int failedNode;
		/** time from the previous event in minutes.*/
		public final double interval;
	}

	public FailureEvent generateNextEvent() {
		if (now > config.maxSimulationPeriod) {
			return null;
		}
		// Now, let's assume exponential distribution of the failure.
		// Thanks to the memory-less property of exponential distribution,
		// we can simplify the failure schedule generation as follows
		double interval = Math.log(1.0d - random.nextDouble()) * -meanTimeToFailCombined;
		now += interval;
		if (now > config.maxSimulationPeriod) {
			now = config.maxSimulationPeriod;
			debugOut();
			return null;
		}
		++eventCount;
		if (random.nextDouble() < nodeFailureFraction) {
			++nodeEventCount;
			return new FailureEvent(false, random.nextInt(config.nodes), interval);
		} else {
			return new FailureEvent(true, random.nextInt(config.racks), interval);
		}
	}
	public double getNow () {
		return now;
	}
	public int getEventCount () {
		return eventCount;
	}
	public int getNodeEventCount () {
		return nodeEventCount;
	}
	public int getRackEventCount () {
		return eventCount - nodeEventCount;
	}
	
	public void debugOut () {
		LOG.info("now=" + now + ". generated " + eventCount + " events (among them, rack failures=" + getRackEventCount() + ")");
	}
}

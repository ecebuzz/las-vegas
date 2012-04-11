package edu.brown.lasvegas.costmodels.recovery.sim;

import java.util.ArrayList;
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
		generateSchedule();
	}
	
	public final ExperimentalConfiguration config;
	private final Random random;
	public final ArrayList<FailureEvent> events = new ArrayList<FailureEvent>();
    private static Logger LOG = Logger.getLogger(FailureSchedule.class);
	
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

	private void generateSchedule() {
		// assuming independence between each node failure and rack failure,
		// and also ignoring the reduction of contribution from the currently failing nodes/racks,
		// we apply Matthiessen's rule: 1/MTTF_combined = 1/MTTF_1 + 1/MTTF_2 + ...
		final double nodeFailureContribution = (double) config.nodes / config.nodeMeanTimeToFail;
		final double rackFailureContribution = (double) config.racks / config.rackMeanTimeToFail;
		final double meanTimeToFailCombined = 1.0d / (nodeFailureContribution + rackFailureContribution);
		final double nodeFailureFraction = nodeFailureContribution / (nodeFailureContribution + rackFailureContribution);
		LOG.info("generating failure schedule for " + config.maxSimulationPeriod + " minutes..");
		
		// Now, let's assume exponential distribution of the failure.
		// Thanks to the memory-less property of exponential distribution,
		// we can simplify the failure schedule generation as follows
		int nodeFailureCount = 0;
		for (double now = 0; now < config.maxSimulationPeriod; ) {
			double interval = Math.log(1.0d - random.nextDouble()) / -meanTimeToFailCombined;
			if (random.nextDouble() < nodeFailureFraction) {
				++nodeFailureCount;
				events.add (new FailureEvent(false, random.nextInt(config.nodes), interval));
			} else {
				events.add (new FailureEvent(true, random.nextInt(config.racks), interval));
			}
		}

		LOG.info("generated failure schedule. " + events.size() + " events (among them, rack failures=" + (events.size() - nodeFailureCount) + ")");
	}
}

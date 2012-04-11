package edu.brown.lasvegas.costmodels.recovery.sim;

import java.util.ArrayList;
import java.util.Random;

import org.apache.log4j.Logger;

public abstract class Simulator {
    private static Logger LOG = Logger.getLogger(Simulator.class);
	public Simulator(ExperimentalConfiguration config, long firstRandomSeed) {
		this.config = config;
		this.firstRandomSeed = firstRandomSeed;
	}
	protected final ExperimentalConfiguration config;
	protected final long firstRandomSeed;

	public abstract void decidePlacement ();

	/** simulate one failure schedule and returns the fail time with the schedule. +Infinity when max-simulation-period elapses. */
	protected abstract double simulateTimeToFail (FailureSchedule schedule);

	public ArrayList<Double> simulateMeanTimeToFail (int iterations) {
		LOG.info("Started " + iterations + " iterations");
		ArrayList<Double> results = new ArrayList<Double>();
		Random seedGenerator = new Random(firstRandomSeed);
		for (int i = 0; i < iterations; ++i) {
			FailureSchedule schedule = new FailureSchedule(config, seedGenerator.nextLong());
			results.add(simulateTimeToFail(schedule));
		}
		LOG.info("Done iterations");
		return results;
	}
}


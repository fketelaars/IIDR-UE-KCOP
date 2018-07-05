package com.ibm.replication.cdc.userexit.kcop.util;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

public class NanoClock extends Clock {

	private final Clock clock;
	private final long initialNanos;
	private final Instant initialInstant;

	public NanoClock() {
		this(Clock.systemUTC());
	}

	private NanoClock(final Clock clock) {
		this.clock = clock;
		initialInstant = clock.instant();
		initialNanos = getSystemNanos();
	}

	@Override
	public ZoneId getZone() {
		return clock.getZone();
	}

	@Override
	public Instant instant() {
		return initialInstant.plusNanos(getSystemNanos() - initialNanos);
	}

	@Override
	public NanoClock withZone(final ZoneId zone) {
		return new NanoClock(clock.withZone(zone));
	}

	private long getSystemNanos() {
		return System.nanoTime();
	}

}

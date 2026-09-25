package com.github.highcumontoa.jobschedulerquotajava.tests.support;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;

/** 可控时钟：测试里手动推进时间，稳定复现租约超时。 */
public class ManualClock extends Clock {

    private final AtomicLong now;

    public ManualClock(long startMs) {
        this.now = new AtomicLong(startMs);
    }

    @Override
    public long millis() {
        return now.get();
    }

    public void advanceMs(long delta) {
        now.addAndGet(delta);
    }

    public void setMs(long t) {
        now.set(t);
    }

    @Override
    public ZoneId getZone() {
        return ZoneId.of("UTC");
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(now.get());
    }
}

package com.artos.inttests.core.sim.perf;

import com.exametrika.common.json.JsonObject;
import com.exametrika.common.json.JsonObjectBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PerfPeriodType {
    private final String name;
    private final long period;
    private final Map<String, IPerfObject> meters = new HashMap<>();

    public PerfPeriodType(String name, long period) {
        this.name = name;
        this.period = period;
        if (period != 0)
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> reset(), period, period, TimeUnit.MILLISECONDS);
    }

    public String getName() {
        return name;
    }

    public long getPeriod() {
        return period;
    }

    public synchronized IMeter getMeter(String name) {
        IPerfObject meter = meters.get(name);
        if (meter == null) {
            meter = new PerfCounter(name);
            meters.put(name, meter);
        }

        return meter;
    }

    public synchronized JsonObject getStatistics() {
        JsonObjectBuilder stats = new JsonObjectBuilder();
        for (IPerfObject meter : new TreeMap<>(meters).values())
            stats.put(meter.getName(), meter.getStatistics(period));

        return stats;
    }

    private synchronized void reset() {
        for (IPerfObject meter : meters.values())
            meter.reset();
    }
}

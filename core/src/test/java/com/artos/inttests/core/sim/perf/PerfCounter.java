package com.artos.inttests.core.sim.perf;

import com.exametrika.common.json.JsonObject;
import com.exametrika.common.json.JsonObjectBuilder;

public class PerfCounter implements IPerfObject {
    private final String name;
    private long count;
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    private long sum;

    public PerfCounter(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public synchronized void measure(long value) {
        count++;
        sum += value;
        if (min > value)
            min = value;
        if (max < value)
            max = value;
    }

    @Override
    public synchronized void reset() {
        count = 0;
        sum = 0;
        min = Long.MAX_VALUE;
        max = Long.MIN_VALUE;
    }

    @Override
    public JsonObject getStatistics(long period) {
        JsonObjectBuilder stats = new JsonObjectBuilder();
        if (count > 0) {
            stats.put("count", count);
            stats.put("min", min);
            stats.put("max", max);
            stats.put("sum", sum);
            stats.put("avg", (double) sum / count);

            if (period != 0) {
                stats.put("rateCount(m)", (double) count * 60000 / period);
                stats.put("rateSum(m)", (double) sum * 60000 / period);
                stats.put("rateAvg(m)", (double) sum / count * 60000 / period);
            }
        }

        return stats;
    }
}

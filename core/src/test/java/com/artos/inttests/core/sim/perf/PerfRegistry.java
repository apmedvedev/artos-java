package com.artos.inttests.core.sim.perf;

import com.exametrika.common.json.JsonObject;
import com.exametrika.common.json.JsonObjectBuilder;

import java.util.ArrayList;
import java.util.List;

public class PerfRegistry {
    private final List<PerfPeriodType> periodTypes;
    private final boolean enabled;

    public PerfRegistry(List<Integer> periodTypesPeriods, boolean enabled) {
        List<PerfPeriodType> periodTypes = new ArrayList<>();
        for (Integer period : periodTypesPeriods)
            periodTypes.add(new PerfPeriodType(getPeriodName(period), period * 60000L));

        this.periodTypes = periodTypes;
        this.enabled = enabled;
    }

    public IMeter getMeter(String name) {
        List<IMeter> meters = new ArrayList<>();
        for (PerfPeriodType periodType : periodTypes)
            meters.add(periodType.getMeter(name));

        return new PerfCompositeMeter(name, meters, enabled);
    }

    public JsonObject getStatistics() {
        JsonObjectBuilder stats = new JsonObjectBuilder();
        for (PerfPeriodType periodType : periodTypes)
            stats.put(periodType.getName(), periodType.getStatistics());

        return stats;
    }

    private String getPeriodName(int period) {
        if (period != 0)
            return Long.toString(period) + "(m)";
        else
            return "total";
    }
}

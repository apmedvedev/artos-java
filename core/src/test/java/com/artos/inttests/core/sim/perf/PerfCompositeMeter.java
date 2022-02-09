package com.artos.inttests.core.sim.perf;

import java.util.List;

public class PerfCompositeMeter implements IMeter {
    private final String name;
    private final List<IMeter> meters;
    private final boolean enabled;

    public PerfCompositeMeter(String name, List<IMeter> meters, boolean enabled) {
        this.name = name;
        this.meters = meters;
        this.enabled = enabled;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void measure(long value) {
        if (!enabled)
            return;

        for (IMeter meter : meters)
            meter.measure(value);
    }
}

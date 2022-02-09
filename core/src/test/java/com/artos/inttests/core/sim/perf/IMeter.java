package com.artos.inttests.core.sim.perf;

public interface IMeter {
    String getName();

    void measure(long value);
}

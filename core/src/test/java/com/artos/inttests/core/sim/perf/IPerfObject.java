package com.artos.inttests.core.sim.perf;

import com.exametrika.common.json.JsonObject;

public interface IPerfObject extends IMeter {
    void reset();

    JsonObject getStatistics(long period);
}

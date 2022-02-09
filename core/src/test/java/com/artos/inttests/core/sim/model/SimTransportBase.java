package com.artos.inttests.core.sim.model;

import com.artos.inttests.core.sim.perf.IMeter;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.exametrika.common.utils.ByteArray;

public class SimTransportBase {
    private final IMeter requestsBytesMeter;
    private final IMeter responsesMeter;
    private final IMeter failuresMeter;

    public SimTransportBase(String prefix, String endpoint, PerfRegistry perfRegistry) {
        this.requestsBytesMeter = perfRegistry.getMeter(prefix + ":" + endpoint + ".requests(bytes)");
        this.responsesMeter = perfRegistry.getMeter(prefix + ":" + endpoint + ".responses(bytes)");
        this.failuresMeter = perfRegistry.getMeter(prefix + ":" + endpoint + ".failures(counts)");
    }

    protected void measureRequest(ByteArray request) {
        requestsBytesMeter.measure(request.getLength());
    }

    protected void measureFailure(Throwable error) {
        failuresMeter.measure(1);
    }

    protected void measureResponse(ByteArray response) {
        responsesMeter.measure(response.getLength());
    }
}

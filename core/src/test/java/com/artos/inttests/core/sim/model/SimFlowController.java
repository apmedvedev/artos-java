package com.artos.inttests.core.sim.model;

import com.artos.spi.core.FlowType;
import com.exametrika.common.tasks.IFlowController;
import com.exametrika.common.utils.Assert;

public class SimFlowController implements IFlowController<FlowType> {
    private final SimPublishDriver publishDriver;
    private final SimQueryDriver queryDriver;

    public SimFlowController(SimPublishDriver publishDriver, SimQueryDriver queryDriver) {
        this.publishDriver = publishDriver;
        this.queryDriver = queryDriver;
    }

    @Override
    public void lockFlow(FlowType flow) {
        switch (flow) {
            case PUBLISH: {
                publishDriver.lockFlow(flow);
                return;
            }
            case QUERY: {
                queryDriver.lockFlow(flow);
                return;
            }
            default:
                Assert.error();
                return;
        }
    }

    @Override
    public void unlockFlow(FlowType flow) {
        switch (flow) {
            case PUBLISH: {
                publishDriver.unlockFlow(flow);
                return;
            }
            case QUERY: {
                queryDriver.unlockFlow(flow);
                return;
            }
            default:
                Assert.error();
                return;
        }
    }
}

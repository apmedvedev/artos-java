package com.artos.tests.core.mocks;

import com.artos.spi.core.FlowType;
import com.exametrika.common.tasks.IFlowController;

public class FlowControllerMock implements IFlowController<FlowType> {
    private boolean flowLocked;
    private Object flow;

    public boolean isFlowLocked() {
        return flowLocked;
    }

    public Object getFlow() {
        return flow;
    }

    @Override
    public void lockFlow(FlowType flow) {
        this.flow = flow;
        flowLocked = true;
    }

    @Override
    public void unlockFlow(FlowType flow) {
        this.flow = flow;
        flowLocked = false;
    }
}

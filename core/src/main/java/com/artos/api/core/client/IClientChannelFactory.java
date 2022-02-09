package com.artos.api.core.client;

import com.artos.api.core.client.conf.ClientChannelConfiguration;
import com.artos.spi.core.FlowType;
import com.exametrika.common.tasks.IFlowController;

public interface IClientChannelFactory {
    IClientChannel createChannel(ClientChannelConfiguration configuration, IFlowController<FlowType> flowController);
}

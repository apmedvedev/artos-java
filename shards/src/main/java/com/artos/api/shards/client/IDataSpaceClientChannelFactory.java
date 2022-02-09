package com.artos.api.shards.client;

import com.artos.spi.core.FlowType;
import com.artos.spi.shards.conf.DataSpaceClientChannelConfiguration;
import com.exametrika.common.tasks.IFlowController;

public interface IDataSpaceClientChannelFactory<K> {
    IDataSpaceClientChannel<K> createChannel(DataSpaceClientChannelConfiguration<K> configuration, IFlowController<FlowType> flowController);
}

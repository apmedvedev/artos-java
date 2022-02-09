package com.artos.impl.shards.client.channel;

import com.artos.api.core.client.conf.ClientChannelFactoryConfiguration;
import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.shards.client.IDataSpaceClientChannelFactory;
import com.artos.api.shards.client.conf.StaticDataSpaceClientChannelConfiguration;
import com.artos.impl.core.client.ClientProtocol;
import com.artos.impl.core.client.IClientProtocol;
import com.artos.impl.core.client.PublishClientProtocol;
import com.artos.impl.core.client.QueryClientProtocol;
import com.artos.spi.core.FlowType;
import com.artos.spi.core.IClientStore;
import com.artos.spi.core.IMessageSenderFactory;
import com.artos.spi.shards.conf.DataSpaceClientChannelConfiguration;
import com.exametrika.common.compartment.ICompartment;
import com.exametrika.common.compartment.ICompartmentFactory;
import com.exametrika.common.compartment.ICompartmentFactory.Parameters;
import com.exametrika.common.compartment.impl.CompartmentFactory;
import com.exametrika.common.tasks.IFlowController;
import com.exametrika.common.tasks.impl.ReentrantFlowController;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Exceptions;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class StaticDataSpaceClientChannelFactory<K> implements IDataSpaceClientChannelFactory<K> {
    private final ClientChannelFactoryConfiguration factoryConfiguration;

    public StaticDataSpaceClientChannelFactory(ClientChannelFactoryConfiguration factoryConfiguration) {
        Assert.notNull(factoryConfiguration);

        this.factoryConfiguration = factoryConfiguration;
    }

    @Override
    public StaticDataSpaceClientChannel<K> createChannel(DataSpaceClientChannelConfiguration<K> configuration,
                                                         IFlowController<FlowType> flowController) {
        Assert.notNull(configuration);

        StaticDataSpaceClientChannelConfiguration<K> channelConfiguration = (StaticDataSpaceClientChannelConfiguration<K>) configuration;

        IMessageSenderFactory messageSenderFactory = channelConfiguration.getMessageSender().createFactory();
        UUID clientId = UUID.randomUUID();

        if (flowController != null) {
            flowController = new ReentrantFlowController<>(flowController);
        }

        IClientStore clientStore = null;
        if (channelConfiguration.getClientStore() != null)
            clientStore = channelConfiguration.getClientStore().createClientStore();

        String endpoint;
        if (configuration.getEndpoint() != null)
            endpoint = configuration.getEndpoint();
        else
            endpoint = getHostName();

        Parameters compartmentParameters = new ICompartmentFactory.Parameters();
        compartmentParameters.name = endpoint;
        compartmentParameters.dispatchPeriod = factoryConfiguration.getTimerPeriod();

        List<IClientProtocol> clientProtocols = new ArrayList<>();
        for (GroupConfiguration groupConfiguration : channelConfiguration.getGroups()) {
            ClientProtocol clientProtocol = new ClientProtocol(messageSenderFactory, groupConfiguration,
                endpoint, clientId, groupConfiguration.getGroupId(),
                factoryConfiguration.getHeartbeatPeriod(), factoryConfiguration.getUpdateConfigurationPeriod(),
                factoryConfiguration.getServerTimeout(), clientStore);

            PublishClientProtocol publishProtocol = new PublishClientProtocol(clientId, groupConfiguration.getGroupId(),
                factoryConfiguration.getLockQueueCapacity(), factoryConfiguration.getUnlockQueueCapacity(),
                factoryConfiguration.getMaxValueSize(), factoryConfiguration.getMaxBatchSize());

            QueryClientProtocol queryProtocol = new QueryClientProtocol(messageSenderFactory, endpoint, clientId,
                groupConfiguration.getGroupId(),
                factoryConfiguration.getLockQueueCapacity(), factoryConfiguration.getUnlockQueueCapacity(),
                factoryConfiguration.getMaxValueSize(), factoryConfiguration.getMaxBatchSize(),
                factoryConfiguration.getServerTimeout(), factoryConfiguration.getHeartbeatPeriod(),
                factoryConfiguration.getResendQueryPeriod(), factoryConfiguration.getResubscriptionPeriod());

            compartmentParameters.processors.add(publishProtocol);
            compartmentParameters.processors.add(queryProtocol);
            compartmentParameters.timerProcessors.add(clientProtocol);
            compartmentParameters.timerProcessors.add(queryProtocol);

            clientProtocol.setPublishProtocol(publishProtocol);
            clientProtocol.setQueryProtocol(queryProtocol);

            publishProtocol.setClientProtocol(clientProtocol);
            if (flowController != null)
                publishProtocol.setFlowController(flowController);

            queryProtocol.setClientProtocol(clientProtocol);
            queryProtocol.setPublishProtocol(publishProtocol);
            if (flowController != null)
                queryProtocol.setFlowController(flowController);

            clientProtocols.add(clientProtocol);
        }

        ICompartment compartment = new CompartmentFactory().createCompartment(compartmentParameters);

        for (IClientProtocol clientProtocol : clientProtocols) {
            ((ClientProtocol)clientProtocol).setCompartment(compartment);

            ((QueryClientProtocol)clientProtocol.getQueryProtocol()).setCompartment(compartment);
        }

        StaticDataSpaceClientChannel<K> clientChannel = new StaticDataSpaceClientChannel(endpoint, clientProtocols,
            configuration.getHashFunction().createHashFunction(), compartment);

        return clientChannel;
    }

    private String getHostName() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (Exception e) {
            return Exceptions.wrapAndThrow(e);
        }
    }
}

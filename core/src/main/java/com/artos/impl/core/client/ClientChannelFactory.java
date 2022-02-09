package com.artos.impl.core.client;

import com.artos.api.core.client.IClientChannelFactory;
import com.artos.api.core.client.conf.ClientChannelConfiguration;
import com.artos.api.core.client.conf.ClientChannelFactoryConfiguration;
import com.artos.spi.core.FlowType;
import com.artos.spi.core.IClientStore;
import com.artos.spi.core.IMessageSenderFactory;
import com.exametrika.common.compartment.ICompartment;
import com.exametrika.common.compartment.ICompartmentFactory;
import com.exametrika.common.compartment.ICompartmentFactory.Parameters;
import com.exametrika.common.compartment.impl.CompartmentFactory;
import com.exametrika.common.tasks.IFlowController;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Exceptions;

import java.net.InetAddress;
import java.util.UUID;

public class ClientChannelFactory implements IClientChannelFactory {
    private final ClientChannelFactoryConfiguration factoryConfiguration;

    public ClientChannelFactory(ClientChannelFactoryConfiguration factoryConfiguration) {
        Assert.notNull(factoryConfiguration);

        this.factoryConfiguration = factoryConfiguration;
    }

    @Override
    public ClientChannel createChannel(ClientChannelConfiguration configuration, IFlowController<FlowType> flowController) {
        Assert.notNull(configuration);

        IMessageSenderFactory messageSenderFactory = configuration.getMessageSender().createFactory();
        UUID clientId = UUID.randomUUID();

        IClientStore clientStore = null;
        if (configuration.getClientStore() != null)
            clientStore = configuration.getClientStore().createClientStore();

        String endpoint;
        if (configuration.getEndpoint() != null)
            endpoint = configuration.getEndpoint();
        else
            endpoint = getHostName();

        ClientProtocol clientProtocol = new ClientProtocol(messageSenderFactory, configuration.getGroup(),
            endpoint, clientId, configuration.getGroup().getGroupId(),
            factoryConfiguration.getHeartbeatPeriod(), factoryConfiguration.getUpdateConfigurationPeriod(),
            factoryConfiguration.getServerTimeout(), clientStore);

        PublishClientProtocol publishProtocol = new PublishClientProtocol(clientId, configuration.getGroup().getGroupId(),
                factoryConfiguration.getLockQueueCapacity(), factoryConfiguration.getUnlockQueueCapacity(),
                factoryConfiguration.getMaxValueSize(), factoryConfiguration.getMaxBatchSize());

        QueryClientProtocol queryProtocol = new QueryClientProtocol(messageSenderFactory, endpoint, clientId,
                configuration.getGroup().getGroupId(),
                factoryConfiguration.getLockQueueCapacity(), factoryConfiguration.getUnlockQueueCapacity(),
                factoryConfiguration.getMaxValueSize(), factoryConfiguration.getMaxBatchSize(),
                factoryConfiguration.getServerTimeout(), factoryConfiguration.getHeartbeatPeriod(),
                factoryConfiguration.getResendQueryPeriod(), factoryConfiguration.getResubscriptionPeriod());

        Parameters compartmentParameters = new ICompartmentFactory.Parameters();
        compartmentParameters.name = endpoint;
        compartmentParameters.dispatchPeriod = factoryConfiguration.getTimerPeriod();
        compartmentParameters.processors.add(publishProtocol);
        compartmentParameters.processors.add(queryProtocol);
        compartmentParameters.timerProcessors.add(clientProtocol);
        compartmentParameters.timerProcessors.add(queryProtocol);
        ICompartment compartment = new CompartmentFactory().createCompartment(compartmentParameters);

        clientProtocol.setPublishProtocol(publishProtocol);
        clientProtocol.setQueryProtocol(queryProtocol);
        clientProtocol.setCompartment(compartment);

        publishProtocol.setClientProtocol(clientProtocol);
        if (flowController != null)
            publishProtocol.setFlowController(flowController);

        queryProtocol.setClientProtocol(clientProtocol);
        queryProtocol.setPublishProtocol(publishProtocol);
        queryProtocol.setCompartment(compartment);
        if (flowController != null)
            queryProtocol.setFlowController(flowController);

        ClientChannel clientChannel = new ClientChannel(endpoint, clientProtocol, compartment);

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

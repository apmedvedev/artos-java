package com.artos.api.core.client.conf;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.spi.core.conf.ClientStoreConfiguration;
import com.artos.spi.core.conf.MessageSenderFactoryConfiguration;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Objects;

public class ClientChannelConfiguration {
    private final String endpoint;
    private final GroupConfiguration group;
    private final MessageSenderFactoryConfiguration messageSender;
    private final ClientStoreConfiguration clientStore;

    public ClientChannelConfiguration(String endpoint, GroupConfiguration group, MessageSenderFactoryConfiguration messageSender,
                                      ClientStoreConfiguration clientStore) {
        Assert.notNull(group);
        Assert.notNull(messageSender);

        this.endpoint = endpoint;
        this.group = group;
        this.messageSender = messageSender;
        this.clientStore = clientStore;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public GroupConfiguration getGroup() {
        return group;
    }

    public MessageSenderFactoryConfiguration getMessageSender() {
        return messageSender;
    }

    public ClientStoreConfiguration getClientStore() {
        return clientStore;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ClientChannelConfiguration))
            return false;

        ClientChannelConfiguration configuration = (ClientChannelConfiguration) o;
        return group.equals(configuration.group) && messageSender.equals(configuration.messageSender) &&
            Objects.equals(clientStore, configuration.clientStore);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(group, messageSender, clientStore);
    }
}

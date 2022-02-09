package com.artos.api.shards.client.conf;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.spi.core.conf.ClientStoreConfiguration;
import com.artos.spi.core.conf.MessageSenderFactoryConfiguration;
import com.artos.spi.shards.conf.DataSpaceClientChannelConfiguration;
import com.artos.spi.shards.conf.HashFunctionConfiguration;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Immutables;
import com.exametrika.common.utils.Objects;

import java.util.List;

public class StaticDataSpaceClientChannelConfiguration<K> extends DataSpaceClientChannelConfiguration<K> {
    private final List<GroupConfiguration> groups;
    private final MessageSenderFactoryConfiguration messageSender;
    private final ClientStoreConfiguration clientStore;

    public StaticDataSpaceClientChannelConfiguration(String endpoint, List<GroupConfiguration> groups,
                                                     HashFunctionConfiguration<K> hashFunction,
                                                     MessageSenderFactoryConfiguration messageSender,
                                                     ClientStoreConfiguration clientStore) {
        super(endpoint, hashFunction);

        Assert.notNull(groups);
        Assert.notNull(messageSender);

        this.groups = Immutables.wrap(groups);
        this.messageSender = messageSender;
        this.clientStore = clientStore;
    }

    public List<GroupConfiguration> getGroups() {
        return groups;
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
        if (!(o instanceof StaticDataSpaceClientChannelConfiguration))
            return false;

        StaticDataSpaceClientChannelConfiguration configuration = (StaticDataSpaceClientChannelConfiguration) o;
        return super.equals(o) && groups.equals(configuration.groups) &&
            messageSender.equals(configuration.messageSender) && Objects.equals(clientStore, configuration.clientStore);
    }

    @Override
    public int hashCode() {
        return super.hashCode() + 31 * Objects.hashCode(groups, messageSender, clientStore);
    }
}

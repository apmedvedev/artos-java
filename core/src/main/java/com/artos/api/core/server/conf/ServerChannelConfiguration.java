package com.artos.api.core.server.conf;

import com.artos.spi.core.conf.LogStoreFactoryConfiguration;
import com.artos.spi.core.conf.MessageListenerFactoryConfiguration;
import com.artos.spi.core.conf.MessageSenderFactoryConfiguration;
import com.exametrika.common.config.Configuration;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Objects;

public class ServerChannelConfiguration extends Configuration {
    private final ServerConfiguration server;
    private final GroupConfiguration group;
    private final LogStoreFactoryConfiguration logStore;
    private final MessageListenerFactoryConfiguration messageListener;
    private final MessageSenderFactoryConfiguration messageSender;
    private final Integer stateTransferPortRangeStart;
    private final Integer stateTransferPortRangeEnd;
    private final String stateTransferBindAddress;
    private final boolean stateTransferUseCompression;
    private final long stateTransferMaxSize;
    private final String workDir;
    private final boolean secured;
    private final String keyStoreFile;
    private final String keyStorePassword;

    public ServerChannelConfiguration(ServerConfiguration server, GroupConfiguration group,
                                      LogStoreFactoryConfiguration logStore, MessageListenerFactoryConfiguration messageListener,
                                      MessageSenderFactoryConfiguration messageSender,
                                      Integer stateTransferPortRangeStart, Integer stateTransferPortRangeEnd,
                                      String stateTransferBindAddress, boolean stateTransferUseCompression, long stateTransferMaxSize,
                                      String workDir, boolean secured, String keyStoreFile, String keyStorePassword) {
        Assert.notNull(server);
        Assert.notNull(group);
        Assert.notNull(logStore);
        Assert.notNull(messageListener);
        Assert.notNull(messageSender);
        Assert.notNull(workDir);
        if (secured) {
            Assert.notNull(keyStoreFile);
            Assert.notNull(keyStorePassword);
        }

        this.server = server;
        this.group = group;
        this.logStore = logStore;
        this.messageListener = messageListener;
        this.messageSender = messageSender;
        this.stateTransferPortRangeStart = stateTransferPortRangeStart;
        this.stateTransferPortRangeEnd = stateTransferPortRangeEnd;
        this.stateTransferBindAddress = stateTransferBindAddress;
        this.stateTransferUseCompression = stateTransferUseCompression;
        this.stateTransferMaxSize = stateTransferMaxSize;
        this.workDir = workDir;
        this.secured = secured;
        this.keyStoreFile = keyStoreFile;
        this.keyStorePassword = keyStorePassword;
    }

    public ServerConfiguration getServer() {
        return server;
    }

    public GroupConfiguration getGroup() {
        return group;
    }

    public LogStoreFactoryConfiguration getLogStore() {
        return logStore;
    }

    public MessageListenerFactoryConfiguration getMessageListener() {
        return messageListener;
    }

    public MessageSenderFactoryConfiguration getMessageSender() {
        return messageSender;
    }

    public Integer getStateTransferPortRangeStart() {
        return stateTransferPortRangeStart;
    }

    public Integer getStateTransferPortRangeEnd() {
        return stateTransferPortRangeEnd;
    }

    public String getStateTransferBindAddress() {
        return stateTransferBindAddress;
    }

    public boolean isStateTransferUseCompression() {
        return stateTransferUseCompression;
    }

    public long getStateTransferMaxSize() {
        return stateTransferMaxSize;
    }

    public String getWorkDir() {
        return workDir;
    }

    public boolean isSecured() {
        return secured;
    }

    public String getKeyStoreFile() {
        return keyStoreFile;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof ServerChannelConfiguration))
            return false;

        ServerChannelConfiguration configuration = (ServerChannelConfiguration) o;
        return server.equals(configuration.server) && group.equals(configuration.group) &&
            logStore.equals(configuration.logStore) &&
            messageListener.equals(configuration.messageListener) && messageSender.equals(configuration.messageSender) &&
            Objects.equals(stateTransferPortRangeStart, configuration.stateTransferPortRangeStart) &&
            Objects.equals(stateTransferPortRangeEnd, configuration.stateTransferPortRangeEnd) &&
            Objects.equals(stateTransferBindAddress, configuration.stateTransferBindAddress) &&
            stateTransferUseCompression == configuration.stateTransferUseCompression &&
            stateTransferMaxSize == configuration.stateTransferMaxSize &&
            workDir.equals(configuration.workDir) && secured == configuration.secured &&
            Objects.equals(keyStoreFile, configuration.keyStoreFile) &&
            Objects.equals(keyStorePassword, configuration.keyStorePassword);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(server, group, logStore, messageListener, messageSender,
            stateTransferPortRangeStart, stateTransferPortRangeEnd, stateTransferBindAddress,
            stateTransferUseCompression, stateTransferMaxSize, workDir, secured, keyStoreFile, keyStorePassword);
    }
}

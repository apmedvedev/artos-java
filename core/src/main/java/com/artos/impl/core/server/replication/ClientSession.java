package com.artos.impl.core.server.replication;

import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.SimpleDeque;
import com.exametrika.common.utils.SimpleList.Element;
import com.exametrika.common.utils.Times;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class ClientSession {
    private final Element<ClientSession> element = new Element<>(this);
    private final UUID clientId;
    private long lastReceivedMessageId;
    private long lastCommittedMessageId;
    private boolean outOfOrderReceived;
    private long lastAccessTime;
    private SimpleDeque<QueryInfo> queryQueue = new SimpleDeque<>();
    private boolean committed;
    private Set<UUID> subscriptions = new HashSet<>();
    private long lastUpdateSubscriptionTime;

    public ClientSession(UUID clientId) {
        Assert.notNull(clientId);

        this.clientId = clientId;
    }

    public Element<ClientSession> getElement() {
        return element;
    }

    public UUID getClientId() {
        return clientId;
    }

    public long getLastReceivedMessageId() {
        return lastReceivedMessageId;
    }

    public void setLastReceivedMessageId(long lastReceivedMessageId) {
        this.lastReceivedMessageId = lastReceivedMessageId;
    }

    public long getLastCommittedMessageId() {
        return lastCommittedMessageId;
    }

    public boolean isOutOfOrderReceived() {
        return outOfOrderReceived;
    }

    public void setOutOfOrderReceived(boolean outOfOrderReceived) {
        this.outOfOrderReceived = outOfOrderReceived;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public boolean isCommitted() {
        return committed;
    }

    public void setCommitted(boolean committed) {
        this.committed = committed;
    }

    public SimpleDeque<QueryInfo> getQueryQueue() {
        return queryQueue;
    }

    public void addQuery(QueryInfo query) {
        queryQueue.offer(query);
    }

    public Set<UUID> getSubscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(Set<UUID> subscriptions) {
        this.subscriptions = subscriptions;
        lastUpdateSubscriptionTime = Times.getCurrentTime();
    }

    public long getLastUpdateSubscriptionTime() {
        return lastUpdateSubscriptionTime;
    }

    public boolean receive(long messageId) {
        if (messageId == lastReceivedMessageId + 1) {
            lastReceivedMessageId = messageId;
            outOfOrderReceived = false;
            return true;
        } else if (messageId > lastReceivedMessageId + 1) {
            outOfOrderReceived = true;
        }

        return false;
    }

    public void commit(long messageId) {
        if (messageId > lastCommittedMessageId)
            lastCommittedMessageId = messageId;

        if (lastReceivedMessageId < lastCommittedMessageId) {
            lastReceivedMessageId = lastCommittedMessageId;
            outOfOrderReceived = false;
        }

        committed = true;
    }
}

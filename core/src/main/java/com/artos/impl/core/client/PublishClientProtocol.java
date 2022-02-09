/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.client;

import com.artos.impl.core.message.MessageFlags;
import com.artos.impl.core.message.PublishRequest;
import com.artos.impl.core.message.PublishRequestBuilder;
import com.artos.impl.core.message.PublishResponse;
import com.artos.spi.core.FlowType;
import com.artos.spi.core.LogEntry;
import com.artos.spi.core.LogValueType;
import com.exametrika.common.compartment.ICompartmentProcessor;
import com.exametrika.common.tasks.IFlowController;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.Enums;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.SimpleDeque;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class PublishClientProtocol implements IPublishClientProtocol, ICompartmentProcessor {
    private final UUID clientId;
    private final UUID groupId;
    private final int lockQueueCapacity;
    private final int unlockQueueCapacity;
    private final int maxValueSize;
    private final int maxBatchSize;
    private IFlowController<FlowType> flowController;
    private final SimpleDeque<EntryInfo> queue = new SimpleDeque<>();
    private IClientProtocol clientProtocol;
    private long nextMessageId = 1;
    private long lastSentMessageId;
    private long lastCommittedMessageId;
    private boolean flowLocked;

    public PublishClientProtocol(UUID clientId, UUID groupId, int lockQueueCapacity, int unlockQueueCapacity,
                                 int maxValueSize, int maxBatchSize) {
        Assert.notNull(clientId);
        Assert.notNull(groupId);
        Assert.isTrue(maxBatchSize >= maxValueSize);

        this.clientId = clientId;
        this.groupId = groupId;
        this.lockQueueCapacity = lockQueueCapacity;
        this.unlockQueueCapacity = unlockQueueCapacity;
        this.maxValueSize = maxValueSize;
        this.maxBatchSize = maxBatchSize;
    }

    public void setClientProtocol(IClientProtocol clientProtocol) {
        Assert.notNull(clientProtocol);
        Assert.isNull(this.clientProtocol);

        this.clientProtocol = clientProtocol;
    }

    public void setFlowController(IFlowController<FlowType> flowController) {
        Assert.notNull(flowController);
        Assert.isNull(this.flowController);

        this.flowController = flowController;
    }

    @Override
    public void process() {
        if (clientProtocol.isConnected() && !clientProtocol.isRequestInProgress()) {
            PublishRequest request = createRequest();
            if (request != null)
                clientProtocol.sendRequest(request);
        }
    }

    @Override
    public long getNextMessageId() {
        return nextMessageId;
    }

    @Override
    public void publish(ByteArray value, ICompletionHandler<ByteArray> commitHandler) {
        Assert.notNull(value);
        Assert.notNull(commitHandler);
        Assert.isTrue(value.getLength() <= maxValueSize);

        LogEntry logEntry = new LogEntry(0, value, LogValueType.APPLICATION, clientId, nextMessageId);
        nextMessageId++;
        queue.offer(new EntryInfo(logEntry, commitHandler));

        checkFlow();
    }

    @Override
    public void handleAcceptedPublishResponse(PublishResponse response) {
        if (response.getFlags().contains(MessageFlags.OUT_OF_ORDER_RECEIVED)) {
            if (response.getLastReceivedMessageId() == 0 && lastCommittedMessageId > 0) {
                requestRestartSession();
            } else {
                Assert.checkState(response.getLastReceivedMessageId() >= lastCommittedMessageId);
                lastSentMessageId = response.getLastReceivedMessageId();
            }
        }

        long lastCommittedMessageId = response.getLastCommittedMessageId();
        if (lastCommittedMessageId != 0)
            removeCommittedEntries(lastCommittedMessageId);
    }

    @Override
    public void onLeaderDisconnected() {
        lastSentMessageId = lastCommittedMessageId;
    }

    private PublishRequest createRequest() {
        List<LogEntry> entries = null;
        int batchSize = 0;

        int start = (int)(lastSentMessageId - lastCommittedMessageId);
        Assert.checkState(start >= 0);

        for (int i = start; i < queue.size(); i++) {
            LogEntry logEntry = queue.get(i).entry;
            if (logEntry.getValue().getLength() + batchSize > maxBatchSize)
                break;

            if (entries == null)
                entries = new ArrayList<>();

            entries.add(logEntry);
            batchSize += logEntry.getValue().getLength();
        }

        if (entries != null && !entries.isEmpty()) {
            PublishRequestBuilder request = new PublishRequestBuilder();
            request.setSource(clientId);
            request.setGroupId(groupId);
            request.setLogEntries(entries);

            return request.toMessage();
        } else {
            return null;
        }
    }

    private void removeCommittedEntries(long lastCommittedMessageId) {
        while (!queue.isEmpty()) {
            EntryInfo info = queue.peek();

            if (info.entry.getMessageId() <= lastCommittedMessageId) {
                queue.poll();

                this.lastCommittedMessageId = info.entry.getMessageId();
                if (lastSentMessageId < this.lastCommittedMessageId)
                    lastSentMessageId = this.lastCommittedMessageId;

                info.commitHandler.onSucceeded(info.entry.getValue());

                checkFlow();
            } else {
                break;
            }
        }
    }

    private void checkFlow() {
        if (flowController == null)
            return;

        if (!flowLocked && queue.size() >= lockQueueCapacity) {
            flowLocked = true;
            flowController.lockFlow(FlowType.PUBLISH);
        }

        if (flowLocked && queue.size() <= unlockQueueCapacity) {
            flowLocked = false;
            flowController.unlockFlow(FlowType.PUBLISH);
        }
    }

    private void requestRestartSession() {
        PublishRequestBuilder request = new PublishRequestBuilder();
        request.setSource(clientId);
        request.setGroupId(groupId);
        request.setFlags(Enums.of(MessageFlags.RESTART_SESSION));
        request.setLogEntries(Collections.singletonList(new LogEntry(0, null, LogValueType.APPLICATION, clientId, lastSentMessageId)));

        Assert.checkState(!clientProtocol.isRequestInProgress());

        clientProtocol.sendRequest(request.toMessage());
    }

    private  static class EntryInfo {
        private final LogEntry entry;
        private final ICompletionHandler<ByteArray> commitHandler;

        private EntryInfo(LogEntry entry, ICompletionHandler<ByteArray> commitHandler) {
            this.entry = entry;
            this.commitHandler = commitHandler;
        }
    }
}

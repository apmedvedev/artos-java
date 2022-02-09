package com.artos.inttests.core.sim.model;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.impl.core.server.utils.Utils;
import com.artos.inttests.core.sim.perf.IMeter;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.artos.spi.core.FlowType;
import com.artos.spi.core.IGroupChannel;
import com.artos.spi.core.IStateMachine;
import com.artos.spi.core.IStateMachineTransaction;
import com.artos.spi.core.ServerState;
import com.exametrika.common.io.impl.ByteInputStream;
import com.exametrika.common.io.impl.ByteOutputStream;
import com.exametrika.common.io.impl.DataDeserialization;
import com.exametrika.common.io.impl.DataSerialization;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.Exceptions;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.IOs;
import com.exametrika.common.utils.Pair;
import com.exametrika.common.utils.Times;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SimStateMachine implements IStateMachine, ISimPublishChannel {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final IGroupChannel groupChannel;
    private final boolean enableStateMachineChecks;
    private final long subscriptionEventPeriod;
    private final int subscriptionEventBatchSize;
    private final SimPublishDriver driver;
    private final UUID serverId;
    private final String endpoint;
    private final IMeter publishMeter;
    private final IMeter publishLatencyMeter;
    private final IMeter queryMeter;
    private final IMeter queryLatencyMeter;
    private final IMeter configurationWritesMeter;
    private final IMeter stateWritesMeter;
    private final IMeter subscriptionsMeter;
    private final IMeter unsubscriptionsMeter;
    private final IMeter subscriptionEventsMeter;
    private ServerState serverState;
    private GroupConfiguration configuration;
    private Map<String, Long> state = new LinkedHashMap<>();
    private Map<UUID, Pair<ByteArray, ICompletionHandler<ByteArray>>> subscriptions = new ConcurrentHashMap<>();
    private long commitIndex;
    private ScheduledExecutorService executor;
    private long subscriptionEventCount;

    public SimStateMachine(UUID serverId, String endpoint, PerfRegistry perfRegistry, SimPublishDriver driver, IGroupChannel groupChannel,
                           boolean enableStateMachineChecks, long subscriptionEventPeriod, int subscriptionEventBatchSize) {
        this.serverId = serverId;
        this.endpoint = endpoint;
        this.marker = Loggers.getMarker(endpoint);
        this.groupChannel = groupChannel;
        this.enableStateMachineChecks = enableStateMachineChecks;
        this.subscriptionEventPeriod = subscriptionEventPeriod;
        this.subscriptionEventBatchSize = subscriptionEventBatchSize;

        driver.setChannel(this);
        groupChannel.addMembershipListener(driver);

        this.driver = driver;
        publishMeter = perfRegistry.getMeter("state-machine:" + endpoint + ".publish(bytes)");
        publishLatencyMeter = perfRegistry.getMeter("state-machine:" + endpoint + ".publish.latency(ms)");
        queryMeter = perfRegistry.getMeter("state-machine:" + endpoint + ".query(bytes)");
        queryLatencyMeter = perfRegistry.getMeter("state-machine:" + endpoint + ".query.latency(ms)");
        configurationWritesMeter = perfRegistry.getMeter("state-machine:" + endpoint + ".configuration.writes(counts)");
        stateWritesMeter = perfRegistry.getMeter("state-machine:" + endpoint + ".serverState.writes(counts)");
        subscriptionsMeter = perfRegistry.getMeter("state-machine:" + endpoint + ".subscriptions(counts)");
        unsubscriptionsMeter = perfRegistry.getMeter("state-machine:" + endpoint + ".unsubscriptions(counts)");
        subscriptionEventsMeter = perfRegistry.getMeter("state-machine:" + endpoint + ".subscription-events(counts)");

        driver.start();
    }

    public synchronized Map<String, Long> getState() {
        return new LinkedHashMap<>(state);
    }

    public UUID getServerId() {
        return serverId;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public SimPublishDriver getDriver() {
        return driver;
    }

    @Override
    public IStateMachineTransaction beginTransaction(boolean readOnly) {
        return new SimStateMachineTransaction(readOnly, serverState, configuration, this);
    }

    @Override
    public synchronized long acquireSnapshot(OutputStream stream) {
        ByteOutputStream out = new ByteOutputStream();
        DataSerialization serialization = new DataSerialization(out);

        serialization.writeInt(state.size());
        for (Map.Entry<String, Long> entry : state.entrySet()) {
            serialization.writeString(entry.getKey());
            serialization.writeLong(entry.getValue());
        }

        Utils.writeGroupConfiguration(serialization, configuration);

        try {
            ByteInputStream in = new ByteInputStream(out.getBuffer(), 0, out.getLength());
            IOs.copy(in, stream);
            return commitIndex;
        } catch (Throwable e) {
            return Exceptions.wrapAndThrow(e);
        }
    }

    @Override
    public synchronized void applySnapshot(long logIndex, InputStream stream) {
        ByteInputStream in;
        try {
            ByteOutputStream out = new ByteOutputStream();
            IOs.copy(stream, out);
            in = new ByteInputStream(out.getBuffer(), 0, out.getLength());
        } catch (Throwable e) {
            Exceptions.wrapAndThrow(e);
            return;
        }

        state.clear();

        DataDeserialization deserialization = new DataDeserialization(in);
        int count = deserialization.readInt();
        for (int i = 0; i < count; i++) {
            String key = deserialization.readString();
            long value = deserialization.readLong();
            state.put(key, value);
        }

        configuration = Utils.readGroupConfiguration(deserialization);
        commitIndex = logIndex;
    }

    public synchronized void commit(ServerState serverState, GroupConfiguration configuration,
                                    List<Pair<Long, ByteArray>> publishState,
                                    List<ByteArray> queryState,
                                    List<Pair<UUID, Pair<ByteArray, ICompletionHandler<ByteArray>>>> subscriptions,
                                    List<UUID> unsubscriptions) {
        if (serverState != this.serverState) {
            this.serverState = serverState;
            stateWritesMeter.measure(1);
        }

        if (configuration != this.configuration) {
            this.configuration = configuration;
            configurationWritesMeter.measure(1);
        }

        if (!publishState.isEmpty()) {
            for (Pair<Long, ByteArray> pair : publishState)
                commitPublish(pair.getKey(), pair.getValue());
        }

        if (!queryState.isEmpty()) {
            for (ByteArray value : queryState)
                commitQuery(value);
        }

        commitSubscriptions(subscriptions, unsubscriptions);
    }

    @Override
    public void lockFlow(FlowType flow) {
        driver.lockFlow(flow);
    }

    @Override
    public void unlockFlow(FlowType flow) {
        driver.unlockFlow(flow);
    }

    @Override
    public void publish(ByteArray value, ICompletionHandler<ByteArray> commitHandler) {
        groupChannel.publish(value, commitHandler);
    }

    @Override
    public void start() {
        if (subscriptionEventPeriod != 0) {
            executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(() -> sendSubscriptionEvent(), 0, subscriptionEventPeriod, TimeUnit.MILLISECONDS);
        } else {
            executor = null;
        }
    }

    @Override
    public void stop() {
        if (executor != null)
            executor.shutdown();
    }

    private void commitPublish(long logIndex, ByteArray data) {
        Assert.checkState(logIndex >= commitIndex + 1);
        commitIndex = logIndex;

        String message = new String(data.getBuffer(), data.getOffset(), data.getLength(), StandardCharsets.UTF_8);

        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.messagePublished(message));

        int pos = message.indexOf(":");
        String key = message.substring(0, pos);
        String value = message.substring(pos + 1);
        pos = value.indexOf(":");
        long start = Long.parseLong(value.substring(0, pos));
        long counter = Long.parseLong(value.substring(pos + 1));

        Long prevCounter = state.put(key, counter);
        if (enableStateMachineChecks) {
            if (key.startsWith("client")) {
                if (prevCounter == null)
                    Assert.checkState(counter == 1);
                else
                    Assert.checkState(counter == prevCounter + 1);
            } else {
                if (prevCounter == null)
                    Assert.checkState(counter >= 1);
                else
                    Assert.checkState(counter >= prevCounter + 1);
            }
        }

        publishMeter.measure(data.getLength());

        long delta = Times.getCurrentTime() - start;
        publishLatencyMeter.measure(delta);
    }

    private void commitQuery(ByteArray data) {
        String message = new String(data.getBuffer(), data.getOffset(), data.getLength(), StandardCharsets.UTF_8);

        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.messageQueried(message));

        int pos = message.indexOf(":");
        String key = message.substring(0, pos);
        String value = message.substring(pos + 1);
        pos = value.indexOf(":");
        long start = Long.parseLong(value.substring(0, pos));
        long counter = Long.parseLong(value.substring(pos + 1));

        Long existingCounter = state.get(key);
        Assert.checkState(key.startsWith("client"));
        if (existingCounter != null)
            Assert.checkState(counter <= existingCounter);

        queryMeter.measure(data.getLength());

        long delta = Times.getCurrentTime() - start;
        queryLatencyMeter.measure(delta);
    }

    private void commitSubscriptions(List<Pair<UUID, Pair<ByteArray, ICompletionHandler<ByteArray>>>> subscriptions, List<UUID> unsubscriptions) {
        if (!subscriptions.isEmpty()) {
            subscriptionsMeter.measure(subscriptions.size());

            for (Pair<UUID, Pair<ByteArray, ICompletionHandler<ByteArray>>> subscription : subscriptions) {
                this.subscriptions.put(subscription.getKey(), subscription.getValue());
            }
        }

        if (!unsubscriptions.isEmpty()) {
            unsubscriptionsMeter.measure(unsubscriptions.size());
            this.subscriptions.keySet().removeAll(unsubscriptions);
        }
    }

    private void sendSubscriptionEvent() {
        if (subscriptions.isEmpty())
            return;

        for (int i = 0; i < subscriptionEventBatchSize; i++) {
            String message = endpoint + ":" + subscriptionEventCount;
            subscriptionEventCount++;

            ByteArray subscriptionEvent = createValue(message);
            for (Map.Entry<UUID, Pair<ByteArray, ICompletionHandler<ByteArray>>> entry : subscriptions.entrySet()) {
                ICompletionHandler<ByteArray> responseHandler = entry.getValue().getValue();
                responseHandler.onSucceeded(subscriptionEvent);
            }
        }
    }

    private ByteArray createValue(String message) {
        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.subscriptionEventSent(message));

        ByteArray result = new ByteArray(message.getBytes(StandardCharsets.UTF_8));
        subscriptionEventsMeter.measure(result.getLength());

        return result;
    }

    private interface IMessages {
        @DefaultMessage("Message has been published: {0}.")
        ILocalizedMessage messagePublished(String message);

        @DefaultMessage("Message has been queried: {0}.")
        ILocalizedMessage messageQueried(String message);

        @DefaultMessage("Subscription event has been sent: ''{0}''.")
        ILocalizedMessage subscriptionEventSent(String message);
    }
}

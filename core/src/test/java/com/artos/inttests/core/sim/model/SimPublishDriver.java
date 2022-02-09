package com.artos.inttests.core.sim.model;

import com.artos.impl.core.server.membership.MembershipListener;
import com.artos.inttests.core.sim.perf.IMeter;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.artos.spi.core.FlowType;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.tasks.IFlowController;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.CompletionHandler;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.ILifecycle;
import com.exametrika.common.utils.Times;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SimPublishDriver extends MembershipListener implements IFlowController<FlowType>, ILifecycle {
    private static final IMessages messages = Messages.get(IMessages.class);
    static final ByteArray dummyValue = new ByteArray("It's a dummy message.".getBytes(StandardCharsets.UTF_8));
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final int batchSize;
    protected final String endpoint;
    private final boolean client;
    private ISimPublishChannel channel;
    private final IMeter sendsMeter;
    private final IMeter latencyMeter;
    private final IMeter failuresMeter;
    private final IMeter locksMeter;
    private final IMeter unlocksMeter;
    private final long period;
    private ScheduledExecutorService executor;
    private final boolean dummy;
    private volatile boolean enabled;
    private volatile long counter;
    private volatile boolean flowLocked;
    private volatile boolean leader;

    public SimPublishDriver(String endpoint, long period, int batchSize, boolean enabled,
                            PerfRegistry perfRegistry, boolean client, boolean dummy) {
        this.batchSize = batchSize;
        this.marker = Loggers.getMarker(endpoint);
        this.endpoint = endpoint;
        this.enabled = enabled;
        this.period = period;
        this.leader = client;
        this.client = client;
        this.dummy = dummy;
        this.sendsMeter = perfRegistry.getMeter("publisher:" + endpoint + ".sends(bytes)");
        this.latencyMeter = perfRegistry.getMeter("publisher:" + endpoint + ".latency(ms)");
        this.failuresMeter = perfRegistry.getMeter("publisher:" + endpoint + ".failures(counts)");
        this.locksMeter = perfRegistry.getMeter("publisher:" + endpoint + ".locks(counts)");
        this.unlocksMeter = perfRegistry.getMeter("publisher:" + endpoint + ".unlocks(counts)");
    }

    public String getEndpoint() {
        return endpoint;
    }

    public long getCounter() {
        return counter;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isClient() {
        return client;
    }

    @Override
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> run(), 0, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        executor.shutdown();
    }

    public void setChannel(ISimPublishChannel channel) {
        this.channel = channel;
    }

    public boolean isDummy() {
        return dummy;
    }

    @Override
    public void lockFlow(FlowType flow) {
        flowLocked = true;
        locksMeter.measure(1);

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.flowLocked());
    }

    @Override
    public void unlockFlow(FlowType flow) {
        flowLocked = false;
        unlocksMeter.measure(1);

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.flowUnlocked());
    }

    @Override
    public void onLeader() {
        leader = true;
    }

    @Override
    public void onFollower() {
        leader = false;
    }

    protected void doSend(String message, ByteArray value, ICompletionHandler commitHandler) {
        channel.publish(value, commitHandler);
    }

    private synchronized void send() {
        long start = Times.getCurrentTime();
        String message = createMessage();
        doSend(message, createValue(message), createCommitHandler(start));
    }

    private CompletionHandler createCommitHandler(long start) {
        return new CompletionHandler() {
            @Override
            public void onSucceeded(Object result) {
                long delta = Times.getCurrentTime() - start;
                latencyMeter.measure(delta);
            }

            @Override
            public void onFailed(Throwable error) {
                failuresMeter.measure(1);
            }
        };
    }

    private String createMessage() {
        counter++;
        long start = Times.getCurrentTime();
        return endpoint + ":" + start + ":" + counter;
    }

    private ByteArray createValue(String message) {
        if (!dummy) {
            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.messageSent(message));

            ByteArray result = new ByteArray(message.getBytes(StandardCharsets.UTF_8));
            sendsMeter.measure(result.getLength());

            return result;
        } else {
            return dummyValue;
        }
    }

    private void run() {
        if (!enabled || flowLocked || !leader)
            return;

        for (int i = 0; i < batchSize; i++) {
            try {
                send();
            } catch (Throwable e) {
                if (logger.isLogEnabled(LogLevel.ERROR))
                    logger.log(LogLevel.ERROR, marker, e);
            }
        }
    }

    private interface IMessages {
        @DefaultMessage("Message has been sent: ''{0}''.")
        ILocalizedMessage messageSent(String message);

        @DefaultMessage("Flow has been locked.")
        ILocalizedMessage flowLocked();

        @DefaultMessage("Flow has been unlocked.")
        ILocalizedMessage flowUnlocked();
    }
}

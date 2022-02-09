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
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SimQueryDriver extends MembershipListener implements IFlowController<FlowType>, ILifecycle {
    private static final IMessages messages = Messages.get(IMessages.class);
    private static final long INITIAL_QUERY_DELAY = 5000;
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final int batchSize;
    protected final String endpoint;
    private ISimQueryChannel channel;
    private final IMeter sendsMeter;
    private final IMeter latencyMeter;
    private final IMeter failuresMeter;
    private final IMeter locksMeter;
    private final IMeter unlocksMeter;
    private final IMeter subscriptionEventsMeter;
    private final long period;
    private ScheduledExecutorService executor;
    private volatile boolean enabled;
    private volatile boolean flowLocked;
    private final SimPublishDriver publishDriver;
    private boolean subscribed;

    public SimQueryDriver(String endpoint, long period, int batchSize, boolean enabled,
                          PerfRegistry perfRegistry, SimPublishDriver publishDriver) {
        this.batchSize = batchSize;
        this.marker = Loggers.getMarker(endpoint);
        this.endpoint = endpoint;
        this.enabled = enabled;
        this.period = period;
        this.publishDriver = publishDriver;
        this.sendsMeter = perfRegistry.getMeter("querier:" + endpoint + ".sends(bytes)");
        this.latencyMeter = perfRegistry.getMeter("querier:" + endpoint + ".latency(ms)");
        this.failuresMeter = perfRegistry.getMeter("querier:" + endpoint + ".failures(counts)");
        this.locksMeter = perfRegistry.getMeter("querier:" + endpoint + ".locks(counts)");
        this.unlocksMeter = perfRegistry.getMeter("querier:" + endpoint + ".unlocks(counts)");
        this.subscriptionEventsMeter = perfRegistry.getMeter("querier:" + endpoint + ".subscription-events(counts)");
    }

    public String getEndpoint() {
        return endpoint;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isClient() {
        return publishDriver.isClient();
    }

    @Override
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> run(), INITIAL_QUERY_DELAY, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        executor.shutdown();
    }

    public void setChannel(ISimQueryChannel channel) {
        this.channel = channel;
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

    protected void doSend(String message, ByteArray value, ICompletionHandler responseHandler) {
        channel.query(value, responseHandler);
    }

    private void send() {
        synchronized (publishDriver) {
            long start = Times.getCurrentTime();
            String message = createMessage();
            doSend(message, createValue(message), createResponseHandler(start));
        }
    }

    private CompletionHandler createResponseHandler(long start) {
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
        long start = Times.getCurrentTime();
        return endpoint + ":" + start + ":" + publishDriver.getCounter();
    }

    private ByteArray createValue(String message) {
        if (!publishDriver.isDummy()) {
            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.messageSent(message));

            ByteArray result = new ByteArray(message.getBytes(StandardCharsets.UTF_8));
            sendsMeter.measure(result.getLength());

            return result;
        } else {
            return SimPublishDriver.dummyValue;
        }
    }

    private void run() {
        if (!subscribed) {
            subscribed = true;
            subscribe();
        }

        if (!enabled || flowLocked)
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

    private void subscribe() {
        channel.subscribe(UUID.randomUUID(), createValue(endpoint + ":subscription"), new CompletionHandler<ByteArray>() {
            @Override
            public void onSucceeded(ByteArray result) {
                subscriptionEventsMeter.measure(1);

                String message = new String(result.getBuffer(), result.getOffset(), result.getLength(), StandardCharsets.UTF_8);

                if (logger.isLogEnabled(LogLevel.DEBUG))
                    logger.log(LogLevel.DEBUG, marker, messages.subscriptionEventReceived(message));
            }

            @Override
            public void onFailed(Throwable error) {
                failuresMeter.measure(1);
            }
        });
    }

    private interface IMessages {
        @DefaultMessage("Message has been sent: ''{0}''.")
        ILocalizedMessage messageSent(String message);

        @DefaultMessage("Flow has been locked.")
        ILocalizedMessage flowLocked();

        @DefaultMessage("Flow has been unlocked.")
        ILocalizedMessage flowUnlocked();

        @DefaultMessage("Subscription event has been received: ''{0}''.")
        ILocalizedMessage subscriptionEventReceived(String message);
    }
}

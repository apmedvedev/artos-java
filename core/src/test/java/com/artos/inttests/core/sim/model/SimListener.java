package com.artos.inttests.core.sim.model;

import com.artos.inttests.core.sim.perf.IMeter;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.artos.spi.core.IMessageReceiver;
import com.artos.spi.core.IMessageListener;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.CompletionHandler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimListener extends SimTransportBase implements IMessageListener {
    private final SimLink link;
    private final IMessageReceiver messageReceiver;
    private final IMeter queueSizeMeter;
    private ExecutorService executor;
    private volatile boolean stopped;

    public SimListener(SimLink link, PerfRegistry perfRegistry, IMessageReceiver messageReceiver) {
        super("listener", link.getEndpoint(), perfRegistry);
        this.link = link;
        this.messageReceiver = messageReceiver;
        this.queueSizeMeter = perfRegistry.getMeter("listener:" + link.getEndpoint() + ".queue.size(counts)");
    }

    @Override
    public void start() {
        executor = Executors.newSingleThreadExecutor();
        executor.execute(() -> run());
    }

    @Override
    public void stop() {
        stopped = true;
        executor.shutdown();
    }

    private void run() {
        while (!stopped) {
           SimMessage message = link.take();
            if (message != null)
                receive(message);
        }
    }

    @Override
    protected void measureRequest(ByteArray request) {
        super.measureRequest(request);

        queueSizeMeter.measure(link.getQueueSize());
    }

    private void receive(SimMessage message) {
        measureRequest(message.getRequest());

        messageReceiver.receive(message.getRequest(), new CompletionHandler<ByteArray>() {
            @Override
            public boolean isCanceled() {
                return message.getResponse().isCanceled();
            }

            @Override
            public void onSucceeded(ByteArray result) {
                measureResponse(result);
                message.getResponse().onSucceeded(result);
            }

            @Override
            public void onFailed(Throwable error) {
                measureFailure(error);
                message.getResponse().onFailed(error);
            }
        });
    }
}

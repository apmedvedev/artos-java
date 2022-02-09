package com.artos.inttests.core.sim.model;

import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.artos.spi.core.IMessageSender;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.CompletionHandler;
import com.exametrika.common.utils.ICompletionHandler;

public class SimSender extends SimTransportBase implements IMessageSender {
    private final SimLink link;
    private final String endpoint;
    private final ICompletionHandler<ByteArray> responseHandler;

    public SimSender(SimLink link, String targetEndpoint, PerfRegistry perfRegistry, ICompletionHandler<ByteArray> responseHandler) {
        super("sender[" + link.getEndpoint() + "]", targetEndpoint, perfRegistry);
        this.link = link;
        this.endpoint = targetEndpoint;
        this.responseHandler = responseHandler;
    }

    @Override
    public void send(ByteArray request) {
        SimMessage message = new SimMessage(request, new CompletionHandler<ByteArray>() {
            @Override
            public boolean isCanceled() {
                return !link.isEnabled();
            }

            @Override
            public void onSucceeded(ByteArray result) {
                measureResponse(result);
                responseHandler.onSucceeded(result);
            }

            @Override
            public void onFailed(Throwable error) {
                measureFailure(error);
                responseHandler.onFailed(error);
            }
        });

        measureRequest(request);

        link.send(endpoint, message);
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
}

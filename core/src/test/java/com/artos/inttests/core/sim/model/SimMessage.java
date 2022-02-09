package com.artos.inttests.core.sim.model;

import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

public class SimMessage {
    private final ByteArray request;
    private final ICompletionHandler<ByteArray> response;

    public SimMessage(ByteArray request, ICompletionHandler<ByteArray> response) {
        this.request = request;
        this.response = response;
    }

    public ByteArray getRequest() {
        return request;
    }

    public ICompletionHandler<ByteArray> getResponse() {
        return response;
    }
}

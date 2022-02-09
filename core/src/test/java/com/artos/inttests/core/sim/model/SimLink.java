package com.artos.inttests.core.sim.model;

import com.exametrika.common.utils.Times;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SimLink {
    private final SimRouter router;
    private final String endpoint;
    private final BlockingQueue<MessageInfo> queue = new LinkedBlockingQueue<>();
    private volatile boolean enabled = true;
    private volatile long latency;

    public SimLink(SimRouter router, String endpoint) {
        this.router = router;
        this.endpoint = endpoint;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        if (enabled != this.enabled) {
            if (enabled)
                queue.clear();

            this.enabled = enabled;

            if (!enabled)
                queue.clear();
        }
    }

    public long getLatency() {
        return latency;
    }

    public void setLatency(long latency) {
        this.latency = latency;
    }

    public int getQueueSize() {
        return queue.size();
    }

    public void offer(SimMessage message) {
        if (enabled)
            queue.offer(new MessageInfo(message, Times.getCurrentTime()));
    }

    public SimMessage take() {
        try {
            if (enabled) {
                long currentTime = Times.getCurrentTime();
                MessageInfo info = queue.peek();
                if (info != null && currentTime > info.time + latency)
                    return queue.take().message;
            }

            Thread.yield();
            return null;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void send(String endpoint, SimMessage message) {
        if (enabled)
            router.route(endpoint, message);
    }

    private static class MessageInfo {
        private final SimMessage message;
        private final long time;

        private MessageInfo(SimMessage message, long time) {
            this.message = message;
            this.time = time;
        }
    }
}

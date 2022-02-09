package com.artos.inttests.core.sim.api;

public interface ISimGroupLinks {
    void enableAllLinks();

    void disableRandomLink();

    void setRandomLinkLatency(long latency);

    void setAllLinksLatency(long latency);
}

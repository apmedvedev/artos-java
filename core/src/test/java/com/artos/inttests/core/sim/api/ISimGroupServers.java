package com.artos.inttests.core.sim.api;

public interface ISimGroupServers {
    void joinServer();

    void failServer(boolean leader);

    void gracefullyStopServer(boolean leader);

    void failRandomServer();

    void leaveServer(boolean leader);

    void leaveRandomServer();

    void restoreServer();

    void disableLink(boolean leader);
}

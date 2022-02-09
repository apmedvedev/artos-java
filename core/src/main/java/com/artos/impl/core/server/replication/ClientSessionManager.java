package com.artos.impl.core.server.replication;

import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.SimpleList;
import com.exametrika.common.utils.SimpleList.Element;
import com.exametrika.common.utils.Times;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

public class ClientSessionManager {
    private final IReplicationProtocol replicationProtocol;
    private final long clientSessionTimeout;
    private final SimpleList<ClientSession> sessions = new SimpleList<>();
    private final Map<UUID, ClientSession> sessionsMap = new HashMap<>();

    public ClientSessionManager(IReplicationProtocol replicationProtocol, long clientSessionTimeout) {
        Assert.notNull(replicationProtocol);

        this.replicationProtocol = replicationProtocol;
        this.clientSessionTimeout = clientSessionTimeout;
    }

    public SimpleList<ClientSession> getSessions() {
        return sessions;
    }

    public ClientSession ensureSession(UUID clientId) {
        ClientSession session = sessionsMap.get(clientId);
        if (session == null) {
            session = new ClientSession(clientId);
            sessionsMap.put(clientId, session);
        } else {
            session.getElement().remove();
            session.getElement().reset();
        }

        sessions.addLast(session.getElement());
        session.setLastAccessTime(Times.getCurrentTime());

        return session;
    }

    public void onTimer(long currentTime) {
        for (Iterator<Element<ClientSession>> it = sessions.iterator(); it.hasNext(); ) {
            ClientSession session = it.next().getValue();
            if (currentTime < session.getLastAccessTime() + clientSessionTimeout)
                break;

            it.remove();
            sessionsMap.remove(session.getClientId());
            replicationProtocol.onSessionRemoved(session);
        }
    }

    public void clear() {
        for (Iterator<Element<ClientSession>> it = sessions.iterator(); it.hasNext(); ) {
            ClientSession session = it.next().getValue();
            replicationProtocol.onSessionRemoved(session);
        }

        sessions.clear();
        sessionsMap.clear();
    }
}

package com.artos.impl.core.server.utils;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.Message;
import com.artos.impl.core.server.election.IElectionProtocol;
import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.IServerPeer;
import com.artos.impl.core.server.impl.State;
import com.artos.impl.core.server.state.StateTransferConfiguration;
import com.artos.spi.core.IStateMachineTransaction;
import com.artos.spi.core.LogEntry;
import com.artos.spi.core.LogValueType;
import com.exametrika.common.io.IDataDeserialization;
import com.exametrika.common.io.IDataSerialization;
import com.exametrika.common.io.impl.ByteInputStream;
import com.exametrika.common.io.impl.ByteOutputStream;
import com.exametrika.common.io.impl.DataDeserialization;
import com.exametrika.common.io.impl.DataSerialization;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.Serializers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Utils {
    public static boolean updateTerm(Context context, IElectionProtocol electionProtocol, State state, long term) {
        if (term > state.getServerState().getTerm()) {
            state.getServerState().setTerm(term);
            state.getServerState().setVotedFor(null);

            IStateMachineTransaction transaction = context.getStateMachine().beginTransaction(false);
            transaction.writeState(state.getServerState());
            transaction.commit();

            electionProtocol.becomeFollower();
            return true;
        }

        return false;
    }

    public static String toString(Message message, State state, boolean additionalFields) {
        return message.toString(getPeerEndpoint(state.getPeers(), message.getSource()), additionalFields);
    }

    public static String toString(UUID serverId, State state) {
        return getPeerEndpoint(state.getPeers(), serverId);
    }

    public static ServerConfiguration readServerConfiguration(ByteArray buffer) {
        ByteInputStream in = new ByteInputStream(buffer.getBuffer(), buffer.getOffset(), buffer.getLength());
        DataDeserialization deserialization = new DataDeserialization(in);
        return readServerConfiguration(deserialization);
    }

    public static ServerConfiguration readServerConfiguration(IDataDeserialization deserialization) {
        UUID id = Serializers.readUUID(deserialization);
        String endpoint = deserialization.readString();
        return new ServerConfiguration(id, endpoint);
    }

    public static ByteArray writeServerConfiguration(ServerConfiguration serverConfiguration) {
        ByteOutputStream out = new ByteOutputStream();
        DataSerialization serialization = new DataSerialization(out);
        writeServerConfiguration(serialization, serverConfiguration);

        return new ByteArray(out.getBuffer(), 0, out.getLength());
    }

    public static void writeServerConfiguration(IDataSerialization serialization, ServerConfiguration serverConfiguration) {
        Serializers.writeUUID(serialization, serverConfiguration.getId());
        serialization.writeString(serverConfiguration.getEndpoint());
    }

    public static GroupConfiguration readGroupConfiguration(ByteArray buffer) {
        ByteInputStream in = new ByteInputStream(buffer.getBuffer(), buffer.getOffset(), buffer.getLength());
        DataDeserialization deserialization = new DataDeserialization(in);
        return readGroupConfiguration(deserialization);

    }

    public static GroupConfiguration readGroupConfiguration(IDataDeserialization deserialization) {
        String name = deserialization.readString();
        UUID groupId = Serializers.readUUID(deserialization);
        int count = deserialization.readInt();
        List<ServerConfiguration> servers = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
            servers.add(readServerConfiguration(deserialization));
        boolean singleServer = deserialization.readBoolean();

        return new GroupConfiguration(name, groupId, servers, singleServer);
    }

    public static ByteArray writeGroupConfiguration(GroupConfiguration groupConfiguration) {
        ByteOutputStream out = new ByteOutputStream();
        DataSerialization serialization = new DataSerialization(out);
        writeGroupConfiguration(serialization, groupConfiguration);

        return new ByteArray(out.getBuffer(), 0, out.getLength());
    }

    public static void writeGroupConfiguration(IDataSerialization serialization, GroupConfiguration groupConfiguration) {
        serialization.writeString(groupConfiguration.getName());
        Serializers.writeUUID(serialization, groupConfiguration.getGroupId());
        serialization.writeInt(groupConfiguration.getServers().size());
        for (ServerConfiguration configuration : groupConfiguration.getServers())
            writeServerConfiguration(serialization, configuration);
        serialization.writeBoolean(groupConfiguration.isSingleServer());
    }

    public static LogEntry readLogEntry(IDataDeserialization deserialization) {
        long term = deserialization.readLong();
        ByteArray value = deserialization.readByteArray();
        LogValueType valueType = Serializers.readEnum(deserialization, LogValueType.class);
        UUID clientId = null;
        if (deserialization.readBoolean())
            clientId = Serializers.readUUID(deserialization);

        long messageId = deserialization.readLong();

        return new LogEntry(term, value, valueType, clientId, messageId);
    }

    public static void writeLogEntry(IDataSerialization serialization, LogEntry logEntry) {
        serialization.writeLong(logEntry.getTerm());
        serialization.writeByteArray(logEntry.getValue());

        Serializers.writeEnum(serialization, logEntry.getValueType());
        if (logEntry.getClientId() != null) {
            serialization.writeBoolean(true);
            Serializers.writeUUID(serialization, logEntry.getClientId());
        } else {
            serialization.writeBoolean(false);
        }

        serialization.writeLong(logEntry.getMessageId());
    }

    public static StateTransferConfiguration readStateTransferConfiguration(ByteArray buffer) {
        ByteInputStream in = new ByteInputStream(buffer.getBuffer(), buffer.getOffset(), buffer.getLength());
        DataDeserialization deserialization = new DataDeserialization(in);
        return readStateTransferConfiguration(deserialization);
    }

    public static StateTransferConfiguration readStateTransferConfiguration(IDataDeserialization deserialization) {
        String host = deserialization.readString();
        int port = deserialization.readInt();
        long startLogIndex = deserialization.readLong();
        return new StateTransferConfiguration(host, port, startLogIndex);
    }

    public static ByteArray writeStateTransferConfiguration(StateTransferConfiguration configuration) {
        ByteOutputStream out = new ByteOutputStream();
        DataSerialization serialization = new DataSerialization(out);
        writeStateTransferConfiguration(serialization, configuration);

        return new ByteArray(out.getBuffer(), 0, out.getLength());
    }

    public static void writeStateTransferConfiguration(IDataSerialization serialization, StateTransferConfiguration configuration) {
        serialization.writeString(configuration.getHost());
        serialization.writeInt(configuration.getPort());
        serialization.writeLong(configuration.getStartLogIndex());
    }

    private static String getPeerEndpoint(Map<UUID, IServerPeer> peers, UUID peerId) {
        if (peerId == null)
            return "";

        IServerPeer peer = peers.get(peerId);
        if (peer == null)
            return peerId.toString();

        return peer.getConfiguration().getEndpoint();
    }

    private Utils() {
    }
}

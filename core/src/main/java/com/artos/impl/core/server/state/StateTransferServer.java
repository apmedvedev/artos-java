package com.artos.impl.core.server.state;

import com.artos.impl.core.server.state.IStateTransferAcquirer.LogState;
import com.artos.impl.core.server.state.IStateTransferAcquirer.SnapshotState;
import com.artos.impl.core.server.state.IStateTransferAcquirer.StateInfo;
import com.exametrika.common.compartment.ICompartment;
import com.exametrika.common.config.resource.ClassPathResourceLoader;
import com.exametrika.common.config.resource.FileResourceLoader;
import com.exametrika.common.config.resource.IResourceLoader;
import com.exametrika.common.config.resource.ResourceManager;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.l10n.SystemException;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.messaging.ChannelException;
import com.exametrika.common.tasks.impl.Daemon;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Exceptions;
import com.exametrika.common.utils.Files;
import com.exametrika.common.utils.IOs;
import com.exametrika.common.utils.SimpleList;
import com.exametrika.common.utils.SimpleList.Element;
import com.exametrika.common.utils.Times;

import javax.net.ServerSocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

public class StateTransferServer {
    private  static int DEFAULT_BUFFER_SIZE = 8192;
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final int portRangeStart;
    private final int portRangeEnd;
    private final File workDir;
    private final boolean secured;
    private final String keyStoreFile;
    private final String keyStorePassword;
    private final String bindAddress;
    private final SimpleList<Request> requests = new SimpleList<>();
    private final IStateTransferAcquirer stateAcquirer;
    private final ICompartment compartment;
    private final Daemon daemon;
    private volatile ServerSocket serverSocket;
    private volatile boolean stopped;

    public StateTransferServer(String endpoint, Integer portRangeStart, Integer portRangeEnd, String bindAddress, String workDir,
                               boolean secured, String keyStoreFile, String keyStorePassword, IStateTransferAcquirer stateAcquirer,
                               ICompartment compartment) {
        Assert.notNull(workDir);
        Assert.notNull(stateAcquirer);
        Assert.notNull(compartment);

        if (portRangeStart == null)
            portRangeStart = 1;
        if (portRangeEnd == null)
            portRangeEnd = 65535;

        if (portRangeStart == 0 && portRangeEnd != 65535 && portRangeEnd != 0)
            portRangeStart = 1;

        if (portRangeStart < 0 || portRangeStart > 65535 || portRangeEnd < 0 || portRangeEnd > 65535 || portRangeStart > portRangeEnd)
            throw new IllegalArgumentException();

        if (secured) {
            Assert.notNull(keyStoreFile);
            Assert.notNull(keyStorePassword);
        }

        this.workDir = new File(workDir);
        if (!this.workDir.exists())
            Assert.isTrue(this.workDir.mkdirs());

        this.portRangeStart = portRangeStart;
        this.portRangeEnd = portRangeEnd;
        this.bindAddress = bindAddress;

        this.secured = secured;
        this.keyStoreFile = keyStoreFile;
        this.keyStorePassword = keyStorePassword;
        this.stateAcquirer = stateAcquirer;
        this.compartment = compartment;
        this.marker = Loggers.getMarker(endpoint);
        this.daemon = new Daemon(() -> run(), "State transfer server thread", null);
    }

    public InetSocketAddress getLocalAddress() {
        ServerSocket serverSocket = this.serverSocket;
        Assert.checkState(serverSocket != null);

        return (InetSocketAddress) serverSocket.getLocalSocketAddress();
    }

    public void start() {
        synchronized (this) {
            Assert.checkState(serverSocket == null);

            Files.emptyDir(workDir);
            serverSocket = createServerSocket();
        }

        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.serverStarted(serverSocket.getLocalPort(), portRangeStart, portRangeEnd));

        daemon.start();
    }

    public void stop() {
        synchronized (this) {
            stopped = true;
            IOs.close(serverSocket);

            for (Request request : requests.values())
                IOs.close(request.socket);
        }

        daemon.stop();

        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.serverStopped());
    }

    private void run() {
        while (!stopped) {
            try {
                Socket socket = serverSocket.accept();
                Request request = new Request(socket);

                synchronized (this) {
                    requests.addLast(request.element);
                }

                compartment.execute(request);
            } catch (IOException e) {
                if (stopped)
                    break;
                else if (logger.isLogEnabled(LogLevel.ERROR))
                    logger.log(LogLevel.ERROR, marker, e);
            }
        }
    }

    private ServerSocket createServerSocket() {
        try {
            ServerSocketFactory socketFactory = createServerSocketFactory();
            InetAddress bindAddress = this.bindAddress != null ? InetAddress.getByName(this.bindAddress) : InetAddress.getLocalHost();

            for (int port = portRangeStart; port <= portRangeEnd; port++) {
                try {
                    return socketFactory.createServerSocket(port, 50, bindAddress);
                } catch (SocketException e) {
                    continue;
                } catch (SecurityException e) {
                    continue;
                }
            }

            throw new SystemException(messages.createSocketError(bindAddress, portRangeStart, portRangeEnd));
        } catch (Exception e) {
            return Exceptions.wrapAndThrow(e);
        }
    }

    private ServerSocketFactory createServerSocketFactory() {
        if (!secured)
            return ServerSocketFactory.getDefault();
        else
            return createSslContext().getServerSocketFactory();
    }

    private SSLContext createSslContext() {
        Map<String, IResourceLoader> resourceLoaders = new HashMap<String, IResourceLoader>();
        resourceLoaders.put(FileResourceLoader.SCHEMA, new FileResourceLoader());
        resourceLoaders.put(ClassPathResourceLoader.SCHEMA, new ClassPathResourceLoader());
        ResourceManager resourceManager = new ResourceManager(resourceLoaders, "file");
        InputStream stream = null;
        try {
            stream = resourceManager.getResource(keyStoreFile);
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(stream, keyStorePassword.toCharArray());
            SSLContext sslContext = SSLContext.getInstance("TLS");
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(keyStore);

            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());

            return sslContext;
        } catch (Exception e) {
            throw new ChannelException(e);
        } finally {
            IOs.close(stream);
        }
    }

    private class Request implements Runnable {
        private final Socket socket;
        private final Element<Request> element = new Element<>(this);
        private File tempDir;
        private UUID groupId;
        private long logIndex;
        private StateInfo stateInfo;

        private Request(Socket socket) {
            Assert.notNull(socket);

            this.socket = socket;

            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.requestAccepted(socket.getRemoteSocketAddress()));
        }

        @Override
        public void run() {
            try {
                tempDir = Files.createTempDirectory(workDir, "raft-");
                readRequest();
                acquireState();
                writeResponse();
            } catch (IOException e) {
                if (logger.isLogEnabled(LogLevel.ERROR))
                    logger.log(LogLevel.ERROR, marker, e);
            } finally {
                synchronized (StateTransferServer.this) {
                    element.remove();
                }

                IOs.close(socket);
                Files.delete(tempDir);
            }
        }

        private void readRequest() throws IOException {
            InputStream in = new BufferedInputStream(socket.getInputStream());

            ByteBuffer buffer = ByteBuffer.allocate(24);
            in.read(buffer.array(), buffer.arrayOffset(), buffer.limit());

            groupId = new UUID(buffer.getLong(), buffer.getLong());
            logIndex = buffer.getLong();

            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.requestReceived(groupId, logIndex));
        }

        private void acquireState() {
            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.startAcquireState(groupId, logIndex));
            long start = Times.getCurrentTime();

            stateInfo = stateAcquirer.acquireState(groupId, logIndex, tempDir);
            Assert.checkState(stateInfo != null);

            long delta = Times.getCurrentTime() - start;
            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.endAcquireState(groupId, logIndex, delta));
        }

        private void writeResponse() throws IOException {
            OutputStream out = new BufferedOutputStream(socket.getOutputStream());

            int fileCount = 0;
            if (stateInfo.snapshotState != null)
                fileCount++;
            if (stateInfo.logStates != null)
                fileCount += stateInfo.logStates.size();

            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt(fileCount);
            buffer.flip();

            out.write(buffer.array(), buffer.arrayOffset(), buffer.limit());

            if (stateInfo.snapshotState != null)
                transferFile(stateInfo.snapshotState, out);

            if (stateInfo.logStates != null) {
                for (LogState state : stateInfo.logStates) {
                    transferFile(state, out);
                }
            }

            out.flush();

            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.requestCompleted(groupId, logIndex));
        }

        private void transferFile(LogState state, OutputStream out) throws IOException {
            long fileSize = state.file.length();
            long start = Times.getCurrentTime();
            boolean isSnapshot = false;
            long logIndex = 0;
            if (state instanceof SnapshotState) {
                isSnapshot = true;
                logIndex = ((SnapshotState) state).logIndex;
            }

            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.startTransferFile(state.file, fileSize, isSnapshot, state.compressed, logIndex));

            ByteBuffer buffer = ByteBuffer.allocate(18);
            buffer.put(isSnapshot ? (byte)1 : 0);
            buffer.put(state.compressed ? (byte)1 : 0);
            buffer.putLong(logIndex);
            buffer.putLong(fileSize);
            buffer.flip();

            out.write(buffer.array(), buffer.arrayOffset(), buffer.limit());

            CRC32 crc = new CRC32();
            try (InputStream in = new CheckedInputStream(new BufferedInputStream(new FileInputStream(state.file)), crc)) {
                long transferred = transfer(in, out);
                if (fileSize != transferred)
                    throw new SystemException(messages.wrongSize());
            }

            buffer = ByteBuffer.allocate(8);
            buffer.putLong(crc.getValue());
            buffer.flip();
            out.write(buffer.array(), buffer.arrayOffset(), buffer.limit());

            long delta = Times.getCurrentTime() - start;
            double rate = (double)fileSize / 1000000 * 1000 / delta;

            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.endTransferFile(state.file, delta, rate));
        }

        private long transfer(InputStream in, OutputStream out) throws IOException {
            long transferred = 0;
            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            int read;
            while ((read = in.read(buffer, 0, DEFAULT_BUFFER_SIZE)) >= 0) {
                out.write(buffer, 0, read);
                transferred += read;
            }
            return transferred;
        }
    }

    private interface IMessages {
        @DefaultMessage("State transfer server has been started on port {0} from range [{1}..{2}].")
        ILocalizedMessage serverStarted(int port, int portRangeStart, int portRangeEnd);

        @DefaultMessage("State transfer request has been received. Group-id: {0}, log-index: {1}.")
        ILocalizedMessage requestReceived(UUID groupId, long logIndex);

        @DefaultMessage("State transfer request has been completed. Group-id: {0}, log-index: {1}.")
        ILocalizedMessage requestCompleted(UUID groupId, long logIndex);

        @DefaultMessage("Start transferring file ''{0}'', file-size: {1}, snapshot: {2}, compressed: {3}, log-index: {4}.")
        ILocalizedMessage startTransferFile(File file, long fileSize, boolean isSnapshot, boolean compressed, long logIndex);

        @DefaultMessage("End transferring file ''{0}'', time(ms): {1}, rate(mb/s): {2}.")
        ILocalizedMessage endTransferFile(File file, long delta, double rate);

        @DefaultMessage("Start acquiring state. Group-id: {0}, log-index: {1}.")
        ILocalizedMessage startAcquireState(UUID groupId, long logIndex);

        @DefaultMessage("End acquiring state. Group-id: {0}, log-index: {1}, time(ms): {2}.")
        ILocalizedMessage endAcquireState(UUID groupId, long logIndex, long delta);

        @DefaultMessage("File size is not equal to size of transferred data.")
        ILocalizedMessage wrongSize();

        @DefaultMessage("Could not create socket on bind address ''{0}'' and port in range [{1}..{2}].")
        ILocalizedMessage createSocketError(InetAddress bindAddress, int portRangeStart, int portRangeEnd);

        @DefaultMessage("State transfer server has been stopped.")
        ILocalizedMessage serverStopped();

        @DefaultMessage("Request has been accepted from ''{0}''.")
        ILocalizedMessage requestAccepted(SocketAddress remoteSocketAddress);
    }
}

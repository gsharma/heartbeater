package com.github.heartbeater.server;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.github.heartbeater.server.HeartbeatServerException.Code;

final class HeartbeatPersisterImpl implements HeartbeatPersister {
    private static final Logger logger = LogManager.getLogger(HeartbeatPersisterImpl.class.getSimpleName());

    private final String identity = UUID.randomUUID().toString();

    private final AtomicBoolean running;
    private final AtomicBoolean ready;

    private File storeDirectory;
    private RocksDB dataStore;

    // private ColumnFamilyHandle defaultCF;
    private ColumnFamilyHandle peerServerRegistry;
    private Map<String, ColumnFamilyHandle> peerServerHeartbeats;

    HeartbeatPersisterImpl() {
        this.running = new AtomicBoolean(false);
        this.ready = new AtomicBoolean(false);
    }

    @Override
    public String getIdentity() {
        return identity;
    }

    @Override
    public void start() throws Exception {
        if (running.compareAndSet(false, true)) {
            // cleanly handle resumption cases
            Path dataStorePath = null;
            try {
                peerServerHeartbeats = new ConcurrentHashMap<>();
                final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
                final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(4);
                columnFamilyDescriptors.add(0, new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
                final String peerRegistryCfName = "peerRegistry-" + identity;
                columnFamilyDescriptors.add(1, new ColumnFamilyDescriptor(peerRegistryCfName.getBytes(StandardCharsets.UTF_8), columnFamilyOptions));

                final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

                RocksDB.loadLibrary();
                final DBOptions options = new DBOptions();
                options.setCreateIfMissing(true);
                options.setCreateMissingColumnFamilies(true);
                storeDirectory = new File("./heartbeatdb", "heartbeater-" + getIdentity());
                Files.createDirectories(storeDirectory.getParentFile().toPath());
                dataStorePath = Files.createDirectories(storeDirectory.getAbsoluteFile().toPath());
                dataStore = RocksDB.open(options, storeDirectory.getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles);

                // defaultCF = columnFamilyHandles.get(0);
                peerServerRegistry = columnFamilyHandles.get(1);
            } catch (Exception initProblem) {
                throw new HeartbeatServerException(Code.HEARTBEATER_INIT_FAILURE, initProblem);
            }
            ready.set(true);
            logger.info("Started HeartbeatPersister [{}] at {}", getIdentity(), dataStorePath);
        } else {
            throw new HeartbeatServerException(Code.INVALID_HEARTBEATER_LCM, "Invalid attempt to start an already running heartbeater");
        }
    }

    @Override
    public void stop() throws Exception {
        if (running.compareAndSet(true, false)) {
            try {
                ready.set(false);
                // dataStore.dropColumnFamily(defaultCF);
                dataStore.dropColumnFamily(peerServerRegistry);
                for (final ColumnFamilyHandle peerServerHeartbeatCf : peerServerHeartbeats.values()) {
                    dataStore.dropColumnFamily(peerServerHeartbeatCf);
                }
                logger.info("Dropped column families: peerServerRegistry, peerServerHeartbeats");
                dataStore.close();
                logger.info("Closed dataStore");
                Files.walk(storeDirectory.toPath())
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
                logger.info("Deleted directory {}", storeDirectory.getPath());
                logger.info("Stopped HeartbeatPersister [{}]", getIdentity());
            } catch (Exception tiniProblem) {
                throw new HeartbeatServerException(Code.HEARTBEATER_TINI_FAILURE, tiniProblem);
            }
        } else {
            throw new HeartbeatServerException(Code.INVALID_HEARTBEATER_LCM, "Invalid attempt to stop an already stopped heartbeater");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get() && ready.get();
    }

    @Override
    public void savePeer(final String serverId, final String serverIp, final int port) throws HeartbeatServerException {
        if (!isRunning()) {
            throw new HeartbeatServerException(Code.INVALID_HEARTBEATER_LCM, "Invalid attempt to operate an already stopped heartbeater");
        }
        try {
            if (peerServerHeartbeats.containsKey(serverId)) {
                throw new HeartbeatServerException(Code.PEER_ALREADY_EXISTS, String.format("Peer already exists:%s", serverId));
            }
            final String heartbeatPeerCfName = "heartbeat-" + serverId;
            final ColumnFamilyDescriptor heartbeatPeerCfDescriptor = new ColumnFamilyDescriptor(heartbeatPeerCfName.getBytes(StandardCharsets.UTF_8),
                    new ColumnFamilyOptions());
            final ColumnFamilyHandle heartbeatPeerCf = dataStore.createColumnFamily(heartbeatPeerCfDescriptor);
            peerServerHeartbeats.put(serverId, heartbeatPeerCf);

            // TODO
            // final byte[] serializedServerId = serverId.getBytes(StandardCharsets.UTF_8);
            // final byte[] serializedPeerInfo = PeerInfo.serialize(peer);
            // dataStore.put(peerServerRegistry, serializedServerId, serializedPeerInfo);
            logger.info("Saved peer::[serverId:{}, serverIp:{}, port:{}]", serverId, serverIp, port);
        } catch (RocksDBException persistenceIssue) {
            throw new HeartbeatServerException(Code.HEARTBEATER_PERSISTENCE_FAILURE, persistenceIssue);
        }
    }

    @Override
    public void removePeer(final String serverId) throws HeartbeatServerException {
        if (!isRunning()) {
            throw new HeartbeatServerException(Code.INVALID_HEARTBEATER_LCM, "Invalid attempt to operate an already stopped heartbeater");
        }
        try {
            dataStore.dropColumnFamily(peerServerHeartbeats.get(serverId));
            peerServerHeartbeats.remove(serverId);

            logger.info("Removed peer::[serverId:{}]", serverId);
        } catch (RocksDBException persistenceIssue) {
            throw new HeartbeatServerException(Code.HEARTBEATER_PERSISTENCE_FAILURE, persistenceIssue);
        }
    }

    @Override
    public void findPeer(final String serverId) throws HeartbeatServerException {
        // TODO Auto-generated method stub
    }

}

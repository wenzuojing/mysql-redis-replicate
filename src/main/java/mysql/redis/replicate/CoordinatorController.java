package mysql.redis.replicate;

import com.alibaba.otter.canal.common.utils.AddressUtils;
import mysql.redis.replicate.config.DestinationConfig;
import mysql.redis.replicate.config.DestinationConfigManager;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;

import java.util.List;
import java.util.Random;

/**
 * Created by wens on 15/12/5.
 */
public class CoordinatorController implements ZookeeperLeaderElector.LeaderListener {

    private final Logger logger = LoggerFactory.getLogger();

    private DestinationConfigManager destinationConfigManager;

    private String myId;
    private String httpEndpoint;
    private volatile List<String> aliveServerIds;

    private ZookeeperLeaderElector zookeeperLeaderElector;

    public CoordinatorController(DestinationConfigManager destinationConfigManager, String myId, String httpEndPoint) {
        zookeeperLeaderElector = new ZookeeperLeaderElector(myId, this);
        this.destinationConfigManager = destinationConfigManager;
        this.myId = myId;
        this.httpEndpoint = httpEndPoint;
    }


    public void start() {
        zookeeperLeaderElector.start();
        ServerInfo serverInfo = new ServerInfo(myId, AddressUtils.getHostIp(), httpEndpoint, System.currentTimeMillis());
        ZookeeperUtils.createEphemeral(ZkPathUtils.getIdsPath(myId), serverInfo);
        ZookeeperUtils.subscribeStateChanges(new IZkStateListener() {
            @Override
            public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
                System.out.println("----");
            }

            @Override
            public void handleNewSession() throws Exception {

                try {
                    ZookeeperUtils.createEphemeral(ZkPathUtils.getIdsPath(myId), serverInfo);
                } catch (Exception e) {
                    logger.error("Create ephemeral path fail : " + ZkPathUtils.getIdsPath(myId));
                }

            }
        });
        this.aliveServerIds = ZookeeperUtils.getChildren(ZkPathUtils.getIdsPath());
    }


    public void stop() {
        zookeeperLeaderElector.stop();
        ZookeeperUtils.delete(ZkPathUtils.getIdsPath(myId));
    }

    @Override
    public void onBecomingLeader() {
        ZookeeperUtils.subscribeChildChanges(ZkPathUtils.getIdsPath(), new ServerChangeListener());
    }

    public boolean stopDestination(String destination) {
        DestinationConfig destinationConfig = destinationConfigManager.getDestinationConfig(destination);
        String ret = doStopDestination(destination, destinationConfig);
        destinationConfig.setStopped(true);
        destinationConfigManager.saveOrUpdate(destinationConfig);
        return "ok".equals(ret) ? true : false;

    }

    private String doStopDestination(String destination, DestinationConfig destinationConfig) {
        String ret = "ok";
        if (aliveServerIds.contains(destinationConfig.getRunOn())) {
            ServerInfo serverInfo = getServerInfo(destinationConfig.getRunOn());
            ret = HttpClientUtils.post(serverInfo.getHttpEndpoint(), Tuple.of("cmd", "stop"), Tuple.of("destination", destination));
        } else {
            logger.warn("stop destination on {},but {} is not alive.", destinationConfig.getRunOn(), destinationConfig.getRunOn());
        }
        return ret;
    }

    public boolean startDestination(String destination, String serverId) {

        DestinationConfig destinationConfig = destinationConfigManager.getDestinationConfig(destination);
        if (!destinationConfig.isStopped()) {
            doStopDestination(destination, destinationConfig);
        }

        String ret = doStartDestination(destination, serverId, destinationConfig);
        if ("ok".equals(ret)) {
            destinationConfig.setStopped(false);
            destinationConfig.setRunOn(serverId);
        } else {
            destinationConfig.setStopped(true);
            destinationConfig.setRunFail(true);
            destinationConfig.setRunOn(serverId);
        }

        destinationConfigManager.saveOrUpdate(destinationConfig);
        return "ok".equals(ret) ? true : false;

    }

    private String doStartDestination(String destination, String serverId, DestinationConfig destinationConfig) {
        String ret = "ok";
        if (aliveServerIds.contains(serverId)) {
            ServerInfo serverInfo = getServerInfo(serverId);
            ret = HttpClientUtils.post(serverInfo.getHttpEndpoint(), Tuple.of("cmd", "start"), Tuple.of("destination", destination));
        } else {
            logger.warn("start destination on {},but {} is not alive.", serverId, serverId);
            throw new RuntimeException("Server " + serverId + " is not alive");
        }
        return ret;
    }

    public void deleteDestination(String destination) {
        DestinationConfig destinationConfig = destinationConfigManager.getDestinationConfig(destination);
        if (!destinationConfig.isRunFail()) {
            doStopDestination(destination, destinationConfig);
        }
        destinationConfigManager.delete(destination);
    }

    public List<String> getAliveServerIds() {
        return aliveServerIds;
    }

    private class ServerChangeListener implements IZkChildListener {

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            aliveServerIds = currentChilds;
            List<DestinationConfig> allDestinationConfig = destinationConfigManager.getAllDestinationConfig();

            for (DestinationConfig destinationConfig : allDestinationConfig) {
                if (!destinationConfig.isStopped()) {
                    String serverId = destinationConfig.getRunOn();
                    if (!currentChilds.contains(destinationConfig.getRunOn())) {
                        Random random = new Random();
                        serverId = currentChilds.get(random.nextInt(currentChilds.size()));
                    }
                    ServerInfo serverInfo = getServerInfo(serverId);
                    boolean fail = invokeEndpointForStart(destinationConfig, serverInfo);
                    destinationConfig.setRunOn(serverInfo.id);
                    destinationConfig.setRunFail(fail);
                    destinationConfigManager.saveOrUpdate(destinationConfig);
                }
            }
        }

        private boolean invokeEndpointForStart(DestinationConfig destinationConfig, ServerInfo serverInfo) {
            boolean fail = true;
            int retry = 0;
            while (retry <= 2) {
                try {
                    String ret = HttpClientUtils.post(serverInfo.getHttpEndpoint(), Tuple.of("cmd", "start"), Tuple.of("destination", destinationConfig.getDestination()));
                    if ("ok".equals(ret)) {
                        fail = false;
                        break;
                    }
                } catch (Exception e) {
                    logger.error("Retry " + retry + " Run destination fail : dest=" + destinationConfig.getDestination() + ",serverId = " + serverInfo.getId(), e);
                    retry++;
                }
            }
            return fail;
        }
    }

    public ServerInfo getServerInfo(String id) {
        return ZookeeperUtils.readData(ZkPathUtils.getIdsPath(id), ServerInfo.class);
    }

    private static class ServerInfo {

        String id;
        String host;
        String httpEndpoint;
        long timestamp;

        public ServerInfo() {
        }

        public ServerInfo(String id, String host, String httpEndpoint, long timestamp) {
            this.id = id;
            this.host = host;
            this.httpEndpoint = httpEndpoint;
            this.timestamp = timestamp;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getHttpEndpoint() {
            return httpEndpoint;
        }

        public void setHttpEndpoint(String httpEndpoint) {
            this.httpEndpoint = httpEndpoint;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}

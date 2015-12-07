package mysql.redis.replicate;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;

/**
 * Created by wens on 15/12/5.
 */
public class ZookeeperLeaderElector {

    private final Logger logger = LoggerFactory.getLogger();

    private String myId;

    private volatile String leaderId;

    private LeaderListener leaderListener;

    public ZookeeperLeaderElector(String myId, LeaderListener leaderListener) {
        this.myId = myId;
        this.leaderListener = leaderListener;
    }

    public void start() {
        ZookeeperUtils.subscribeStateChanges(new ZkStateListener());
        ZookeeperUtils.subscribeDataChanges(ZkPathUtils.getControllerPath(), new ZkDataChangeListener());
        elect();

    }

    public boolean elect() {

        leaderId = getLeaderId();

        ControllerInfo controllerInfo = new ControllerInfo();
        controllerInfo.id = myId;
        controllerInfo.timestamp = System.currentTimeMillis();
        try {
            ZookeeperUtils.createEphemeral(ZkPathUtils.getControllerPath(), controllerInfo);
            leaderId = myId;
            logger.info("I am leader : {}", myId);
            if (leaderListener != null) {
                try {
                    leaderListener.onBecomingLeader();
                } catch (Exception e) {
                    logger.error("Call leaderListener fail.", e);
                }
            }
            return true;
        } catch (Exception e) {
            logger.info("Leader is {}", leaderId);
        }

        return false;

    }

    private String getLeaderId() {
        ControllerInfo controllerInfo = ZookeeperUtils.readData(ZkPathUtils.getControllerPath(), ControllerInfo.class);
        if (controllerInfo != null) {
            return controllerInfo.id;
        }
        return null;
    }

    public void stop() {
        if (myId.equals(leaderId)) {
            ZookeeperUtils.delete(ZkPathUtils.getControllerPath());
        }

    }

    private class ZkStateListener implements IZkStateListener {

        public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {

        }

        public void handleNewSession() throws Exception {
            elect();

        }
    }

    private class ZkDataChangeListener implements IZkDataListener {

        public void handleDataChange(String s, Object o) throws Exception {

        }

        public void handleDataDeleted(String s) throws Exception {
            elect();
        }
    }

    private static class ControllerInfo {
        String id;
        long timestamp;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }

    interface LeaderListener {
        void onBecomingLeader();
    }
}

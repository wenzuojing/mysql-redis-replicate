package mysql.redis.replicate;

import org.I0Itec.zkclient.ZkClient;

/**
 * Created by wens on 15-12-4.
 */
public class ZkPathUtils {

    private static String ROOT;

    public static void init(String root, ZkClient zkClient) {
        ROOT = root;

        if (!zkClient.exists(getDestinationConfigPath())) {
            zkClient.createPersistent(getDestinationConfigPath(), true);
        }

        if (!zkClient.exists(getIdsPath())) {
            zkClient.createPersistent(getIdsPath(), true);
        }

        if (!zkClient.exists(getDestinationSinkLogOffsetPath())) {
            zkClient.createPersistent(getDestinationSinkLogOffsetPath(), true);
        }
    }

    public static String getDestinationConfigPath() {
        return ROOT + "/destinations";
    }

    public static String getDestinationConfigPath(String destination) {
        return getDestinationConfigPath() + "/" + destination;
    }

    public static String getControllerPath() {
        return ROOT + "/controller";
    }

    public static String getIdsPath() {
        return ROOT + "/ids";
    }

    public static String getIdsPath(String id) {
        return getIdsPath() + "/" + id;
    }

    public static String getDestinationSinkLogOffsetPath() {
        return ROOT + "/sink";
    }

    public static String getDestinationSinkLogOffsetPath(String destination) {
        return getDestinationSinkLogOffsetPath() + "/" + destination;
    }
}

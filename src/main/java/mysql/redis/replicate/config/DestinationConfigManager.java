package mysql.redis.replicate.config;

import com.google.common.collect.Sets;
import mysql.redis.replicate.ZkPathUtils;
import mysql.redis.replicate.ZookeeperUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by wens on 15-10-14.
 */
public class DestinationConfigManager {


    public Set<String> getAllDestination() {
        String path = ZkPathUtils.getDestinationConfigPath();
        return Sets.newHashSet(ZookeeperUtils.getChildren(path));
    }

    public List<DestinationConfig> getAllDestinationConfig() {
        Set<String> destinations = getAllDestination();
        List<DestinationConfig> destinationConfigs = new ArrayList<>(destinations.size());
        for (String destination : destinations) {
            DestinationConfig destinationConfig = getDestinationConfig(destination);
            if (destinationConfig != null) {
                destinationConfigs.add(destinationConfig);
            }
        }
        return destinationConfigs;
    }

    public DestinationConfig getDestinationConfig(String destination) {
        return ZookeeperUtils.readData(ZkPathUtils.getDestinationConfigPath(destination), DestinationConfig.class);
    }

    public void saveOrUpdate(DestinationConfig destinationConfig) {
        String path = ZkPathUtils.getDestinationConfigPath(destinationConfig.getDestination());
        ZookeeperUtils.writeData(path, destinationConfig, true);
    }


    public void delete(String destination) {
        String path = ZkPathUtils.getDestinationConfigPath(destination);
        ZookeeperUtils.delete(path);
    }
}

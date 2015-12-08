package mysql.redis.replicate;

import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;

import java.util.List;

/**
 * Created by wens on 15-12-7.
 */
public class ZookeeperUtils {

    private volatile static ZkClient zkClient;

    public static void init(Conf conf) {
        zkClient = openClient(conf.getZookeeperServer());
        ZkPathUtils.init(conf.getZookeeperRootPath(), zkClient);
    }

    public static ZkClient openClient(String zkServer) {
        return new ZkClient(zkServer, 10000, 10000, new BytesPushThroughSerializer());
    }

    public static List<String> getChildren(String path) {
        return zkClient.getChildren(path);
    }


    public static <T> T readData(String path, Class<T> dataClass) {
        byte[] data = zkClient.readData(path, true);
        if (data != null && data.length != 0) {
            return JsonUtils.unmarshalFromByte(data, dataClass);
        }
        return null;
    }

    public static void writeData(String path, Object object, boolean createPathIfNotExist) {
        try {
            zkClient.writeData(path, JsonUtils.marshalToByte(object, SerializerFeature.WriteClassName));
        } catch (ZkException e) {
            if (createPathIfNotExist) {
                createIfNotExist(path);
                zkClient.writeData(path, JsonUtils.marshalToByte(object, SerializerFeature.WriteClassName));
            } else {
                throw e;
            }
        }
    }

    public static void delete(String path) {
        zkClient.delete(path);
    }

    public static void createIfNotExist(String path) {
        try {
            zkClient.createPersistent(path, true);
        } catch (Exception e) {
            //
        }
    }

    public static void createEphemeral(String path, Object object) {
        zkClient.createEphemeral(path, JsonUtils.marshalToByte(object));
    }

    public static void subscribeChildChanges(String path, IZkChildListener listener) {
        zkClient.subscribeChildChanges(path, listener);
    }

    public static void subscribeStateChanges(IZkStateListener listener) {
        zkClient.subscribeStateChanges(listener);
    }

    public static void subscribeDataChanges(String path, IZkDataListener listener) {
        zkClient.subscribeDataChanges(path, listener);
    }

    public static void deleteRecursive(String path) {
        zkClient.deleteRecursive(path);
    }


}

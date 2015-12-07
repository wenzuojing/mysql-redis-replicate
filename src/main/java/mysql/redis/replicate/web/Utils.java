package mysql.redis.replicate.web;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import mysql.redis.replicate.config.DestinationConfig;
import mysql.redis.replicate.groovy.IRow2map;
import mysql.redis.replicate.groovy.Row2map;
import mysql.redis.replicate.groovy.Row2mapManager;
import mysql.redis.replicate.redis.RedisUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Map;
import java.util.Random;

/**
 * Created by wens on 15-11-16.
 */
public class Utils {

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Load mysql driver fail.");
        }
    }

    public static String testConfig(DestinationConfig destinationConfig) throws SQLException {
        Connection conn = null;
        try {
            StringBuilder sb = new StringBuilder();
            conn = DriverManager.getConnection(String.format("jdbc:mysql://%s?connectTimeout=4000", destinationConfig.getDbAddress()), destinationConfig.getDbUser(), destinationConfig.getDbPassword());
            for (DestinationConfig.TableConfig tableConfig : destinationConfig.getTableConfigs()) {
                String template;
                try {
                    template = Files.toString(new File(Thread.currentThread().getContextClassLoader().getResource(Row2mapManager.SCRIPT_TEMPLATE_FILE).getPath()), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                IRow2map row2map = new Row2map(template.replace("#script#", tableConfig.getScript()));

                PreparedStatement statement = conn.prepareStatement("select * from  " + tableConfig.getTableName() + "  limit 1 ");
                ResultSet resultSet = statement.executeQuery();

                if (resultSet.next()) {
                    Map<String, String> data = Maps.newHashMap();
                    Map<String, String> type = Maps.newHashMap();
                    int columnCount = resultSet.getMetaData().getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = resultSet.getMetaData().getColumnName(i);
                        data.put(columnName, resultSet.getString(columnName));
                        type.put(columnName, resultSet.getMetaData().getColumnTypeName(i));
                    }
                    Map<String, String> map = Maps.newHashMap();
                    row2map.convert(type, data, map);

                    ShardedJedis sharedJedis = RedisUtils.createSharedJedis(tableConfig.getRedisInfos(), tableConfig.getRedisPassword());

                    Random random = new Random(500);
                    String id = String.valueOf(random.nextInt());
                    String key = "_test_key_" + id;
                    Jedis jedis = sharedJedis.getShard(id);
                    jedis.del(key);
                    RedisUtils.putMap(jedis, key, map);
                    sb.append(map).append("\n");
                }
                resultSet.close();
                statement.close();
            }
            return sb.toString();
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

}

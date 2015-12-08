package mysql.redis.replicate.web;

import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mysql.redis.replicate.Tuple;
import mysql.redis.replicate.ZkPathUtils;
import mysql.redis.replicate.ZookeeperUtils;
import mysql.redis.replicate.config.DestinationConfig;
import mysql.redis.replicate.config.DestinationConfigManager;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by wens on 15-11-16.
 */
public class AllDestinationServlet extends HttpServlet {

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Load mysql driver fail.");
        }
    }


    private DestinationConfigManager destinationConfigManager;


    public AllDestinationServlet(DestinationConfigManager destinationConfigManager) {
        this.destinationConfigManager = destinationConfigManager;
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        Set<String> allDestination = destinationConfigManager.getAllDestination();
        List<Map<String, Object>> retList = Lists.newArrayList();

        for (String dest : allDestination) {

            DestinationConfig config = destinationConfigManager.getDestinationConfig(dest);
            Tuple<String, String> masterBinlogPosition = null;
            try {
                masterBinlogPosition = getMasterBinlogPosition("jdbc:mysql://" + config.getDbAddress(), config.getDbUser(), config.getDbPassword());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            String mysqlLogOffset = masterBinlogPosition == null ? "" : masterBinlogPosition.getOne() + ":" + masterBinlogPosition.getTwo();

            String sinkOffset = ZookeeperUtils.readData(ZkPathUtils.getDestinationSinkLogOffsetPath(dest), String.class);
            if (sinkOffset == null) {
                sinkOffset = "n/a";
            }

            HashMap<String, Object> map = Maps.newHashMap();
            map.put("destination", dest);
            map.put("mysqlLogOffset", mysqlLogOffset);
            map.put("sinkLogOffset", sinkOffset);
            map.put("stopped", config.isStopped());
            map.put("runOn", config.getRunOn());
            map.put("runFail", config.isRunFail());
            retList.add(map);
        }
        response.getWriter().write(JsonUtils.marshalToString(retList));
    }


    public Tuple<String, String> getMasterBinlogPosition(String url, String user, String password) throws SQLException {
        Connection conn = null;
        Tuple<String, String> position = null;
        try {
            conn = DriverManager.getConnection(url, user, password);

            Statement statement = conn.createStatement();

            ResultSet resultSet = statement.executeQuery("show master status ");

            while (resultSet.next()) {
                position = new Tuple<>(resultSet.getString("File"), resultSet.getString("Position"));
                break;
            }
            resultSet.close();
            statement.close();
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
        return position;
    }
}

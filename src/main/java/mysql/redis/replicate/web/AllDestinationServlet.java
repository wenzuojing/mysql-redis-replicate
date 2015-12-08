package mysql.redis.replicate.web;

import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mysql.redis.replicate.CoordinatorController;
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




    private DestinationConfigManager destinationConfigManager;


    public AllDestinationServlet(DestinationConfigManager destinationConfigManager) {
        this.destinationConfigManager = destinationConfigManager;
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        Set<String> allDestination = destinationConfigManager.getAllDestination();
        List<Map<String, Object>> retList = Lists.newArrayList();

        for (String dest : allDestination) {

            DestinationConfig config = destinationConfigManager.getDestinationConfig(dest);
            CoordinatorController.ServerInfo serverInfo = ZookeeperUtils.readData(ZkPathUtils.getIdsPath(config.getRunOn()), CoordinatorController.ServerInfo.class);
            String monitorUrl = String.format("%s?destination=%s&mysqlAddress=%s&mysqlUser=%s&mysqlPassword=%s" , serverInfo.getHttpEndpoint().replace("endpoint" , "monitor"), config.getDestination() , config.getDbAddress(), config.getDbUser(),config.getDbPassword()) ;
            HashMap<String, Object> map = Maps.newHashMap();
            map.put("destination", dest);
            map.put("stopped", config.isStopped());
            map.put("runOn", config.getRunOn());
            map.put("runFail", config.isRunFail());
            map.put("monitorUrl", monitorUrl );
            retList.add(map);
        }
        response.getWriter().write(JsonUtils.marshalToString(retList));
    }



}

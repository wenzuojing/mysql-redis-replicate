package mysql.redis.replicate.web;

import com.alibaba.otter.canal.common.utils.JsonUtils;
import mysql.redis.replicate.config.DestinationConfig;
import mysql.redis.replicate.config.DestinationConfigManager;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by wens on 15-11-16.
 */
public class SaveConfigServlet extends HttpServlet {


    private DestinationConfigManager destinationConfigManager;

    public SaveConfigServlet(DestinationConfigManager destinationConfigManager) {
        this.destinationConfigManager = destinationConfigManager;
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String jsonStr = request.getParameter("json");
        try {
            DestinationConfig destinationConfig = JsonUtils.unmarshalFromString(jsonStr, DestinationConfig.class);
            Utils.testConfig(destinationConfig);
            DestinationConfig old = destinationConfigManager.getDestinationConfig(destinationConfig.getDestination());

            if (old != null) {
                destinationConfig.setStopped(old.isStopped());
                destinationConfig.setRunFail(old.isRunFail());
                destinationConfig.setRunOn(old.getRunOn());
            }

            destinationConfigManager.saveOrUpdate(destinationConfig);
            response.getWriter().write("ok");
        } catch (Exception e) {
            response.getWriter().write("fail:" + e.getMessage());
        }


    }
}

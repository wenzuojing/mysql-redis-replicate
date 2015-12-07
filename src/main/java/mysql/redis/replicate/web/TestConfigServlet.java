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
public class TestConfigServlet extends HttpServlet {


    private DestinationConfigManager destinationConfigManager;

    public TestConfigServlet(DestinationConfigManager destinationConfigManager) {
        this.destinationConfigManager = destinationConfigManager;
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String jsonStr = request.getParameter("json");
        try {
            DestinationConfig destinationConfig = JsonUtils.unmarshalFromString(jsonStr, DestinationConfig.class);
            response.getWriter().write("ok:\n" + Utils.testConfig(destinationConfig));
        } catch (Exception e) {
            response.getWriter().write("fail:" + e.getMessage());
        }


    }


}

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
public class GetConfigServlet extends HttpServlet {


    private DestinationConfigManager destinationConfigManager;

    public GetConfigServlet(DestinationConfigManager destinationConfigManager) {
        this.destinationConfigManager = destinationConfigManager;
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String destination = request.getParameter("destination");
        DestinationConfig config = destinationConfigManager.getDestinationConfig(destination);
        response.getWriter().write(JsonUtils.marshalToString(config));
    }
}

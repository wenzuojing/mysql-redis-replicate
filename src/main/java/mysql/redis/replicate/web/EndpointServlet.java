package mysql.redis.replicate.web;

import mysql.redis.replicate.canal.ControllerService;
import mysql.redis.replicate.config.DestinationConfigManager;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by wens on 15/12/6.
 */
public class EndpointServlet extends HttpServlet {

    private ControllerService controllerService;

    private DestinationConfigManager destinationConfigManager;

    public EndpointServlet(DestinationConfigManager destinationConfigManager, ControllerService controllerService) {
        this.destinationConfigManager = destinationConfigManager;
        this.controllerService = controllerService;
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        try {
            String opt = req.getParameter("cmd");
            String destination = req.getParameter("destination");
            if (opt != null && destination != null) {
                if (opt.equalsIgnoreCase("stop")) {
                    controllerService.stopTask(destination);
                } else if (opt.equalsIgnoreCase("start")) {
                    controllerService.startTask(destination);
                } else if (opt.equalsIgnoreCase("delete")) {
                    controllerService.stopTask(destination);
                    destinationConfigManager.delete(destination);
                }
                resp.getWriter().write("ok");
            }
        } catch (Exception e) {
            resp.getWriter().write("fail :" + e.getMessage());
        }


    }
}

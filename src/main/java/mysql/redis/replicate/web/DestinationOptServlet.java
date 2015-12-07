package mysql.redis.replicate.web;

import mysql.redis.replicate.CoordinatorController;
import mysql.redis.replicate.LoggerFactory;
import org.slf4j.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by wens on 15-11-17.
 */
public class DestinationOptServlet extends HttpServlet {

    private final Logger logger = LoggerFactory.getLogger();

    private CoordinatorController coordinatorController;


    public DestinationOptServlet(CoordinatorController coordinatorController) {
        this.coordinatorController = coordinatorController;
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        try {
            String opt = req.getParameter("opt");
            String destination = req.getParameter("destination");
            String runOn = req.getParameter("runOn");
            if (opt != null && destination != null) {
                if (opt.equalsIgnoreCase("stop")) {
                    coordinatorController.stopDestination(destination);
                } else if (opt.equalsIgnoreCase("start")) {
                    coordinatorController.startDestination(destination, runOn);
                } else if (opt.equalsIgnoreCase("delete")) {
                    coordinatorController.deleteDestination(destination);
                } else {
                    resp.getWriter().write("unknown opt :" + opt);
                    return;
                }

                resp.getWriter().write("ok");
            }
        } catch (Exception e) {
            logger.error("opt fail : opt = {} ", e);
            resp.getWriter().write("fail");
        }


    }
}

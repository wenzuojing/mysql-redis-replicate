package mysql.redis.replicate.web;

import com.alibaba.otter.canal.common.utils.JsonUtils;
import mysql.redis.replicate.CoordinatorController;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

/**
 * Created by wens on 15/12/6.
 */
public class AliveServerServlet extends HttpServlet {

    private CoordinatorController coordinatorController;

    public AliveServerServlet(CoordinatorController coordinatorController) {
        this.coordinatorController = coordinatorController;
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        List<String> serverIds = coordinatorController.getAliveServerIds();
        resp.getWriter().write(JsonUtils.marshalToString(serverIds));
    }


}

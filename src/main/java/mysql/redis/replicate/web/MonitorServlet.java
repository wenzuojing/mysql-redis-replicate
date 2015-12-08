package mysql.redis.replicate.web;

import mysql.redis.replicate.Monitor;
import mysql.redis.replicate.Tuple;
import mysql.redis.replicate.ZkPathUtils;
import mysql.redis.replicate.ZookeeperUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;

/**
 * Created by wens on 15-12-8.
 */
public class MonitorServlet extends HttpServlet {

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Load mysql driver fail.");
        }
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        String destination = req.getParameter("destination");
        String mysqlAddress = req.getParameter("mysqlAddress");
        String mysqlUser = req.getParameter("mysqlUser");
        String mysqlPassword = req.getParameter("mysqlPassword");

        Tuple<String, String> masterBinlogPosition = null;
        try {
            masterBinlogPosition = getMasterBinlogPosition("jdbc:mysql://" +mysqlAddress , mysqlUser , mysqlPassword );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String mysqlLogOffset = masterBinlogPosition == null ? "" : masterBinlogPosition.getOne() + ":" + masterBinlogPosition.getTwo();

        Monitor.MonitorData monitorData = Monitor.getMonitorData(destination);

        PrintWriter writer = resp.getWriter();

        writer.write("mysql binlog offset:" + mysqlLogOffset + "\n");

        if(monitorData != null ){
            writer.write("replicate binlog offset:" +monitorData.logfileName + ":" + monitorData.logfileOffset + "\n" );
            writer.write("insert count:" +monitorData.insertCount+ "\n" );
            writer.write("update count:" +monitorData.updateCount+ "\n" );
            writer.write("delete count:" +monitorData.deleteCount+ "\n" );
            writer.write("speed:" +monitorData.speed+ "\n" );
            writer.flush();
        }
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

package mysql.redis.replicate.canal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by wens on 15-10-15.
 */
public class MysqlUtils {


    public static Connection getConnection(String user, String pwd, String url) throws SQLException {
        return DriverManager.getConnection(url, user, pwd);
    }


    public static void executeSQL(Connection connection, String sql) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(sql);
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    public static void executeUpdateSQL(Connection connection, String sql) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.executeUpdate(sql);
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

}

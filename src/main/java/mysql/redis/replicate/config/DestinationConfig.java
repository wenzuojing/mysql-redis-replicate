package mysql.redis.replicate.config;

import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.List;

/**
 * Created by wens on 15-10-14.
 */
public class DestinationConfig implements Serializable {

    private String destination;

    private String dbAddress;

    private String dbUser;

    private String dbPassword;


    private String runOn;

    private boolean runFail;

    private List<TableConfig> tableConfigs;

    private boolean stopped = true;


    public List<TableConfig> getTableConfigs() {
        return tableConfigs;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getDbAddress() {
        return dbAddress;
    }

    public void setDbAddress(String dbAddress) {
        this.dbAddress = dbAddress;
    }

    public String getDbUser() {
        return dbUser;
    }

    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public boolean isStopped() {
        return stopped;
    }

    public void setStopped(boolean stopped) {
        this.stopped = stopped;
    }

    public String getRunOn() {
        return runOn;
    }

    public void setRunOn(String runOn) {
        this.runOn = runOn;
    }

    public boolean isRunFail() {
        return runFail;
    }

    public void setRunFail(boolean runFail) {
        this.runFail = runFail;
    }

    public static class TableConfig {

        private String tableName;

        private String script;

        private String redisInfos;

        private String redisPassword;

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getScript() {
            return script;
        }

        public void setScript(String script) {
            this.script = script;
        }

        public String getRedisInfos() {
            return redisInfos;
        }

        public void setRedisInfos(String redisInfos) {
            this.redisInfos = redisInfos;
        }

        public String getRedisPassword() {
            return redisPassword;
        }

        public void setRedisPassword(String redisPassword) {
            this.redisPassword = redisPassword;
        }

        public String getPureTable() {
            return this.getTableName().substring(this.getTableName().indexOf(".") + 1);
        }
    }


    public void validate() {

        if (StringUtils.isEmpty(this.dbAddress)) {
            throw new IllegalArgumentException("Require `dbAddress`");
        }

        if (tableConfigs == null || tableConfigs.size() == 0) {
            throw new IllegalArgumentException("Require `tableConfigs`");
        }

        for (TableConfig tc : tableConfigs) {

            if (StringUtils.isEmpty(tc.getTableName())) {
                throw new IllegalArgumentException("Table config require `tableName`");
            }

            if (StringUtils.isEmpty(tc.getScript())) {
                throw new IllegalArgumentException("Table config require `script`");
            }

            if (StringUtils.isEmpty(tc.getRedisInfos())) {
                throw new IllegalArgumentException("Table config require `script`");
            }


        }

    }

    public void setTableConfigs(List<TableConfig> tableConfigs) {
        this.tableConfigs = tableConfigs;
    }
}

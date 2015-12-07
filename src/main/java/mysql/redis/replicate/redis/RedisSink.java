package mysql.redis.replicate.redis;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.common.collect.Maps;
import mysql.redis.replicate.LoggerFactory;
import mysql.redis.replicate.Threads;
import mysql.redis.replicate.canal.AbstractSink;
import mysql.redis.replicate.config.DestinationConfig;
import mysql.redis.replicate.groovy.IRow2map;
import mysql.redis.replicate.groovy.Row2mapManager;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by wens on 15-12-3.
 */
public class RedisSink extends AbstractSink {

    private final static Logger logger = LoggerFactory.getLogger();

    private Row2mapManager row2mapManager;

    private Map<String, SinkWorker> redisSinkMap;

    public RedisSink(DestinationConfig destinationConfig, GenericObjectPoolConfig config) {
        super(destinationConfig);
        this.row2mapManager = new Row2mapManager(destinationConfig);
        redisSinkMap = Maps.newHashMap();

        for (DestinationConfig.TableConfig tableConfig : destinationConfig.getTableConfigs()) {
            SinkWorker sinkWorker = new SinkWorker(config, tableConfig.getRedisInfos(), tableConfig.getRedisPassword());
            redisSinkMap.put(tableConfig.getTableName(), sinkWorker);
            new Thread(sinkWorker, tableConfig.getTableName() + "-sink-worker-thread").start();
        }
    }

    @Override
    protected void handleInsert(DestinationConfig.TableConfig tableConfig, String mysqlLogOffset, List<CanalEntry.RowData> rowDataList) throws IOException {
        List<Map<String, String>> dataList = new ArrayList<>(rowDataList.size());
        for (CanalEntry.RowData rowData : rowDataList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

            if (logger.isDebugEnabled()) {
                logger.debug("{} receive insert data:\n{}", tableConfig.getTableName(), toString(afterColumnsList));
            }
            StringBuilder id = new StringBuilder();
            Map<String, String> row = Maps.newHashMap();
            Map<String, String> rowType = Maps.newHashMap();
            for (CanalEntry.Column c : afterColumnsList) {
                if (c.getIsKey()) {
                    id.append(c.getValue());
                }
                row.put(c.getName(), c.getValue());
                rowType.put(c.getName(), c.getMysqlType().toUpperCase());
            }
            IRow2map row2document = row2mapManager.get(tableConfig.getTableName());
            Map<String, String> map = Maps.newHashMap();
            row2document.convert(rowType, row, map);

            if (map.isEmpty()) {
                return;
            }
            map.put("_id", id.toString());
            dataList.add(map);
        }
        redisSinkMap.get(tableConfig.getTableName()).push(new ChangeEntry(tableConfig.getPureTable(), ChangeEntry.INSERT_TYPE, mysqlLogOffset, dataList));

    }

    @Override
    public void stopSafe() {
        for (SinkWorker sinkWorker : redisSinkMap.values()) {
            sinkWorker.stopSafe();
        }
    }

    @Override
    protected String getSinkLogOffset() {
        String min = null;
        for (SinkWorker sinkWorker : redisSinkMap.values()) {
            if (min == null) {
                min = sinkWorker.getSinkLogOffset();
            }
            min = minLogOffset(min, sinkWorker.getSinkLogOffset());
        }
        return min;
    }

    private String minLogOffset(String logOffset1, String logOffset2) {
        if (logOffset1 == null && logOffset2 == null) {
            return "n/a";
        }
        if (logOffset1 == null) {
            return logOffset2;
        }

        if (logOffset2 == null) {
            return logOffset1;
        }

        String[] logOffset1Parts = logOffset1.split(":");
        String[] logOffset2Parts = logOffset2.split(":");

        int i = logOffset1Parts[0].compareTo(logOffset2Parts[1]);
        if (i < 0) {
            return logOffset1;
        } else if (i > 0) {
            return logOffset2;
        } else {
            return Long.parseLong(logOffset1Parts[1]) <= Long.parseLong(logOffset2Parts[1]) ? logOffset1 : logOffset2;
        }
    }

    @Override
    protected void handleUpdate(DestinationConfig.TableConfig tableConfig, String mysqlLogOffset, List<CanalEntry.RowData> rowDataList) throws IOException {
        List<Map<String, String>> dataList = new ArrayList<>(rowDataList.size());
        for (CanalEntry.RowData rowData : rowDataList) {

            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            if (logger.isDebugEnabled()) {
                logger.debug("{} receive update data:\n{}", tableConfig.getTableName(), toString(afterColumnsList));
            }
            boolean drop = true;
            StringBuilder id = new StringBuilder();
            Map<String, String> row = Maps.newHashMap();
            Map<String, String> rowType = Maps.newHashMap();
            for (CanalEntry.Column c : afterColumnsList) {
                if (c.getIsKey()) {
                    id.append(c.getValue());
                }

                if (c.getUpdated()) {
                    drop = false;
                }

                row.put(c.getName(), c.getValue());
                rowType.put(c.getName(), c.getMysqlType().toUpperCase());
            }
            if (!drop) {
                IRow2map row2document = row2mapManager.get(tableConfig.getTableName());
                Map<String, String> map = Maps.newHashMap();
                row2document.convert(rowType, row, map);
                if (map.isEmpty()) {
                    return;
                }
                map.put("_id", id.toString());
                dataList.add(map);
            }
        }
        redisSinkMap.get(tableConfig.getTableName()).push(new ChangeEntry(tableConfig.getPureTable(), ChangeEntry.UPDATE_TYPE, mysqlLogOffset, dataList));

    }


    @Override
    protected void handleDelete(DestinationConfig.TableConfig tableConfig, String mysqlLogOffset, List<CanalEntry.RowData> rowDataList) throws IOException {
        List<Map<String, String>> dataList = new ArrayList<>(rowDataList.size());
        for (CanalEntry.RowData rowData : rowDataList) {
            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
            if (logger.isDebugEnabled()) {
                logger.debug("{} receive delete data :\n", tableConfig.getTableName(), toString(beforeColumnsList));
            }
            StringBuilder id = new StringBuilder();
            for (CanalEntry.Column c : beforeColumnsList) {
                if (c.getIsKey()) {
                    id.append(c.getValue());
                }
            }
            Map<String, String> map = Maps.newHashMap();
            map.put("_id", id.toString());
            dataList.add(map);
        }
        redisSinkMap.get(tableConfig.getTableName()).push(new ChangeEntry(tableConfig.getPureTable(), ChangeEntry.DELETE_TYPE, mysqlLogOffset, dataList));
    }


    private String toString(List<CanalEntry.Column> columns) {
        StringBuilder builder = new StringBuilder();
        for (CanalEntry.Column column : columns) {

            builder.append(column.getName() + " : " + column.getValue());
            builder.append("    type=" + column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=" + column.getUpdated());
            }
            builder.append("\n");
        }
        return builder.toString();
    }


    private static class ChangeEntry {

        static final int INSERT_TYPE = 1;
        static final int UPDATE_TYPE = 2;
        static final int DELETE_TYPE = 3;

        String tableName;
        int changeType;
        String mysqlLogOffset;
        List<Map<String, String>> dataList;

        public ChangeEntry(String tableName, int changeType, String mysqlLogOffset, List<Map<String, String>> dataList) {
            this.tableName = tableName;
            this.changeType = changeType;
            this.mysqlLogOffset = mysqlLogOffset;
            this.dataList = dataList;
        }
    }

    private static class SinkWorker implements Runnable {

        ShardedJedisPool shardedJedisPool;

        LinkedBlockingQueue<ChangeEntry> queue;

        ExecutorService executorService;

        volatile String sinkLogOffset;

        SinkWorker(GenericObjectPoolConfig config, String redisInfos, String redisPassword) {
            queue = new LinkedBlockingQueue<>(10000);
            shardedJedisPool = RedisUtils.createSharedJedisPool(config, redisInfos, redisPassword);
            executorService = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(1000),
                    Threads.makeThreadFactory("redis-execute-"), new ThreadPoolExecutor.CallerRunsPolicy());
        }

        public void push(ChangeEntry changeEntry) {
            try {
                queue.put(changeEntry);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        public void run() {

            while (true) {
                ChangeEntry changeEntry = null;
                try {
                    changeEntry = queue.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (changeEntry != null) {

                    final ChangeEntry entry = changeEntry;
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            int retry = 0;
                            while (retry <= 5) {
                                boolean broken = false;
                                Throwable fail = null;
                                ShardedJedis resource = shardedJedisPool.getResource();
                                try {
                                    sink2Redis(entry, resource);
                                    sinkLogOffset = entry.mysqlLogOffset;
                                } catch (JedisException e) {
                                    broken = true;
                                    fail = e;
                                } catch (Exception e) {
                                    fail = e;
                                } finally {
                                    if (broken) {
                                        shardedJedisPool.returnBrokenResource(resource);
                                    } else {
                                        shardedJedisPool.returnResource(resource);
                                    }
                                }

                                if (fail == null) {
                                    break;
                                }

                                logger.error("Fail to sink2Redis , retry :" + retry++, fail);
                                try {
                                    Thread.sleep(100 * retry);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        }
                    });

                }

            }
        }

        private void sink2Redis(ChangeEntry changeEntry, ShardedJedis shadedJedis) {
            ShardedJedisPipeline pipelined = shadedJedis.pipelined();
            for (Map<String, String> data : changeEntry.dataList) {
                String key = String.format("%s_%s", changeEntry.tableName, data.get("_id"));
                if (ChangeEntry.INSERT_TYPE == changeEntry.changeType || ChangeEntry.UPDATE_TYPE == changeEntry.changeType) {
                    if (data != null) {
                        for (String column : data.keySet()) {
                            pipelined.hset(key, column, data.get(column));
                        }
                    }
                } else if (ChangeEntry.DELETE_TYPE == changeEntry.changeType) {
                    pipelined.del(key);
                }
            }
            pipelined.sync();
        }


        public void stopSafe() {
            while (true) {
                if (queue == null || queue.isEmpty()) {
                    try {
                        executorService.awaitTermination(1, TimeUnit.MINUTES);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    break;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        public String getSinkLogOffset() {
            return sinkLogOffset;
        }
    }
}

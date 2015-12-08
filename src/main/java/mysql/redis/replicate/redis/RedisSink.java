package mysql.redis.replicate.redis;

import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.common.collect.Maps;
import mysql.redis.replicate.LoggerFactory;
import mysql.redis.replicate.canal.AbstractSink;
import mysql.redis.replicate.config.DestinationConfig;
import mysql.redis.replicate.groovy.IRow2map;
import mysql.redis.replicate.groovy.Row2mapManager;
import org.slf4j.Logger;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;

import java.util.List;
import java.util.Map;

/**
 * Created by wens on 15-12-3.
 */
public class RedisSink extends AbstractSink {

    private final static Logger logger = LoggerFactory.getLogger();

    private final Row2mapManager row2mapManager;


    public RedisSink(DestinationConfig destinationConfig) {
        super(destinationConfig);
        this.row2mapManager = new Row2mapManager(destinationConfig);
    }

    @Override
    public void start() {

    }

    @Override
    protected AbstractSink.SinkWorker createSinkWorker(DestinationConfig.TableConfig tableConfig) {
        return new AbstractSink.SinkWorker(tableConfig) {
            private final ShardedJedis shardedJedis = RedisUtils.createSharedJedis(tableConfig.getRedisInfos(), tableConfig.getRedisPassword());


            @Override
            protected void handleInsert(List<CanalEntry.RowData> rowDatasList) {

                ShardedJedisPipeline pipelined = shardedJedis.pipelined();
                for (CanalEntry.RowData rowData : rowDatasList) {
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
                    String key = String.format("%s_%s", tableConfig.getPureTable(), id.toString());
                    pipelined.set(key, JsonUtils.marshalToString(map));
                }

                pipelined.sync();

            }

            @Override
            protected void handleUpdate(List<CanalEntry.RowData> rowDatasList) {

                ShardedJedisPipeline pipelined = shardedJedis.pipelined();
                for (CanalEntry.RowData rowData : rowDatasList) {

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
                        String key = String.format("%s_%s", tableConfig.getPureTable(), id.toString());
                        pipelined.set(key, JsonUtils.marshalToString(map));
                    }
                }

                pipelined.sync();
            }

            @Override
            protected void handleDelete(List<CanalEntry.RowData> rowDatasList) {

                ShardedJedisPipeline pipelined = shardedJedis.pipelined();
                for (CanalEntry.RowData rowData : rowDatasList) {
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
                    String key = String.format("%s_%s", tableConfig.getPureTable(), id.toString());
                    pipelined.del(key);
                }
                pipelined.sync();

            }

        };
    }

    @Override
    protected long getCommitBatchId() {
        return 0;
    }

}

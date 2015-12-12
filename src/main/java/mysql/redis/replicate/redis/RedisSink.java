package mysql.redis.replicate.redis;

import com.alibaba.otter.canal.common.utils.JsonUtils;
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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by wens on 15-12-3.
 */
public class RedisSink extends AbstractSink {

    private final static Logger logger = LoggerFactory.getLogger();

    private final Row2mapManager row2mapManager;

    private final GenericObjectPoolConfig redisConfig;

    public RedisSink(DestinationConfig destinationConfig, GenericObjectPoolConfig redisConfig) {
        super(destinationConfig);
        this.row2mapManager = new Row2mapManager(destinationConfig);
        this.redisConfig = redisConfig;
    }

    @Override
    public void start() {

    }

    @Override
    protected AbstractSink.SinkWorker createSinkWorker(DestinationConfig.TableConfig tableConfig) {
        return new AbstractSink.SinkWorker(tableConfig) {
            private final ShardedJedisPool shardedJedisPool = RedisUtils.createSharedJedisPool(redisConfig, tableConfig.getRedisInfos(), tableConfig.getRedisPassword());

            private final ExecutorService executorService = Executors.newCachedThreadPool(Threads.makeThreadFactory("redis-sync-thread"));

            @Override
            protected void handleInsert(List<CanalEntry.RowData> rowDatasList) {

                ShardedJedis shardedJedis = shardedJedisPool.getResource();
                boolean broken = false;
                try {
                    Map<Jedis, PipelineSyncTask> jedisPipelineMap = Maps.newHashMap();
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
                        Jedis jedis = shardedJedis.getShard(key);
                        PipelineSyncTask pilelineSyncTask = jedisPipelineMap.get(jedis);
                        if (pilelineSyncTask == null) {
                            pilelineSyncTask = new PipelineSyncTask(jedis.pipelined());
                            jedisPipelineMap.put(jedis, pilelineSyncTask);
                        }
                        for (String column : map.keySet()) {
                            pilelineSyncTask.pipeline.hset(key, column, map.get(column));
                        }
                        //pilelineSyncTask.pipeline.set(key, JsonUtils.marshalToString(map));
                    }
                    executorService.invokeAll(jedisPipelineMap.values(), 1, TimeUnit.MINUTES);
                } catch (JedisException e) {
                    broken = true;
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    RedisUtils.returnResource(shardedJedisPool, shardedJedis, broken);
                }


            }


            @Override
            protected void handleUpdate(List<CanalEntry.RowData> rowDatasList) {

                ShardedJedis shardedJedis = shardedJedisPool.getResource();
                boolean broken = false;
                try {
                    Map<Jedis, PipelineSyncTask> jedisPipelineMap = Maps.newHashMap();

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

                            Jedis jedis = shardedJedis.getShard(key);
                            PipelineSyncTask pilelineSyncTask = jedisPipelineMap.get(jedis);
                            if (pilelineSyncTask == null) {
                                pilelineSyncTask = new PipelineSyncTask(jedis.pipelined());
                                jedisPipelineMap.put(jedis, pilelineSyncTask);
                            }
                            for (String column : map.keySet()) {
                                pilelineSyncTask.pipeline.hset(key, column, map.get(column));
                            }
                            //pilelineSyncTask.pipeline.set(key, JsonUtils.marshalToString(map));
                        }
                    }
                    executorService.invokeAll(jedisPipelineMap.values(), 1, TimeUnit.MINUTES);
                } catch (JedisException e) {
                    broken = true;
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    RedisUtils.returnResource(shardedJedisPool, shardedJedis, broken);
                }
            }

            @Override
            protected void handleDelete(List<CanalEntry.RowData> rowDatasList) {

                ShardedJedis shardedJedis = shardedJedisPool.getResource();
                boolean broken = false;
                try {
                    Map<Jedis, PipelineSyncTask> jedisPipelineMap = Maps.newHashMap();
                    for (CanalEntry.RowData rowData : rowDatasList) {
                        List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                        if (logger.isDebugEnabled()) {
                            logger.debug("{} receive delete data :\n {}", tableConfig.getTableName(), toString(beforeColumnsList));
                        }
                        StringBuilder id = new StringBuilder();
                        for (CanalEntry.Column c : beforeColumnsList) {
                            if (c.getIsKey()) {
                                id.append(c.getValue());
                            }
                        }
                        String key = String.format("%s_%s", tableConfig.getPureTable(), id.toString());

                        Jedis jedis = shardedJedis.getShard(key);
                        PipelineSyncTask pilelineSyncTask = jedisPipelineMap.get(jedis);
                        if (pilelineSyncTask == null) {
                            pilelineSyncTask = new PipelineSyncTask(jedis.pipelined());
                            jedisPipelineMap.put(jedis, pilelineSyncTask);
                        }
                        pilelineSyncTask.pipeline.del(key);
                    }
                    executorService.invokeAll(jedisPipelineMap.values(), 1, TimeUnit.MINUTES);
                } catch (JedisException e) {
                    broken = true;
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    RedisUtils.returnResource(shardedJedisPool, shardedJedis, broken);
                }
            }

            @Override
            public void stop() {
                super.stop();
                executorService.shutdown();
                shardedJedisPool.destroy();
            }
        };
    }


    private class PipelineSyncTask implements Callable<Void> {

        Pipeline pipeline;

        PipelineSyncTask(Pipeline pipeline) {
            this.pipeline = pipeline;
        }

        @Override
        public Void call() throws Exception {
            pipeline.sync();
            return null;
        }
    }

}

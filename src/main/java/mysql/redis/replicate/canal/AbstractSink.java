package mysql.redis.replicate.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.collect.Maps;
import mysql.redis.replicate.Lifecycle;
import mysql.redis.replicate.LoggerFactory;
import mysql.redis.replicate.config.DestinationConfig;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by wens on 15-12-4.
 */
public abstract class AbstractSink implements MessageSink, Lifecycle {

    private final static Logger logger = LoggerFactory.getLogger();

    protected DestinationConfig destinationConfig;
    private Map<String, SinkWorker> sinkWorkerMap;

    public AbstractSink(DestinationConfig destinationConfig) {
        this.destinationConfig = destinationConfig;
        this.sinkWorkerMap = Maps.newHashMap();
        for (DestinationConfig.TableConfig tableConfig : destinationConfig.getTableConfigs()) {
            SinkWorker sinkWorker = createSinkWorker(tableConfig);
            sinkWorkerMap.put(tableConfig.getTableName(), sinkWorker);
            new Thread(sinkWorker, tableConfig.getTableName() + "-sink-worker-thread").start();
        }
    }

    protected abstract SinkWorker createSinkWorker(DestinationConfig.TableConfig tableConfig);

    @Override
    public void sink(Message message) {

        for (CanalEntry.Entry entry : message.getEntries()) {

            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                String tableName = String.format("%s.%s", entry.getHeader().getSchemaName(), entry.getHeader().getTableName());
                DestinationConfig.TableConfig tableConfig = null;
                for (DestinationConfig.TableConfig t : destinationConfig.getTableConfigs()) {
                    if (t.getTableName().equals(tableName)) {
                        tableConfig = t;
                        break;
                    }
                }
                if (tableConfig == null) {
                    continue;
                }
                CanalEntry.RowChange rowChange = null;
                try {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                sinkWorkerMap.get(tableConfig.getTableName()).push(rowChange);

            }

        }
    }

    @Override
    public void stop() {
        for (SinkWorker sinkWorker : sinkWorkerMap.values()) {
            sinkWorker.stop();
        }
    }

    protected abstract class SinkWorker implements Runnable, Lifecycle {

        protected volatile boolean stopped = false;

        protected DestinationConfig.TableConfig tableConfig;

        protected BlockingQueue<CanalEntry.RowChange> rowChangeQueue;

        protected SinkWorker(DestinationConfig.TableConfig tableConfig) {
            this.tableConfig = tableConfig;
        }

        public void push(CanalEntry.RowChange rowChange) {
            try {
                rowChangeQueue.put(rowChange);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        public void run() {

            while (!stopped) {
                CanalEntry.RowChange rowChange = null;

                try {
                    rowChange = rowChangeQueue.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (rowChange != null) {

                    CanalEntry.EventType eventType = rowChange.getEventType();

                    if (eventType == CanalEntry.EventType.INSERT) {
                        handleInsert(rowChange.getRowDatasList());
                        continue;
                    }

                    if (eventType == CanalEntry.EventType.UPDATE) {
                        handleUpdate(rowChange.getRowDatasList());
                        continue;
                    }

                    if (eventType == CanalEntry.EventType.DELETE) {
                        handleDelete(rowChange.getRowDatasList());
                        continue;
                    }
                }

            }
        }

        protected abstract void handleInsert(List<CanalEntry.RowData> rowDatasList);

        protected abstract void handleUpdate(List<CanalEntry.RowData> rowDatasList);

        protected abstract void handleDelete(List<CanalEntry.RowData> rowDatasList);


        protected String toString(List<CanalEntry.Column> columns) {
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


        @Override
        public void start() {

        }

        @Override
        public void stop() {
            stopped = true;
        }

    }

    protected abstract long getCommitBatchId();
}

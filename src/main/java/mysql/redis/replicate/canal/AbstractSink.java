package mysql.redis.replicate.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import mysql.redis.replicate.LoggerFactory;
import mysql.redis.replicate.config.DestinationConfig;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created by wens on 15-12-4.
 */
public abstract class AbstractSink implements MessageSink {

    private final static Logger logger = LoggerFactory.getLogger();

    protected DestinationConfig destinationConfig;

    public AbstractSink(DestinationConfig destinationConfig) {
        this.destinationConfig = destinationConfig;
    }

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
                String logFileOffset = String.format("%s:%d", entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset());
                CanalEntry.RowChange rowChange = null;
                try {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }
                CanalEntry.EventType eventType = rowChange.getEventType();
                //todo refactor struct
                if (eventType == CanalEntry.EventType.INSERT) {
                    List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
                    try {
                        handleInsert(tableConfig, logFileOffset, rowDataList);
                    } catch (Exception e) {
                        logger.error("Handle row data fail.", e);
                    }

                    continue;
                }

                if (eventType == CanalEntry.EventType.UPDATE) {
                    List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
                    try {
                        handleUpdate(tableConfig, logFileOffset, rowDataList);
                    } catch (Exception e) {
                        logger.error("Handle row data fail.", e);
                    }
                    continue;
                }

                if (eventType == CanalEntry.EventType.DELETE) {
                    List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
                    try {
                        handleDelete(tableConfig, logFileOffset, rowDataList);
                    } catch (Exception e) {
                        logger.error("Handle row data fail.", e);
                    }
                    continue;
                }
            }

        }
    }

    protected abstract void handleDelete(DestinationConfig.TableConfig tableConfig, String mysqlLogOffset, List<CanalEntry.RowData> rowDataList) throws IOException;

    protected abstract void handleUpdate(DestinationConfig.TableConfig tableConfig, String mysqlLogOffset, List<CanalEntry.RowData> rowDataList) throws IOException;

    protected abstract void handleInsert(DestinationConfig.TableConfig tableConfig, String mysqlLogOffset, List<CanalEntry.RowData> rowDataList) throws IOException;

    public abstract void stopSafe();

    protected abstract String getSinkLogOffset();
}

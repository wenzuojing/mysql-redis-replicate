package mysql.redis.replicate.groovy;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import mysql.redis.replicate.config.DestinationConfig;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Created by wens on 15-11-13.
 */
public class Row2mapManager {

    public final static String SCRIPT_TEMPLATE_FILE = "row2map.groovy.tpl";

    private DestinationConfig destinationConfig;

    private Map<String, IRow2map> row2mapMap = Maps.newHashMap();

    public Row2mapManager(DestinationConfig destinationConfig) {
        this.destinationConfig = destinationConfig;
        init();
    }

    private void init() {

        String template;
        try {
            template = Files.toString(new File(Thread.currentThread().getContextClassLoader().getResource(SCRIPT_TEMPLATE_FILE).getPath()), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (DestinationConfig.TableConfig tableConfig : destinationConfig.getTableConfigs()) {
            row2mapMap.put(tableConfig.getTableName(), new Row2map(template.replace("#script#", tableConfig.getScript())));
        }
    }

    public IRow2map get(String tableName) {
        return row2mapMap.get(tableName);
    }
}

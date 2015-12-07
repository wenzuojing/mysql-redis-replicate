package mysql.redis.replicate.groovy;

import java.util.Map;

/**
 * Created by wens on 15-11-13.
 */
public interface IRow2map {
    void convert(Map<String, String> type, Map<String, String> row, Map<String, String> map);

}

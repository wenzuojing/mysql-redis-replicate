import mysql.redis.replicate.groovy.IRow2map

import java.text.ParseException
import java.text.SimpleDateFormat;

import java.util.Date;
/**
 * Created by wens on 15-11-13.
 */
public class MyRow2map implements IRow2map {

    @Override
    public void convert(Map<String, String> type , Map<String, String> row, Map<String, String> map) {
        #script#
    }
}

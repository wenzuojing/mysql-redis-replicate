package mysql.redis.replicate.groovy;

import groovy.lang.GroovyClassLoader;

import java.util.Map;

/**
 * Created by wens on 15-11-13.
 */
public class Row2map implements IRow2map {

    private IRow2map delegate;

    public Row2map(String source) {

        GroovyClassLoader groovyCl = new GroovyClassLoader();

        Class aClass = groovyCl.parseClass(source);

        try {
            delegate = (IRow2map) aClass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }


    @Override
    public void convert(Map<String, String> type, Map<String, String> row, Map<String, String> map) {
        delegate.convert(type, row, map);
    }
}

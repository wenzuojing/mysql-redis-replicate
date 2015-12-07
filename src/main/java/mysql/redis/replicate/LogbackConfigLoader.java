package mysql.redis.replicate;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * Created by wens on 15-10-21.
 */
public class LogbackConfigLoader {


    public static void load() throws IOException {
        URL resource = Thread.currentThread().getContextClassLoader().getResource("logback.xml");
        load(resource.getFile());
    }

    public static void main(String[] args) throws IOException {
        load();
    }


    public static void load(String location) throws IOException {

        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        File externalConfigFile = new File(location);
        if (!externalConfigFile.exists()) {
            throw new IOException("Logback External Conf File Parameter does not reference a file that exists");
        } else {
            if (!externalConfigFile.isFile()) {
                throw new IOException("Logback External Conf File Parameter exists, but does not reference a file");
            } else {
                if (!externalConfigFile.canRead()) {
                    throw new IOException("Logback External Conf File exists and is a file, but cannot be read.");
                } else {
                    JoranConfigurator configurator = new JoranConfigurator();
                    configurator.setContext(lc);
                    lc.reset();
                    try {
                        configurator.doConfigure(location);
                    } catch (JoranException e) {
                        throw new RuntimeException(e);
                    }
                    StatusPrinter.printInCaseOfErrorsOrWarnings(lc);
                }
            }
        }

    }
}

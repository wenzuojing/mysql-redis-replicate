package mysql.redis.replicate.web;

import mysql.redis.replicate.Conf;
import mysql.redis.replicate.CoordinatorController;
import mysql.redis.replicate.LoggerFactory;
import mysql.redis.replicate.canal.ControllerService;
import mysql.redis.replicate.config.DestinationConfigManager;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;

/**
 * Created by wens on 15-11-16.
 */
public class WebConsole {

    private final Logger logger = LoggerFactory.getLogger();

    private Server server;

    public WebConsole(Conf conf, ControllerService controllerService, DestinationConfigManager destinationConfigManager, CoordinatorController coordinatorController) {
        String siteResourcePath = Thread.currentThread().getContextClassLoader().getResource("site").getPath();
        this.server = new Server();
        SelectChannelConnector connector = new SelectChannelConnector();
        logger.info("Web console bind on 0.0.0.0:{}", conf.getWebConsolePort());
        connector.setPort(conf.getWebConsolePort());
        server.addConnector(connector);

        ResourceHandler resource_handler = new ResourceHandler();
        resource_handler.setDirectoriesListed(true);
        resource_handler.setWelcomeFiles(new String[]{"index.html"});
        resource_handler.setResourceBase(siteResourcePath);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.addServlet(new ServletHolder(new TestConfigServlet(destinationConfigManager)), "/config/test");
        context.addServlet(new ServletHolder(new SaveConfigServlet(destinationConfigManager)), "/config/save");
        context.addServlet(new ServletHolder(new GetConfigServlet(destinationConfigManager)), "/config/get");
        context.addServlet(new ServletHolder(new AllDestinationServlet(destinationConfigManager)), "/destination/all");
        context.addServlet(new ServletHolder(new DestinationOptServlet(coordinatorController)), "/destination/opt");
        context.addServlet(new ServletHolder(new EndpointServlet(destinationConfigManager, controllerService)), "/endpoint");
        context.addServlet(new ServletHolder(new AliveServerServlet(coordinatorController)), "/alive/server/ids");
        context.addServlet(new ServletHolder(new MonitorServlet()), "/monitor");

        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[]{resource_handler, context});
        server.setHandler(handlers);

    }

    public void start() {
        try {
            logger.info("Web console start");
            server.start();
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public void stop() {
        try {
            logger.info("Web console stop");
            server.stop();
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

}

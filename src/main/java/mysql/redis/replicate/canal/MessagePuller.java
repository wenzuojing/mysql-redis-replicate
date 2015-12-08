package mysql.redis.replicate.canal;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import mysql.redis.replicate.Lifecycle;
import mysql.redis.replicate.ZkPathUtils;
import mysql.redis.replicate.ZookeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by wens on 15-10-15.
 */
public class MessagePuller extends Thread implements Lifecycle {

    private final static Logger logger = LoggerFactory.getLogger(ControllerService.class);

    private volatile boolean running = true;
    private volatile boolean stopped = false;

    private final String destination;

    private final ScheduledExecutorService scheduledExecutorService;

    private final com.alibaba.otter.canal.server.CanalService canalService;

    private final int batchSize;

    private final AbstractSink writer;

    private final ConcurrentSkipListSet<Long> batchIds = new ConcurrentSkipListSet<>();

    public MessagePuller(final int batchSize, final String destination, final com.alibaba.otter.canal.server.CanalService canalService, final AbstractSink writer) {
        this.destination = destination;
        this.canalService = canalService;
        this.batchSize = batchSize;
        this.writer = writer;
        this.scheduledExecutorService = Executors.newScheduledThreadPool(1);

        this.scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                ZookeeperUtils.writeData(ZkPathUtils.getDestinationSinkLogOffsetPath(destination), writer.getCommitBatchId(), true);
            }
        }, 0, 10, TimeUnit.SECONDS);

        setName("puller-" + this.destination + "-thread");
    }


    public void safeStop() {

        if (stopped) {
            return;
        }

        running = false;
        while (!stopped) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        writer.stop();
        scheduledExecutorService.shutdownNow();
    }


    @Override
    public void run() {

        try {
            ClientIdentity clientIdentity = new ClientIdentity(destination, (short) 1);
            canalService.subscribe(clientIdentity);
            int retry = 0;
            while (running) {
                Message message;
                message = canalService.getWithoutAck(clientIdentity, this.batchSize, 100L, TimeUnit.MICROSECONDS); // 获取指定数量的数据

                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    continue;
                } else {
                    try {
                        writer.sink(message);
                        //canalService.ack(clientIdentity, batchId); // 提交确认
                        batchIds.add(batchId);
                        retry = 0;
                    } catch (Exception e) {
                        logger.error("Got an Exception, when sink message.", e);
                        canalService.rollback(clientIdentity, batchId);
                        retry++;
                        try {
                            Thread.sleep(5 * 1000);
                        } catch (InterruptedException e1) {
                            Thread.currentThread().interrupt();
                        }
                        if (retry >= 5) {
                            throw new RuntimeException("Retry 5 times ,but still fail.");
                        }
                    }
                }
            }

        } finally {
            stopped = true;
        }
    }

}

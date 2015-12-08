package mysql.redis.replicate.canal;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import mysql.redis.replicate.Lifecycle;
import mysql.redis.replicate.ZkPathUtils;
import mysql.redis.replicate.ZookeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.*;

/**
 * Created by wens on 15-10-15.
 */
public class MessagePuller extends Thread implements Lifecycle {

    private final static Logger logger = LoggerFactory.getLogger(ControllerService.class);

    private volatile boolean running = true;
    private volatile boolean stopped = false;

    private final String destination;

    private final com.alibaba.otter.canal.server.CanalService canalService;

    private final int batchSize;

    private final AbstractSink writer;

    private final ScheduledFuture<?> scheduledFuture;

    private final ConcurrentSkipListSet<Long> commitBatchIds = new ConcurrentSkipListSet() ;

    private final ClientIdentity clientIdentity ;

    public MessagePuller(final int batchSize, final String destination, final com.alibaba.otter.canal.server.CanalService canalService, final AbstractSink writer , ScheduledExecutorService scheduledExecutorService ) {
        this.destination = destination;
        this.canalService = canalService;
        this.batchSize = batchSize;
        this.writer = writer;
        this.clientIdentity = new ClientIdentity(destination, (short) 1) ;

        setName("puller-" + this.destination + "-thread");
        scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try{
                    long commitBatchId = writer.getCommitBatchId();
                    NavigableSet<Long> batchIds = commitBatchIds.headSet(commitBatchId, false);
                    Iterator<Long> iterator = batchIds.iterator();
                    synchronized (canalService){
                        while ( iterator.hasNext() ){
                            Long batchId = iterator.next();
                            canalService.ack(clientIdentity ,  batchId);
                            commitBatchIds.remove(batchId);
                        }
                    }

                }catch (Exception e){
                    e.printStackTrace();
                }

            }
        }, 0, 10 , TimeUnit.SECONDS);
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
        scheduledFuture.cancel(true);
    }


    @Override
    public void run() {

        try {
            canalService.subscribe(clientIdentity);
            while (running) {

                Message message;
                synchronized (canalService){
                    message = canalService.getWithoutAck(clientIdentity, this.batchSize, 100L, TimeUnit.MICROSECONDS); // 获取指定数量的数据
                }


                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    continue;
                } else {
                    try {
                        writer.sink(message);
                        delayAck(batchId) ;
                    } catch (Exception e) {
                        logger.error("Got an Exception, when sink message.", e);
                        synchronized (canalService){
                            canalService.rollback(clientIdentity, batchId);
                        }

                    }
                }
            }
        } finally {
            stopped = true;
        }
    }

    private void delayAck(long batchId) {
        commitBatchIds.add(batchId);
    }


}

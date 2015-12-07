package mysql.redis.replicate;

import org.I0Itec.zkclient.ZkClient;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by wens on 15/12/5.
 */
public class ZookeeperLeaderElectorTest {

    public static void main(String[] args) throws InterruptedException {
        final ZkClient zkClient = new ZkClient("localhost");
        ZkPathUtils.init("", zkClient);

        final AtomicLong atomicLong = new AtomicLong(0);

        final ZookeeperLeaderElector.LeaderListener leaderListener = new ZookeeperLeaderElector.LeaderListener() {
            public void onBecomingLeader() {
                atomicLong.incrementAndGet();
            }
        };

        int n = 3;
        final CountDownLatch countDownLatch = new CountDownLatch(n);
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(n);

        for (int i = 0; i < n; i++) {
            final int ii = i;
            new Thread() {
                @Override
                public void run() {
                    try {
                        cyclicBarrier.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                    new ZookeeperLeaderElector(String.valueOf(ii), leaderListener).start();

                    countDownLatch.countDown();
                }
            }.start();
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (atomicLong.get() != 1) {
            throw new RuntimeException("Not pass");
        }

        zkClient.delete(ZkPathUtils.getControllerPath());

        Thread.sleep(1000);

        if (atomicLong.get() != 2) {
            throw new RuntimeException("Not pass");
        }


    }


}

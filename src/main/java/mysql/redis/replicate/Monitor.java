package mysql.redis.replicate;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by wens on 15-10-20.
 */
public class Monitor {

    public static class MonitorData extends Thread {
        public MonitorData(){
            start();
        }
        public volatile String logfileName = "";
        public volatile long logfileOffset;
        public AtomicLong insertCount = new AtomicLong(0);
        public AtomicLong updateCount = new AtomicLong(0);
        public AtomicLong deleteCount = new AtomicLong(0);
        public AtomicLong speed = new AtomicLong(0);

        @Override
        public void run() {
            long lastTotal = 0 ;
            while (true){
                long total = insertCount.get() + updateCount.get() + deleteCount.get() ;
                speed.set( ( total - lastTotal) / 5 );
                lastTotal = total ;
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }


            }

        }
    }

    private static ConcurrentHashMap<String, MonitorData> cache = new ConcurrentHashMap<>();



    public static void updateLogPosition(String dest, String logfileName, long logfileOffset) {
        MonitorData monitorData = getMonitorData(dest);
        monitorData.logfileName = logfileName;
        monitorData.logfileOffset = logfileOffset;
    }

    public static void incrInsertCount(String dest, int step) {
        getMonitorData(dest).insertCount.addAndGet(step);
    }

    public static void incrUpdateCount(String dest, int step) {
        getMonitorData(dest).updateCount.addAndGet(step);
    }

    public static void incrDeleteCount(String dest, int step) {
        getMonitorData(dest).deleteCount.addAndGet(step);
    }

    public static MonitorData getMonitorData(String dest) {
        MonitorData monitorData = cache.get(dest);

        if (monitorData == null) {
            monitorData = new MonitorData();
            MonitorData old = cache.putIfAbsent(dest, monitorData);
            if (old != null) {
                monitorData = old;
            }
        }
        return monitorData;
    }

}

package mysql.redis.replicate;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by wens on 15-11-5.
 */
public class Threads {

    public static ThreadFactory makeThreadFactory(final String baseName) {
        return new ThreadFactory() {
            AtomicLong atomicLong = new AtomicLong(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("%s-%s", baseName, atomicLong.incrementAndGet()));
            }
        };
    }
}

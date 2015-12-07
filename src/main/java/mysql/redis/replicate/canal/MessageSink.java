package mysql.redis.replicate.canal;

import com.alibaba.otter.canal.protocol.Message;

/**
 * Created by wens on 15-10-20.
 */
public interface MessageSink {
    void sink(Message message);
}

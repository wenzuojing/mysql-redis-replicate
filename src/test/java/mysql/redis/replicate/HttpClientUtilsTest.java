package mysql.redis.replicate;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by wens on 15/12/6.
 */
public class HttpClientUtilsTest {

    public static void main(String[] args) throws IOException, InterruptedException {

        Process nc = Runtime.getRuntime().exec("nc -l 9999");
        new Thread() {
            @Override
            public void run() {
                String ret = HttpClientUtils.post("http://localhost:9999/post", Tuple.of("name", "wens"));
                System.out.println(ret);
            }
        }.start();

        Thread.sleep(1000);

        StringBuffer resp = new StringBuffer();

        resp.append("HTTP/1.1 200 OK\n");
        resp.append("Content-Length: 2\n");
        resp.append("Content-Type: text/html; charset=utf-8\n\n");
        resp.append("ok");
        OutputStream outputStream = nc.getOutputStream();
        outputStream.write(resp.toString().getBytes());
        outputStream.flush();
        Thread.sleep(100);


    }
}

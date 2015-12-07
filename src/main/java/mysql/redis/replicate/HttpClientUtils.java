package mysql.redis.replicate;

import com.google.common.base.Charsets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

/**
 * Created by wens on 15/12/6.
 */
public class HttpClientUtils {

    public static String post(String url, Tuple<String, String>... params) {

        HttpURLConnection httpURLConnection = null;
        try {
            httpURLConnection = (HttpURLConnection) (new URL(url).openConnection());

            httpURLConnection.setRequestMethod("GET");
            httpURLConnection.setReadTimeout(60000);
            httpURLConnection.setDoInput(true);
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            if (params != null && params.length != 0) {
                StringBuilder playload = new StringBuilder(1);

                for (Tuple<String, String> p : params) {
                    if (playload.length() != 0) {
                        playload.append("&");
                    }
                    playload.append(p.getOne()).append("=").append(URLEncoder.encode(p.getTwo(), "utf-8"));
                }

                httpURLConnection.getOutputStream().write(playload.toString().getBytes(Charsets.UTF_8));
            }

            if (httpURLConnection.getResponseCode() == 200) {

                InputStream inputStream = httpURLConnection.getInputStream();

                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1024);
                byte[] buf = new byte[1024];
                int n;
                while ((n = inputStream.read(buf)) != -1) {
                    byteArrayOutputStream.write(buf, 0, n);
                }
                return byteArrayOutputStream.toString();
            } else {
                throw new RuntimeException("Response code is " + httpURLConnection.getResponseCode());
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (httpURLConnection != null) {
                httpURLConnection.disconnect();
            }
        }

    }
}

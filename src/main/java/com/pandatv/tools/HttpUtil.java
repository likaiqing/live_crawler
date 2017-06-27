package com.pandatv.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * Created by likaiqing on 2017/5/26.
 */
public class HttpUtil {
    private static final Logger logger = LoggerFactory.getLogger(HttpUtil.class);

    public static String sendGet(String url) {
        String result = "";
        BufferedReader in = null;
        try {
            String urlNameString = url;
            URL realUrl = new URL(urlNameString);
            URLConnection connection = realUrl.openConnection();
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("Content-Type", "text/html; charset=utf-8");
            connection.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            connection.connect();
            connection.getContentType();
//            logger.info("sendget,url:" + url);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return result;
    }

    public static void main(String[] args) throws UnsupportedEncodingException {
        String encode = URLEncoder.encode("crawler_punch.gif?event=anchor&par_d=20170531&rid=http://chushou.tv/room/1384755.htm&nm=单纯小魔王&tt=国服第一猴，一秒三棍瞬间爆炸！&cate=王者荣耀&pop_s=5.8万&pop_n=58000&task=chushouanchor&plat=chushou&url_c=all&c_time=2017-05-31 14:46:06&url=http://chushou.tv/live/list.htm&t_ran=20170531 14:46:00-dah0odysf4", "UTF-8");
        String decode = URLDecoder.decode(encode, "UTF-8");
        sendGet("http://dd.panda.tv/" + encode);
    }
}

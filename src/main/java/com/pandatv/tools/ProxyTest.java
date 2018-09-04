package com.pandatv.tools;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.*;

/**
 * @author: likaiqing
 * @create: 2018-09-04 12:41
 **/
public class ProxyTest {
    public static void main(String args[]) throws Exception {
        // 要访问的目标页面
        String targetUrl = "https://www.zhanqi.tv/api/static/v2.1/live/list/20/1.json";
        //String targetUrl = "http://proxy.abuyun.com/switch-ip";
        //String targetUrl = "http://proxy.abuyun.com/current-ip";

        // 代理服务器
        String proxyServer = "http-pro.abuyun.com";
        int proxyPort = 9010;

        // 代理隧道验证信息
        String proxyUser = "H7ABSOS1FI3M9I4P";
        String proxyPass = "97CCB7E9284ACAF0";

        try {
            URL url = new URL(targetUrl);

            Authenticator.setDefault(new ProxyAuthenticator(proxyUser, proxyPass));

            // 创建代理服务器地址对象
            InetSocketAddress addr = new InetSocketAddress(proxyServer, proxyPort);
            // 创建HTTP类型代理对象
            Proxy proxy = new Proxy(Proxy.Type.HTTP, addr);

            // 设置通过代理访问目标页面
            HttpURLConnection connection = (HttpURLConnection) url.openConnection(proxy);
            // 设置IP切换头
            connection.setRequestProperty("Proxy-Switch-Ip", "yes");

            // 解析返回数据
            byte[] response = readStream(connection.getInputStream());

            System.out.println(new String(response));
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
        }
    }

    /**
     * 将输入流转换成字符串
     *
     * @param inStream
     * @return
     * @throws Exception
     */
    public static byte[] readStream(InputStream inStream) throws Exception {
        ByteArrayOutputStream outSteam = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len = -1;

        while ((len = inStream.read(buffer)) != -1) {
            outSteam.write(buffer, 0, len);
        }
        outSteam.close();
        inStream.close();

        return outSteam.toByteArray();
    }
}
class ProxyAuthenticator extends Authenticator {
    private String user, password;

    public ProxyAuthenticator(String user, String password) {
        this.user     = user;
        this.password = password;
    }

    protected PasswordAuthentication getPasswordAuthentication() {
        return new PasswordAuthentication(user, password.toCharArray());
    }
}

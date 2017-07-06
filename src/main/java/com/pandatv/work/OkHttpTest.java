package com.pandatv.work;

import com.pandatv.common.Const;
import com.squareup.okhttp.*;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.concurrent.TimeUnit;

/**
 * Created by likaiqing on 2017/7/5.
 */
public class OkHttpTest {
    public static void main(String[] args) throws IOException {
        OkHttpClient client = new OkHttpClient();
        client.setAuthenticator(new Authenticator() {
            public Request authenticate(Proxy proxy, Response response) throws IOException {
                String credential = Credentials.basic("H04Y776P798V9KWP", "680DB7FCBC8D123C");
                return response.request().newBuilder()
                        .header("Authorization", credential)
                        .build();
            }

            public Request authenticateProxy(Proxy proxy, Response response)
                    throws IOException {
                return null;
            }
        }).setProxy(new Proxy(Proxy.Type.HTTP,new InetSocketAddress(Const.ABUYUNPHOST,Const.ABUYUNPORT))).setConnectTimeout(10, TimeUnit.SECONDS);
        Request request = new Request.Builder()
                .url("http://1212.ip138.com/ic.asp")
                .header("Proxy-Authorization","Basic " + (new BASE64Encoder()).encode((Const.GENERATORKEY + ":" + Const.GENERATORPASS).getBytes()))
                .build();

        Request request2 = new Request.Builder()
                .url("http://1212.ip138.com/ic.asp")
                .header("Proxy-Authorization","Basic " + (new BASE64Encoder()).encode((Const.GENERATORKEY + ":" + Const.GENERATORPASS).getBytes()))
                .build();
        //同步调用
        Response response = client.newCall(request).execute();

        //异步调用
        client.newCall(request2).enqueue(new Callback() {
            @Override
            public void onFailure(Request request, IOException e) {

            }

            @Override
            public void onResponse(Response response) throws IOException {
                System.out.println(response.body().string());
            }
        });
        System.out.println(response.body().string());
    }
}

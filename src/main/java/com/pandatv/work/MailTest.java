package com.pandatv.work;

import com.pandatv.mail.SendMail;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by likaiqing on 2016/11/25.
 */
public class MailTest {
    public static void main(String[] args) {
//        SendMail mail = new SendMail("likaiqing@panda.tv", "");
//        mail.sendAlarmmail("斗鱼爬取结束", "爬取时间:"+"\n\r"+"timeouturl:"+"\r"+"failedurl:");
        List<String> urls = new ArrayList<>();
        urls.add("http://douyu.com");
        urls.add("http://huya.com");
        urls.add("http://longzhu.com");
        String s = urls.toString();
        System.out.println(s);
    }
}

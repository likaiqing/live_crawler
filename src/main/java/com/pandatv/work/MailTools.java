package com.pandatv.work;

import com.pandatv.mail.SendMail;

/**
 * Created by likaiqing on 2016/11/25.
 */
public class MailTools {
    public static void sendTaskMail(String s, String s1, String s2, int size, StringBuffer timeOutUrl, StringBuffer failedUrl) {
        SendMail mail = new SendMail("likaiqing@panda.tv", "");
        StringBuffer theMessage = new StringBuffer();
        theMessage.append("<table border=\"1\" cellspacing=\"0\"> ");
        theMessage.append("<tr>").append("<th>").append("抓取时间").append("</th>");
        theMessage.append("<th>").append(s1).append("</th>").append("</tr>");
        theMessage.append("<tr>").append("<th>").append("抓取用时").append("</th>");
        theMessage.append("<th>").append(s2).append("</th>").append("</tr>");
        theMessage.append("<tr>").append("<th>").append("入库条数").append("</th>");
        theMessage.append("<th>").append(size).append("</th>").append("</tr>");
        theMessage.append("<tr>").append("<th>").append("超时urls").append("</th>");
        theMessage.append("<th>").append(timeOutUrl.toString()).append("</th>").append("</tr>");
        theMessage.append("<tr>").append("<th>").append("process失败urls").append("</th>");
        theMessage.append("<th>").append(failedUrl.toString()).append("</th>").append("</tr>");
        theMessage.append("</table>");
        mail.sendAlarmmail(s, theMessage.toString());
    }

    public static void sendAlarmmail(String douyuexit, String s) {
        SendMail mail = new SendMail("likaiqing@panda.tv", "");
        mail.sendAlarmmail(douyuexit, s);
    }

    public static void main(String[] args) {
        sendTaskMail("test", "1:00<-->2:00", "1000秒", 100, new StringBuffer("timeouturl"), new StringBuffer("failedUrl"));
    }
}

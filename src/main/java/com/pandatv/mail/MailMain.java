package com.pandatv.mail;

/**
 * Created by likaiqing on 2016/12/6.
 */
public class MailMain {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage:mailTile mailContent attachFile");
            System.exit(1);
        }
        SendMail sendMail = new SendMail("fengwenbo@panda.tv", "baimuhai@panda.tv,lushenggang@panda.tv,wangshu@panda.tv,likaiqing@panda.tv");
        boolean b = sendMail.sendAttachMail(args[0], args[1], args[2]);
        if (!b) {
            SendMail alarmMail = new SendMail("likaiqing@panda.tv");
            alarmMail.sendAlarmmail("爬取主播分析发送失败", args[0] + ";" + args[1] + ";" + args[2]);
        }
    }
}

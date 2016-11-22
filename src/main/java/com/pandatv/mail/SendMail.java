package com.pandatv.mail;

import org.apache.commons.lang.StringUtils;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.util.Date;
import java.util.Properties;

/**
 * Created by likaiqing on 2016/11/22.
 */
public class SendMail {
    private static String sendto = "";
    private static String sendcc = "";

    public SendMail(String to, String cc) {
        this.sendto = to;
        this.sendcc = cc;
    }

    SendMail(String to) {
        this.sendto = to;
    }
    private boolean sendHtmlMail(String mailname,String mailstr) {

        Properties pro = new Properties();
        pro.put("mail.smtp.host", "114.242.236.200");
        pro.put("mail.smtp.port", "25");
        pro.put("mail.smtp.auth", "false");
        MyAuthenticator authenticator = new MyAuthenticator("l287816895", "x19890606");
        // 根据邮件会话属性和密码验证器构造一个发送邮件的session
        Session sendMailSession = Session.getDefaultInstance(pro, authenticator);
        try {
            // 根据session创建一个邮件消息
            Message mailMessage = new MimeMessage(sendMailSession);
            // 创建邮件发送者地址
            Address from = new InternetAddress("mailman@pandatv.com");
            // 设置邮件消息的发送者
            mailMessage.setFrom(from);
            // 创建邮件的接收者地址，并设置到邮件消息中
            Address to = new InternetAddress(sendto);
            // Message.RecipientType.TO属性表示接收者的类型为TO
            mailMessage.setRecipient(Message.RecipientType.TO, to);
            if (!StringUtils.isEmpty(sendcc)){
                String[] cclist=sendcc.split(",");
                for(String str:cclist){
                    Address cc = new InternetAddress(str);
//				 Message.RecipientType.TO属性表示接收者的类型为TO
                    mailMessage.addRecipient(Message.RecipientType.CC, cc);
                }
            }
            // 设置邮件消息的主题
            mailMessage.setSubject(mailname);
            // 设置邮件消息发送的时间
            mailMessage.setSentDate(new Date());
            // MiniMultipart类是一个容器类，包含MimeBodyPart类型的对象
            Multipart mainPart = new MimeMultipart();
            // 创建一个包含HTML内容的MimeBodyPart
            BodyPart html = new MimeBodyPart();
            // 设置HTML内容
            html.setContent(mailstr, "text/html; charset=utf-8");

            mainPart.addBodyPart(html);
            // 将MiniMultipart对象设置为邮件内容
            mailMessage.setContent(mainPart);
            // 发送邮件
            Transport.send(mailMessage);
            return true;
        } catch (MessagingException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public void sendAlarmmail(String title, String data) {
        sendHtmlMail(title,data);
    }
}

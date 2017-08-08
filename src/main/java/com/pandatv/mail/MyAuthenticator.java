package com.pandatv.mail;

//import javax.mail.Authenticator;
//import javax.mail.PasswordAuthentication;

/**
 * Created by likaiqing on 2016/11/22.
 */
public class MyAuthenticator /**extends Authenticator*/ {
    private String username;
    private String password;
    public MyAuthenticator(){

    }

    public MyAuthenticator(String username, String password) {
        this.username = username;
        this.password = password;
    }

//    @Override
//    protected PasswordAuthentication getPasswordAuthentication() {
//        return new PasswordAuthentication(username, password);
//    }

    public String getUsername() {
        return username;
    }
    public void setUsername(String username) {
        this.username = username;
    }
    public String getPassword() {
        return password;
    }
    public void setPassword(String password) {
        this.password = password;
    }
}

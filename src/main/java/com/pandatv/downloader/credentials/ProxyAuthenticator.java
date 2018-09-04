package com.pandatv.downloader.credentials;

import java.net.Authenticator;
import java.net.PasswordAuthentication;

/**
 * @author: likaiqing
 * @create: 2018-09-04 12:54
 **/
public class ProxyAuthenticator extends Authenticator {
    private String user, password;

    public ProxyAuthenticator(String user, String password) {
        this.user = user;
        this.password = password;
    }

    protected PasswordAuthentication getPasswordAuthentication() {
        return new PasswordAuthentication(user, password.toCharArray());
    }
}

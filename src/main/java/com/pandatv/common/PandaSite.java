package com.pandatv.common;

import us.codecraft.webmagic.Site;

/**
 * Created by likaiqing on 2016/11/18.
 */
public class PandaSite extends Site {
    private boolean useCertificate=false;
    private String certiAppKey;
    private String certiPassword;

    public static PandaSite me() {
        return new PandaSite();
    }
    public PandaSite setUseCertificate(boolean useCertificate) {
        this.useCertificate = useCertificate;
        return this;
    }
    public PandaSite setCertiAppKey(String certiAppKey) {
        this.certiAppKey = certiAppKey;
        return this;
    }
    public PandaSite setCertiPassword(String certiPassword) {
        this.certiPassword = certiPassword;
        return this;
    }
    public boolean getUseCertifiacte(){
        return this.useCertificate;
    }
    public String getCertiAppKey(){
        return this.certiAppKey;
    }
    public String getCertiPassword(){
        return this.certiPassword;
    }
}

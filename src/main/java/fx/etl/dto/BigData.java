package fx.etl.dto;

/**
 * @author zhangdekun on 2019/2/21.
 */
public class BigData {
    private String domain;
    private String ip;
    private PhoneContacts data;

    public String getDomain() {
        return domain;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public PhoneContacts getData() {
        return data;
    }

    public void setData(PhoneContacts data) {
        this.data = data;
    }
}

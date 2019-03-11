package fx.etl.dto;

/**
 * @author zhangdekun on 2019/2/21.
 */
public class PhoneContacts {
    private String user_id;
    private String name;
    private String tel;
    private String mac;
    private InnerData innerData;

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public InnerData getInnerData() {
        return innerData;
    }

    public void setInnerData(InnerData innerData) {
        this.innerData = innerData;
    }
}

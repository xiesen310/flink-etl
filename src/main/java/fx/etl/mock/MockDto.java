package fx.etl.mock;

import com.alibaba.fastjson.JSONObject;
import fx.etl.dto.BigData;
import fx.etl.dto.InnerData;
import fx.etl.dto.PhoneContacts;

/**
 * @author zhangdekun on 2019/2/28.
 */
public class MockDto {
    public static String phoneConcactsJonsData(){
        BigData bigData = new BigData();
        bigData.setDomain("fx");
        bigData.setIp("192.168.1.111");
        PhoneContacts phoneContacts = new PhoneContacts();
        phoneContacts.setName("zhangdekun");
        phoneContacts.setTel("13683674764");
        phoneContacts.setUser_id("111111");
        phoneContacts.setMac("ab:12:d2:4d:ff");
        InnerData innerData = new InnerData();
        innerData.setD1("d4");
        phoneContacts.setInnerData(innerData);
        bigData.setData(phoneContacts);
        return JSONObject.toJSONString(bigData);
    }
}

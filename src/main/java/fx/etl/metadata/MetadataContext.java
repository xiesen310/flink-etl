package fx.etl.metadata;


import com.alibaba.fastjson.JSONObject;
import fx.etl.connector.ConnectorType;

import java.util.List;
import java.util.Properties;

/**
 * @author zhangdekun on 2019/1/14.
 */
public class MetadataContext {
    private Properties properties = new Properties();

    private MetaData metaData = new MetaData();
    private String[] queryResultFieldNames;
    private String name;
    private String aliasName;
    private ConnectorType connectorType = ConnectorType.CONSOLE;

    public MetadataContext(String name,String aliasName){
        this.name = name;
        this.aliasName = aliasName;
    }
    public MetaData usedMetaData(){
        //todo 使用的metadata可能是元数据管理中的子集
        return metaData;
    }

    /**
     * 当metadata是sql查询的result字段时，需要保证顺序的不变性
     *  参数中的字段顺序就是metadata的schema顺序
     * @return
     */
    public MetaData queryResultMetaData(){
        if(queryResultFieldNames != null){
            JSONObject.parseObject(metaData.getJsonSchema());
        }
        return metaData;
    }
    public void setConnectorType(ConnectorType connectorType) {
        this.connectorType = connectorType;
    }

    public ConnectorType getConnectorType() {
        return connectorType;
    }

    public Properties getProperties() {
        return properties;
    }

    public MetaData getMetaData() {
        return metaData;
    }

    public String getName() {
        return name;
    }

    public String getAliasName() {
        return aliasName;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAliasName(String aliasName) {
        this.aliasName = aliasName;
    }

    public void setQueryResultFieldNames(String[] queryResultFieldNames) {
        this.queryResultFieldNames = queryResultFieldNames;
    }
}

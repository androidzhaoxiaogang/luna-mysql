package luna.common.db;

import java.io.Serializable;
import java.util.Properties;

public class DataSourceConfig implements Serializable {
    private static final long serialVersionUID = 1264890831040718463L;

    private String            username;
    private String            password;
    private String            url;
    private String            driver;
    private String            encode;
    private Properties        properties       = new Properties();

    public DataSourceConfig(String url, String username, String password, String driver, Properties properties){
        this.username = username;
        this.password = password;
        this.url = url;
        this.driver = driver;
        this.properties = properties;
    }

    public DataSourceConfig(String url, String username, String password, String driver, String encode,
                            Properties properties){
        this.username = username;
        this.password = password;
        this.url = url;
        this.driver = driver;
        this.encode = encode;
        this.properties = properties;
    }

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

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getEncode() {
        return encode;
    }

    public void setEncode(String encode) {
        this.encode = encode;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}

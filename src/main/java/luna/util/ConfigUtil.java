package luna.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

/**
 * 
* Copyright: Copyright (c) 2017 XueErSi
*
* @version v1.0.0
* @author GaoXing Chen
*
* Modification History:
* Date         Author          Version			Description
*---------------------------------------------------------*
* 2017年8月21日     GaoXing Chen      v1.0.0		  添加注释
 */
public class ConfigUtil {
    private static final String HTTP = "http://";
    private static final String HTTPS = "https://";

    public static Map parse(String filename) throws Exception {
        Yaml yaml = new Yaml();
        InputStream is;
        if (filename.startsWith(ConfigUtil.HTTP) || filename.startsWith(ConfigUtil.HTTPS)) {
            URL httpUrl;
            URLConnection connection;
            httpUrl = new URL(filename);
            connection = httpUrl.openConnection();
            connection.connect();
            is = connection.getInputStream();
        } else {
            is = new FileInputStream(new File(filename));
        }
        return (Map) yaml.load(is);
    }
    
}

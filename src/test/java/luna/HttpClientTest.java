package luna;

import luna.util.HttpClientUtil;
import org.apache.http.HttpEntity;
import org.apache.http.entity.StringEntity;

import java.util.HashMap;
import java.util.Map;

//对接口进行测试
public class HttpClientTest {
    private String url = "https://oapi.dingtalk.com/robot/send?access_token=043776d6a31c02ceb8c3519c15ffe447dd5f91702c158229ab419dc8705cfbd9";
    private String charset = "utf-8";
    private HttpClientUtil httpClientUtil = null;

    public HttpClientTest(){
        httpClientUtil = new HttpClientUtil();
    }

    public void test(){
        String httpOrgCreateTest = url;
        String json ="{\n" +
                "    \"msgtype\": \"text\", \n" +
                "    \"text\": {\n" +
                "        \"content\": \"我就是我, 是不一样的烟火\"\n" +
                "    }, \n" +
                "    \"at\": {\n" +
                "        \"atMobiles\": [\n" +
                "            \"18321787920\", \n" +
                "        ], \n" +
                "        \"isAtAll\": false\n" +
                "    }\n" +
                "}";
        HttpEntity entity = new StringEntity(json,charset);
        Map<String,String> header = new HashMap<>();
        header.put("Content-Type","application/json");
        String httpOrgCreateTestRtn =null;
        try {
            httpOrgCreateTestRtn = httpClientUtil.post(httpOrgCreateTest, header, null, entity);
        }catch(Exception e){
            e.printStackTrace();
        }
        System.out.println(httpOrgCreateTestRtn);
    }

    public static void main(String[] args){
        HttpClientTest test = new HttpClientTest();
        test.test();
    }
}

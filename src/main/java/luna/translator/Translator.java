package luna.translator;

import java.util.Map;

public interface Translator {
    public void translate(Map<String,Object> record)throws Exception;
}

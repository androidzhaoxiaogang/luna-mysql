package luna.translator;

import java.util.List;
import java.util.Map;

public interface Translator {
    void translate(final List<Map<String,Object>> records);
    void translateOneByOne(Map<String, Object> payload);
}

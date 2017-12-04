package luna.common.context;

import luna.util.StringStyle;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

public class KafkaContext implements Serializable{
    private static final long serialVersionUID = -3895503342199392437L;

    private List<String>    topics;
    private Properties      props;
    private int             retryTimes;
    private int             retryInterval;

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public Properties getProps() {
        return props;
    }

    public void setProps(Properties props) {
        this.props = props;
    }

    public String toString(){
        return ToStringBuilder.reflectionToString(this, StringStyle.DEFAULT_STYLE);
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public int getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
    }
}

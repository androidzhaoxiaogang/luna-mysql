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
    //private Logger          log;
    private int             numConsumers;

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

//    public Logger getLog() {
//        return log;
//    }
//
//    public void setLog(Logger log) {
//        this.log = log;
//    }

    public int getNumConsumers() {
        return numConsumers;
    }

    public void setNumConsumers(int numConsumers) {
        this.numConsumers = numConsumers;
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
}

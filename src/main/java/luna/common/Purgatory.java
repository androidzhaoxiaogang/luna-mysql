package luna.common;

import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class Purgatory implements Delayed{
    private List<String> topics;
    private long delay;
    private TimeUnit timeUnit;

    public Purgatory(List<String> topics, long delay, TimeUnit timeUnit){
        this.delay=delay;
        this.topics=topics;
        this.timeUnit=timeUnit;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(timeUnit.toSeconds(delay-System.currentTimeMillis()),TimeUnit.SECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        if(delay < ((Purgatory)o).delay) return -1;
        else if(delay > ((Purgatory)o).delay) return 1;
        return 0;
    }

    public List<String> getTopics() {
        return topics;
    }
}

package luna.common;

public interface LifeCycle {
    public void start();

    public void stop();

    public void abort(String why, Throwable e);

    public boolean isStart();

    public boolean isStop();
}

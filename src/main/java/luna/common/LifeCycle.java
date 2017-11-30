package luna.common;

public interface LifeCycle {
    void start();

    void stop();

    void abort(String why, Throwable e);

    boolean isStart();

    boolean isStop();
}

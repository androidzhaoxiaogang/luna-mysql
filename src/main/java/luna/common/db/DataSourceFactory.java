package luna.common.db;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.cache.*;
import luna.exception.LunaException;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class DataSourceFactory {
    private int                                         maxWait     = 5 * 1000;
    private int                                         minIdle     = 0;
    private int                                         initialSize = 0;
    private int                                         maxActive   = 32;
    private LoadingCache<DataSourceConfig,DataSource>   dataSources;

    public void start() {
        int cacheSize = 100;
        int duration = 10;//minute

        dataSources = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterAccess(duration, TimeUnit.MINUTES)
                .removalListener(new RemovalListener<DataSourceConfig,DataSource>(){
                    public void onRemoval(RemovalNotification<DataSourceConfig,DataSource> removal){
                        DruidDataSource basicDataSource=(DruidDataSource)removal.getValue();
                        basicDataSource.close();
                    }
                })
                .build(new CacheLoader<DataSourceConfig,DataSource>() {
                            public DataSource load(DataSourceConfig config){
                                return createDataSource(config.getUrl(),
                                        config.getUsername(),
                                        config.getPassword(),
                                        config.getDriver(),
                                        config.getProperties());
                            }
                        });

    }


    public void stop() {
        dataSources.invalidateAll();
    }

    public DataSource getDataSource(DataSourceConfig config) {
        return dataSources.getUnchecked(config);
    }

    public DataSource getDataSource(String url, String userName, String password, String driver, Properties props) {
        return getDataSource(new DataSourceConfig(url, userName, password, driver, props));
    }

    private DataSource createDataSource(String url, String userName, String password, String driver, Properties props) {
        try {
            int maxActive = Integer.valueOf(props.getProperty("maxActive", String.valueOf(this.maxActive)));
            if (maxActive < 0) {
                maxActive = 200;
            }
            DruidDataSource dataSource = new DruidDataSource();
            dataSource.setUrl(url);
            dataSource.setUsername(userName);
            dataSource.setPassword(password);
            dataSource.setUseUnfairLock(true);
            dataSource.setNotFullTimeoutRetryCount(2);
            dataSource.setInitialSize(initialSize);
            dataSource.setMinIdle(minIdle);
            dataSource.setMaxActive(maxActive);
            dataSource.setMaxWait(maxWait);
            dataSource.setDriverClassName(driver);
            // 动态的参数
            if (props != null && props.size() > 0) {
                for (Map.Entry<Object, Object> entry : props.entrySet()) {
                    dataSource.addConnectionProperty(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
                }
            }

            dataSource.addConnectionProperty("useServerPrepStmts", "false");
            dataSource.addConnectionProperty("rewriteBatchedStatements", "true");
            dataSource.addConnectionProperty("allowMultiQueries", "true");
            dataSource.addConnectionProperty("readOnlyPropagatesToServer", "false");
            dataSource.setValidationQuery("select 1");
            dataSource.setExceptionSorter("com.alibaba.druid.pool.vendor.MySqlExceptionSorter");
            dataSource.setValidConnectionCheckerClassName("com.alibaba.druid.pool.vendor.MySqlValidConnectionChecker");
            return dataSource;
        } catch (Throwable e) {
             throw new LunaException(e);
        }
    }

    public void setMaxWait(int maxWait) {
        this.maxWait = maxWait;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

    public void setInitialSize(int initialSize) {
        this.initialSize = initialSize;
    }

    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }
}

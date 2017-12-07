package luna.common;

import luna.common.context.KafkaContext;
import luna.common.context.MysqlContext;
import luna.common.db.DataSourceConfig;
import luna.common.db.DataSourceFactory;
import luna.common.model.meta.TableMeta;
import luna.common.model.SchemaTable;
import luna.common.db.TableMetaGenerator;
import luna.exception.LunaException;
import luna.translator.KafkaRecordTranslator;
import luna.applier.MysqlApplier;
import luna.util.ConfigUtil;
import luna.extractor.KafkaExtractor;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Bootstrap extends AbstractLifeCycle{
    private KafkaExtractor          kafkaExtractor;
    private KafkaRecordTranslator   kafkaRecordTranslator;
    private MysqlApplier            mysqlApplier;
    private KafkaContext            kafkaContext = new KafkaContext();
    private MysqlContext            mysqlContext = new MysqlContext();
    private final Map               inputConfigs;
    private final Map               mysqlConfigs;
    private DataSourceFactory       dataSourceFactory = new DataSourceFactory();


    public Bootstrap(String configFile){
        Map configs=null;
        try {
            configs= ConfigUtil.parse(configFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
        inputConfigs = (Map)configs.get("NewKafka");
        mysqlConfigs = (Map)configs.get("Mysql");
    }

    public void start(){
        super.start();
        dataSourceFactory.start();
        initKafkaContext();
        initMysqlContext();
        mysqlApplier = new MysqlApplier(mysqlContext,dataSourceFactory);
        mysqlApplier.start();
        kafkaRecordTranslator = new KafkaRecordTranslator(mysqlContext,mysqlApplier);
        kafkaRecordTranslator.start();
        kafkaExtractor = new KafkaExtractor(kafkaContext,kafkaRecordTranslator);
        kafkaExtractor.start();
        kafkaExtractor.extract();
        logger.info("Bootstrap is started!");
    }

    public void stop(){
        logger.info("Bootstrap is stopped!");
        kafkaRecordTranslator.stop();
        mysqlApplier.stop();
        kafkaExtractor.stop();
        dataSourceFactory.stop();
        super.stop();
    }

    private void initKafkaContext(){
        String groupId = (String)inputConfigs.get("group.id");
        List<String> topics=(List<String>) inputConfigs.get("topics");
        String maxFetchByte = ""+inputConfigs.get("max.fetch.byte");
        int maxPollRecords=(int)inputConfigs.get("max.poll.records");
        int retryTimes =(int)inputConfigs.get("retry.times");
        int retryInterval = (int)inputConfigs.get("retry.interval");
        int purgeInterval = (int)inputConfigs.get("purge.interval");

        Properties props = new Properties();
        props.put("bootstrap.servers", inputConfigs.get("bootstrap.servers"));
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("max.partition.fetch.bytes",maxFetchByte);
        props.put("max.poll.records",maxPollRecords);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("enable.auto.commit", "false");

        kafkaContext.setRetryTimes(retryTimes);
        kafkaContext.setRetryInterval(retryInterval);
        kafkaContext.setProps(props);
        kafkaContext.setTopics(topics);
        kafkaContext.setPurgeInterval(purgeInterval);
        logger.info("KafkaContext has inited!");
    }

    private void initMysqlContext(){
        String driver = "com.mysql.jdbc.Driver";
        String sourceUrl = (String)mysqlConfigs.get("source.url");
        String sourcePoolSize = (String)mysqlConfigs.get("source.poolSize");
        String sourceEncode = (String)mysqlConfigs.get("source.encode");
        String sourceUsername = (String)mysqlConfigs.get("source.username");
        String sourcePassword = (String)mysqlConfigs.get("source.password");

        mysqlContext.setSourceDsConfig(initDataSourceConfig(sourceUrl,sourceUsername,sourcePassword,sourceEncode,sourcePoolSize,driver));

        List<Map> splitTableDetail=(List<Map>)mysqlConfigs.get("split.table.detail");
        for (Map map : splitTableDetail) {
            String schema = (String)map.get("schema");
            String table = (String)map.get("table");
            SchemaTable schemaTable = new SchemaTable(schema,table);
            String splitColumn = (String)map.get("split.column");
            int splitNum = (int)map.get("split.num");
            if(!isBlob(splitNum)){
                throw new LunaException("ERROR: Split number is not a BLOB");
            }
            mysqlContext.addSourceTable(schemaTable);
            TableMeta tableMeta = TableMetaGenerator.buildColumns(dataSourceFactory.getDataSource(mysqlContext.getSourceDsConfig()),schema,table);
            tableMeta.setExtKey(splitColumn);
            tableMeta.setExtNum(splitNum);
            mysqlContext.putTableMeta(schemaTable,tableMeta);
            List<Map> targets = (List<Map>)map.get("target");
            if(splitNum!=targets.size()){
                throw new LunaException("Target database is not equal split numbers!");
            }
            for(int i=0;i<targets.size();i++){
                String url = (String)targets.get(i).get("url");
                String poolSize = (String)targets.get(i).get("poolSize");
                String encode = (String)targets.get(i).get("encode");
                String username = (String)targets.get(i).get("username");
                String password = (String)targets.get(i).get("password");
                String targetSchema = schema+i;
                String targetTable = table;

                DataSourceConfig dsConfig = initDataSourceConfig(url,username,password,encode,poolSize,driver);
                dataSourceFactory.loading(dsConfig);
                mysqlContext.putTargetDsConfig(new SchemaTable(targetSchema,targetTable),dsConfig);
            }

        }
        logger.info("MysqlContext has inited!");
        logger.info("Target table meta: "+mysqlContext.getTableMetas());
        logger.info("Source tables: "+mysqlContext.getSourceTables());
        logger.info("Target datasource: "+mysqlContext.getTargetDsConfigs());
        logger.info("Source datasource: "+mysqlContext.getSourceDsConfig());
        dataSourceFactory.invalidate(mysqlContext.getSourceDsConfig());
    }

    private DataSourceConfig initDataSourceConfig(String url, String username, String password, String encode, String poolSize, String driver){
        Properties properties = new Properties();
        if (poolSize != null) {
            properties.setProperty("maxActive", poolSize);
        } else {
            properties.setProperty("maxActive", "200");
        }

        properties.setProperty("characterEncoding", encode);
        return new DataSourceConfig(url, username, password, driver, properties);
        //return dataSourceFactory.getDataSource(dsConfig);
    }


    private boolean isBlob(int num){
        int i=1;
        while(i<num){
            i=i*2;
        }
        if(i==num){
            return true;
        }
        return false;
    }
}

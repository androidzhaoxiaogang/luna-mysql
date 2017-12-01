package luna.extractor;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import luna.common.*;
import luna.common.context.KafkaContext;
import luna.translator.KafkaRecordTranslator;

import com.google.common.collect.Lists;
import luna.util.DingDingMsgUtil;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.BasicConfigurator;
import org.json.simple.JSONValue;

/**
 *
 * Copyright: Copyright (c) 2017 XueErSi
 *
 * @version v1.0.0
 * @author GaoXing Chen
 *
 * Modification History:
 * Date         Author          Version            Description
 *---------------------------------------------------------*
 * 2017年8月21日     GaoXing Chen      v1.0.0               添加注释
 */
public class KafkaExtractor extends AbstractLifeCycle implements Extractor{
    private KafkaContext            kafkaContext;
    private ExecutorService         executor;
    private List<ConsumerLoop>      consumers = Lists.newArrayList();
    private KafkaRecordTranslator   kafkaRecordTranslator;

    public KafkaExtractor(KafkaContext kafkaContext, KafkaRecordTranslator kafkaRecordTranslator) {
        this.kafkaContext=kafkaContext;
        this.kafkaRecordTranslator=kafkaRecordTranslator;
    }

    public void start() {
        super.start();
        System.setProperty("java.security.auth.login.config", "conf/kafka_client_jaas.conf");
        BasicConfigurator.configure();
    }

    public void stop() {
        super.stop();
        consumers.forEach(consumerThread -> consumerThread.shutdown());
        executor.shutdown();
        logger.info("All consumer is shutdown!");
        try {
            executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error(e);
        }
    }

    public void extract() {
        executor = Executors.newFixedThreadPool(kafkaContext.getNumConsumers());
        int topicNum = kafkaContext.getTopics().size();
        logger.info("thread.num: "+kafkaContext.getNumConsumers()+" and topic.num: "+ topicNum);
        HashMap <Integer,ArrayList<String>> consumerTopics = new HashMap<>();

        for (int j=0;j<topicNum;j++){
            int thread = j%kafkaContext.getNumConsumers();
            if(consumerTopics.containsKey(thread)){
                consumerTopics.get(thread).add(kafkaContext.getTopics().get(j));
            }else{
                ArrayList<String>topicLists = new ArrayList<>();
                topicLists.add(kafkaContext.getTopics().get(j));
                consumerTopics.put(thread,topicLists);
            }
        }

        consumerTopics.forEach((key,topicLists)->{
            ConsumerLoop consumer = new ConsumerLoop(kafkaContext.getProps(),topicLists);
            consumers.add(consumer);
            executor.submit(consumer);
        });
    }

    public class ConsumerLoop implements Runnable {
        private AtomicBoolean running = new AtomicBoolean(true);
        private final KafkaConsumer<String, String> consumer;
        private final List<String> topics;

        public ConsumerLoop(Properties props, List<String> topics) {
            this.topics = topics;
            this.consumer = new KafkaConsumer<>(props);
        }

        public void run() {
            try {
                monitorRebalance();
                logger.info("Thread-"+Thread.currentThread().getId()+" Get kafka client!");
                ConsumerRecords<String, String> records;
                while (running.get()) {
                    records = consumer.poll(Long.MAX_VALUE);
                    for (ConsumerRecord<String, String> consumerRecord : records) {
                        try {
                            logger.info(consumerRecord);
                            Map<String, Object> payload = (Map<String, Object>) JSONValue.parseWithException(consumerRecord.value());
                            kafkaRecordTranslator.translate(payload);
                            consumer.commitSync();
                        }catch (Throwable e){
                            DingDingMsgUtil.sendMsg(e.getLocalizedMessage());
                            logger.error(e.getLocalizedMessage());
                            shutdown();
                        }
                    }
                }
            } catch (WakeupException e) {
                // ignore for shutdown
            } finally {
                consumer.close();
                logger.info("Consumer Thread "+ Thread.currentThread().getId() + "is closed!");
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }

        private void monitorRebalance(){
            consumer.subscribe(topics,new ConsumerRebalanceListener() {
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                }

                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    partitions.forEach(partition -> {
                        logger.info("Rebalance happened " + partition.topic() + ":" + partition.partition());
                    });
                }
            });
        }
    }
}

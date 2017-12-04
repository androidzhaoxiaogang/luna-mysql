package luna.extractor;

import java.util.*;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import luna.common.*;
import luna.common.context.KafkaContext;
import luna.translator.KafkaRecordTranslator;

import com.google.common.collect.Lists;
import luna.util.DingDingMsgUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
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
    private DelayQueue<Purgatory>   delayQueue = new DelayQueue<>();

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
            errorLog.error(ExceptionUtils.getFullStackTrace(e));
        }
    }

    public void extract() {
        int topicNum = kafkaContext.getTopics().size();
        executor = Executors.newFixedThreadPool(topicNum);
        for (String topic:kafkaContext.getTopics()){
            List<String> topicList = Lists.newArrayList();
            topicList.add(topic);
            ConsumerLoop consumer = new ConsumerLoop(kafkaContext.getProps(),topicList);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Thread purge = new Thread(new Purge());
        purge.start();
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
                            //增加重试
                            for(int i=0; i<kafkaContext.getRetryTimes(); i++){
                                try{
                                    logger.info(consumerRecord);
                                    Map<String, Object> payload = (Map<String, Object>) JSONValue.parseWithException(consumerRecord.value());
                                    kafkaRecordTranslator.translate(payload);
                                    consumer.commitSync();
                                    //正常break
                                    break;
                                }catch (Throwable e){
                                    if(processError(e,i)){
                                        //InterruptedException或者重试次数满了: break
                                        throw e;
                                    }
                                }
                            }
                        }catch (Throwable e){
                            DingDingMsgUtil.sendMsg(ExceptionUtils.getFullStackTrace(e));
                            errorLog.error("ERROR can not handle by retry: "+ExceptionUtils.getFullStackTrace(e));
                            shutdown();
                        }
                    }
                }
            } catch (WakeupException e) {
                // ignore for shutdown
            } finally {
                consumer.close();
                errorLog.error("Consumer Thread "+ Thread.currentThread().getId() + "is closed!");
                purge(topics);
                errorLog.error("Put this topic into purgatory.");
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }

        private void purge(List<String> topics){
            long now = System.currentTimeMillis();
            long delay = now + TimeUnit.MINUTES.toMillis(kafkaContext.getPurgeInterval());
            Purgatory purgatory = new Purgatory(topics,delay,TimeUnit.MILLISECONDS);
            delayQueue.offer(purgatory);
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

        private boolean processError(Throwable e, int i){
            if (!(ExceptionUtils.getRootCause(e) instanceof InterruptedException)) {
                errorLog.error("retry {} ,something error happened. caused by {}",
                        (i + 1),
                        ExceptionUtils.getFullStackTrace(e));
                try {
                    DingDingMsgUtil.sendMsg("retry "+(i+1)+",something error happened. caused by "+ExceptionUtils.getFullStackTrace(e));
                } catch (Throwable e1) {
                    logger.error("send DingDing alarm failed. ", e1);
                }

                try {
                    Thread.sleep(kafkaContext.getRetryInterval());
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    return true;
                }
                if(i==kafkaContext.getRetryTimes()-1){
                    return true;
                }
            } else {
                // interrupt事件，响应退出
                return true;
            }

            return false;
        }
    }

    public class Purge implements Runnable{
        @Override
        public void run() {
            while (delayQueue!=null){
                try{
                    Purgatory purgatory=delayQueue.take();
                    ConsumerLoop consumer = new ConsumerLoop(kafkaContext.getProps(),purgatory.getTopics());
                    consumers.add(consumer);
                    executor.submit(consumer);
                }catch (InterruptedException e){
                    errorLog.error(ExceptionUtils.getFullStackTrace(e));
                }
            }
        }
    }
}

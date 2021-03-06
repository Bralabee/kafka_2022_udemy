package io.conduktor.demos.kafka.wikimedia;


import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    KafkaProducer<String, String> kafkaProducer;
    String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());


    public WikimediaChangeHandler(KafkaProducer<String, String> KafkaProducer, String topic){
        this.kafkaProducer =  KafkaProducer;
        this.topic = topic;

    }


    @Override
    public void onOpen() {
        // notthing required when stream is open

    }

    @Override
    public void onClosed()  { // no exception required.
        kafkaProducer.close();

    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        // Use asynchronous
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));

    }

    @Override
    public void onComment(String comment)  {
        // nothing required

    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in Raading Stream", t);

    }
}

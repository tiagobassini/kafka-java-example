package org.tbassini.kafka.consumer.events;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;


@Slf4j
public class EventConsumer {

    private final Consumer<String, String> consumer;

    public EventConsumer() {
        this.consumer = createConsumer();
    }

    private Consumer<String, String> createConsumer(){

        if(consumer !=null){
            return consumer;
        }

        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id","default");

        return new KafkaConsumer<String, String>(properties);
    }

    public void execute() {

        List<String> topicos = new ArrayList<>();

        topicos.add("EventRegistry");

        consumer.subscribe(topicos);

        log.info("Iniciando consumer...");

        boolean  goAhead = true;
        while(goAhead){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : records){
                writeMesage(record.topic(), record.partition() ,record.value());
                if(record.value().contains("FECHAR")){
                    goAhead = false;
                }
            }
        }
        consumer.close();
    }

    private void writeMesage(String topic, int partition, String mesage){
        log.info("Topico: {}, Partição: {}, Mensagem: {}", topic, partition, mesage);
    }

}

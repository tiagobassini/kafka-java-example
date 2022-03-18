package org.tbassini.kafka.producer.events;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

@Slf4j
public class EventProducer {


    private final  Producer<String, String> producer;

    public EventProducer() {
        this.producer = createProducer();
    }

    private Producer<String, String> createProducer(){

        if(producer !=null){
            return producer;
        }

        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("serializer.class","kafka.serializer.DefaultEncoder");

        return new KafkaProducer<String, String>(properties);
    }

    public void execute(){

        String key = UUID.randomUUID().toString();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String mesage = sdf.format(new Date());
        mesage += "| " + key;
        mesage += " | Nova Mensagem";
        //mesage += " | FECHAR";

        log.info("Iniciando envio da mensagem");
        ProducerRecord<String,String> record = new ProducerRecord<String, String>( "EventRegistry", key, mesage );
        producer.send(record);
        producer.flush();
        producer.close();

        log.info("Mensagem enviada com sucesso [{}]", mesage);
    }
}

package org.tbassini.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.tbassini.kafka.producer.events.EventProducer;

@Slf4j
public class ApplicationProducer {


    public static void main(String[] args) {

        ApplicationProducer application = new ApplicationProducer();
        application.start();

    }

    private void start(){
        log.info("Iniciando a aplicação");
        EventProducer producer = new EventProducer();
        producer.execute();

    }
}

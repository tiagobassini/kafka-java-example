package org.tbassini.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.tbassini.kafka.consumer.events.EventConsumer;


@Slf4j
public class ApplicationConsumer {

    public static void main(String[] args) {

        ApplicationConsumer application = new ApplicationConsumer();
        application.start();

    }

    private void start(){
        log.info("Iniciando a aplicação");
        EventConsumer consumer = new EventConsumer();
        consumer.execute();

    }
}

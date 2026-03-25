package com.saga.orchestrator.config;

import com.saga.shared.kafka.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares all Kafka topics used across the saga.
 * Topics are auto-created on application startup if they don't exist.
 */
@Configuration
public class KafkaConfig {

    // ─── Order ───────────────────────────────────────────────────────────────
    @Bean public NewTopic orderCreated()   { return topic(KafkaTopics.ORDER_CREATED); }
    @Bean public NewTopic orderConfirmed() { return topic(KafkaTopics.ORDER_CONFIRMED); }
    @Bean public NewTopic orderCancelled() { return topic(KafkaTopics.ORDER_CANCELLED); }

    // ─── Stock ───────────────────────────────────────────────────────────────
    @Bean public NewTopic stockReserveRequest()    { return topic(KafkaTopics.STOCK_RESERVE_REQUEST); }
    @Bean public NewTopic stockReserved()          { return topic(KafkaTopics.STOCK_RESERVED); }
    @Bean public NewTopic stockReservationFailed() { return topic(KafkaTopics.STOCK_RESERVATION_FAILED); }
    @Bean public NewTopic stockReleaseRequest()    { return topic(KafkaTopics.STOCK_RELEASE_REQUEST); }
    @Bean public NewTopic stockReleased()          { return topic(KafkaTopics.STOCK_RELEASED); }

    // ─── Payment ─────────────────────────────────────────────────────────────
    @Bean public NewTopic paymentProcessRequest() { return topic(KafkaTopics.PAYMENT_PROCESS_REQUEST); }
    @Bean public NewTopic paymentProcessed()      { return topic(KafkaTopics.PAYMENT_PROCESSED); }
    @Bean public NewTopic paymentFailed()         { return topic(KafkaTopics.PAYMENT_FAILED); }
    @Bean public NewTopic paymentCancelRequest()  { return topic(KafkaTopics.PAYMENT_CANCEL_REQUEST); }
    @Bean public NewTopic paymentCancelled()      { return topic(KafkaTopics.PAYMENT_CANCELLED); }

    private NewTopic topic(String name) {
        return TopicBuilder.name(name).partitions(1).replicas(1).build();
    }
}

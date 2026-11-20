package com.ticketflow.messaging;

import com.ticketflow.messaging.config.TicketFlowKafkaProperties;
import com.ticketflow.messaging.consumer.ReservationRequestedListener;
import com.ticketflow.support.IntegrationTest;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@IntegrationTest
class KafkaIntegrationTest {

    @Autowired
    KafkaAdmin kafkaAdmin;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Autowired
    TicketFlowKafkaProperties properties;

    @Autowired
    KafkaListenerEndpointRegistry listenerRegistry;

    @Test
    @DisplayName("기동 시 예약 토픽과 DLT가 설정한 파티션 수로 생성된다")
    void reservationTopicsAreCreatedOnStartup() {
        Map<String, TopicDescription> topics = kafkaAdmin.describeTopics(
                properties.reservationRequestedTopic(),
                properties.reservationRequestedDltTopic());

        assertThat(topics).containsOnlyKeys(
                "reservation.requested.v1",
                "reservation.requested.v1-dlt");
        assertThat(topics.values())
                .allSatisfy(topic -> assertThat(topic.partitions()).hasSize(properties.partitions()));
    }

    @Test
    @DisplayName("예약 Consumer는 설정한 group id(ticketflow-reservation)로 예약 토픽을 구독한다")
    void reservationListenerUsesConfiguredGroupId() {
        MessageListenerContainer container =
                listenerRegistry.getListenerContainer(ReservationRequestedListener.LISTENER_ID);

        assertThat(container).isNotNull();
        assertThat(container.getGroupId()).isEqualTo("ticketflow-reservation");
        assertThat(container.getContainerProperties().getTopics()).containsExactly("reservation.requested.v1");
    }

    @Test
    @DisplayName("Producer로 보낸 메시지를 Consumer가 같은 key·value로 받는다")
    void producerConsumerRoundTrip() throws Exception {
        String topic = "ticketflow.test.round-trip";
        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(topic).partitions(1).replicas(1).build());

        String key = "7";
        String value = """
                {"messageId":"6f1c2a9e-0b7d-4d7e-9a51-3c2f0d8e4b11","eventId":7}""";

        try (Consumer<String, String> consumer = consumerFactory.createConsumer("round-trip-test", "it")) {
            consumer.subscribe(List.of(topic));
            kafkaTemplate.send(topic, key, value).get(10, TimeUnit.SECONDS);

            ConsumerRecord<String, String> record =
                    KafkaTestUtils.getSingleRecord(consumer, topic, Duration.ofSeconds(20));

            assertThat(record.key()).isEqualTo(key);
            assertThat(record.value()).isEqualTo(value);
        }
    }
}

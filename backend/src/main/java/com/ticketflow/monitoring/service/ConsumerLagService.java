package com.ticketflow.monitoring.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Consumer lag = 파티션의 마지막 오프셋 - group이 커밋한 오프셋 (PRD M6).
 * AdminClient로 직접 계산하므로 Consumer가 멈춰 있어도 쌓인 양을 볼 수 있다.
 */
@Service
public class ConsumerLagService implements DisposableBean {

    private static final long TIMEOUT_SECONDS = 3;

    private final KafkaAdmin kafkaAdmin;
    private volatile AdminClient adminClient;

    public ConsumerLagService(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    /**
     * @return 파티션 번호 → lag. 커밋 기록이 없는 파티션은 처음부터 계산한다.
     */
    public Map<Integer, Long> lagByPartition(String groupId, String topic) throws Exception {
        AdminClient client = client();
        TopicDescription description = client.describeTopics(List.of(topic)).allTopicNames()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS).get(topic);
        List<TopicPartition> partitions = description.partitions().stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .toList();

        Map<TopicPartition, OffsetAndMetadata> committed = client.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Map<TopicPartition, ListOffsetsResultInfo> ends = client.listOffsets(partitions.stream()
                        .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest())))
                .all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        Map<Integer, Long> lag = new TreeMap<>();
        for (TopicPartition partition : partitions) {
            OffsetAndMetadata offset = committed.get(partition);
            long end = ends.get(partition).offset();
            lag.put(partition.partition(), Math.max(0, end - (offset == null ? 0 : offset.offset())));
        }
        return lag;
    }

    private AdminClient client() {
        AdminClient client = adminClient;
        if (client == null) {
            synchronized (this) {
                if (adminClient == null) {
                    adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
                }
                client = adminClient;
            }
        }
        return client;
    }

    @Override
    public void destroy() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}

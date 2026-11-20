package com.ticketflow;

import com.ticketflow.support.TestcontainersConfiguration;
import org.springframework.boot.SpringApplication;

/**
 * Compose 없이 Testcontainers로 Kafka·PostgreSQL을 띄워 애플리케이션을 실행한다.
 * {@code ./gradlew bootTestRun}
 */
public class TestTicketFlowApplication {

    public static void main(String[] args) {
        SpringApplication.from(TicketFlowApplication::main).with(TestcontainersConfiguration.class).run(args);
    }
}

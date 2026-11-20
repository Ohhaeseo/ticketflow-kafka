package com.ticketflow.global.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class OpenApiConfig {

    @Bean
    public OpenAPI ticketFlowOpenApi() {
        return new OpenAPI().info(new Info()
                .title("TicketFlow Kafka API")
                .version("v1")
                .description("Kafka 기반 비동기 티켓 예약 서버. 예약 접수(202)는 좌석 확정이 아니라 접수 완료를 뜻한다."));
    }
}

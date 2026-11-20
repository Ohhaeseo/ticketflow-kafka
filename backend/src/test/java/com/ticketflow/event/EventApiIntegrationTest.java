package com.ticketflow.event;

import com.ticketflow.support.IntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@IntegrationTest
class EventApiIntegrationTest {

    private static final String SALES_START = "2026-11-23T10:00:00Z";
    private static final String SALES_END = "2026-12-31T23:59:59Z";

    @Autowired
    WebApplicationContext context;

    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    @DisplayName("100장 공연을 등록하면 잔여 수량이 100이고, 상세 조회에서도 같다")
    void createEventWith100Tickets() throws Exception {
        String location = mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(eventJson("인기 공연", 100, SALES_START, SALES_END)))
                .andExpect(status().isCreated())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.title").value("인기 공연"))
                .andExpect(jsonPath("$.totalTickets").value(100))
                .andExpect(jsonPath("$.remainingTickets").value(100))
                .andExpect(jsonPath("$.salesStartAt").value(SALES_START))
                .andReturn().getResponse().getHeader("Location");

        mockMvc.perform(get(location))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalTickets").value(100))
                .andExpect(jsonPath("$.remainingTickets").value(100))
                .andExpect(jsonPath("$.salesEndAt").value(SALES_END));
    }

    @Test
    @DisplayName("공연 목록에 등록한 공연이 나타난다")
    void listEvents() throws Exception {
        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(eventJson("목록 확인용 공연", 10, SALES_START, SALES_END)))
                .andExpect(status().isCreated());

        mockMvc.perform(get("/api/v1/events"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[*].title", hasItem("목록 확인용 공연")));
    }

    @Test
    @DisplayName("없는 공연을 조회하면 404 EVENT_NOT_FOUND")
    void eventNotFound() throws Exception {
        mockMvc.perform(get("/api/v1/events/{id}", 999_999))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("EVENT_NOT_FOUND"));
    }

    @Test
    @DisplayName("필수값이 비었거나 수량이 0이면 400과 필드별 오류를 반환한다")
    void invalidRequest() throws Exception {
        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"title\":\" \",\"totalTickets\":0}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"))
                .andExpect(jsonPath("$.errors[*].field",
                        containsInAnyOrder("title", "totalTickets", "salesStartAt", "salesEndAt")));
    }

    @Test
    @DisplayName("판매 시작이 종료보다 늦으면 400 INVALID_SALES_PERIOD")
    void invalidSalesPeriod() throws Exception {
        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(eventJson("기간 오류 공연", 100, SALES_END, SALES_START)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_SALES_PERIOD"));
    }

    @Test
    @DisplayName("JSON 형식이 깨지면 400 INVALID_REQUEST")
    void malformedJson() throws Exception {
        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"title\":"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
    }

    private static String eventJson(String title, int totalTickets, String salesStartAt, String salesEndAt) {
        return """
                {"title":"%s","totalTickets":%d,"salesStartAt":"%s","salesEndAt":"%s"}
                """.formatted(title, totalTickets, salesStartAt, salesEndAt);
    }
}

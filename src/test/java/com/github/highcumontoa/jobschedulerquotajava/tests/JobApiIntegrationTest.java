package com.github.highcumontoa.jobschedulerquotajava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * REST 入口集成测试：配额配置、提交、重复/超额拒绝、手动调度、取消、状态查询。
 * 验证所有外部入口与域服务给出一致结论，且拒绝原因可区分。
 */
@SpringBootTest(properties = {
        "scheduler.lease-ttl-ms=3000",
        "scheduler.max-attempts=3",
        "scheduler.local-executor.parallelism=4"
})
@AutoConfigureMockMvc
class JobApiIntegrationTest {

    @Autowired
    private MockMvc mvc;
    private final ObjectMapper json = new ObjectMapper();

    private String submitJson(String id, String subject, int cpu, int prio, String payload) {
        return """
                {"jobId":"%s","subject":"%s","demand":{"cpu":%d,"memoryMb":1,"instances":1},"priority":%d,"payload":"%s"}}
                """.formatted(id, subject, cpu, prio, payload);
    }

    @Test
    void fullFlowViaHttp_quotaDuplicateDispatchCancel() throws Exception {
        // 配置主体配额：2 cpu
        mvc.perform(put("/api/jobs/quotas/team-x")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"cpu":2,"memoryMb":10,"instances":10}
                                """))
                .andExpect(status().isOk());

        // 提交两个 1cpu 任务
        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content(submitJson("http-1", "team-x", 1, 5, "sleep:500")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("PENDING"));
        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content(submitJson("http-2", "team-x", 1, 9, "sleep:500")))
                .andExpect(status().isOk());

        // 重复提交 → 409 DUPLICATE_SUBMISSION
        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content(submitJson("http-1", "team-x", 1, 5, "x")))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("DUPLICATE_SUBMISSION"));

        // 超额提交（已在途 PENDING 不占额度，故先 tick 占满 2cpu，再提交）
        mvc.perform(post("/api/admin/tick")).andExpect(status().isOk());
        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content(submitJson("http-3", "team-x", 1, 5, "x")))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("QUOTA_EXCEEDED_ON_SUBMIT"));

        // 高优先级 http-2 已被下发（与 http-1 一起占满 2cpu）
        String body = mvc.perform(get("/api/jobs/http-2")).andReturn().getResponse().getContentAsString();
        JsonNode node = json.readTree(body);
        org.junit.jupiter.api.Assertions.assertEquals("RUNNING", node.get("state").asText());

        // 取消 http-1 → CANCELLED
        mvc.perform(post("/api/jobs/http-1/cancel"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("CANCELLED"));

        // 再次取消（终态迁移）→ 422 ILLEGAL_STATE_TRANSITION
        mvc.perform(post("/api/jobs/http-1/cancel"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("ILLEGAL_STATE_TRANSITION"));

        // 取消 http-2（执行者迟到的 sleep:500 回传将被拒或被接受，最终态唯一；这里验证取消被接受或已终结）
        mvc.perform(post("/api/jobs/http-2/cancel")).andExpect(
                org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isOk());

        // 不存在任务 → 404
        mvc.perform(get("/api/jobs/missing")).andExpect(status().isNotFound());
    }
}

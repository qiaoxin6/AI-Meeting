package com.hewei.hzyjy.xunzhi.interview.flow.session;

import cn.hutool.core.util.StrUtil;
import com.hewei.hzyjy.xunzhi.interview.api.io.req.DemeanorEvaluationReqDTO;
import com.hewei.hzyjy.xunzhi.interview.api.io.req.InterviewAnswerReqDTO;
import com.hewei.hzyjy.xunzhi.interview.api.io.req.InterviewQuestionReqDTO;
import com.hewei.hzyjy.xunzhi.interview.api.io.resp.InterviewAnswerRespDTO;
import com.hewei.hzyjy.xunzhi.interview.api.io.resp.InterviewQuestionRespDTO;
import com.hewei.hzyjy.xunzhi.interview.application.InterviewWorkflowService;
import com.hewei.hzyjy.xunzhi.interview.application.flow.InterviewFlowStateMachine;
import com.hewei.hzyjy.xunzhi.interview.application.runtime.InterviewSessionRuntimeRehydrateService;
import com.hewei.hzyjy.xunzhi.interview.application.runtime.InterviewRuntimeRehydrateScope;
import com.hewei.hzyjy.xunzhi.interview.flow.answer.InterviewAnswerPipeline;
import com.hewei.hzyjy.xunzhi.interview.flow.answer.InterviewQuestionLockService;
import com.hewei.hzyjy.xunzhi.interview.flow.demeanor.InterviewDemeanorService;
import com.hewei.hzyjy.xunzhi.interview.flow.extraction.InterviewQuestionExtractionService;
import com.hewei.hzyjy.xunzhi.interview.service.InterviewQuestionCacheService;
import com.hewei.hzyjy.xunzhi.interview.service.model.InterviewFlowState;
import com.hewei.hzyjy.xunzhi.interview.service.model.InterviewRuntimeLoadMode;
import com.hewei.hzyjy.xunzhi.interview.service.model.InterviewTurnLog;
import io.micrometer.core.instrument.Metrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 面试 Agent 编排服务：作为对外 API 的统一入口，协调题目提取、答案处理、流程状态管理、仪态评估等子系统。
 * 核心职责：获取当前题目（含 Redis 丢失后的三级恢复链）、答题流程编排、面试流程状态重建。
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class InterviewAgentOrchestrationService implements InterviewWorkflowService {

    private final InterviewQuestionCacheService interviewQuestionCacheService;
    private final InterviewQuestionExtractionService interviewQuestionExtractionService;
    private final InterviewDemeanorService interviewDemeanorService;
    private final InterviewAnswerPipeline interviewAnswerPipeline;
    private final InterviewFlowStateMachine interviewFlowStateMachine;
    private final InterviewQuestionLockService interviewQuestionLockService;
    private final InterviewSessionRuntimeRehydrateService runtimeRehydrateService;

    /**
     * 提取面试题目：从简历中解析并生成面试题目列表。
     * 直接委托给 InterviewQuestionExtractionService 处理。
     */
    public InterviewQuestionRespDTO extractInterviewQuestions(InterviewQuestionReqDTO reqDTO) {
        // 编排入口：提取链路直接委托 extraction flow。
        return interviewQuestionExtractionService.extractInterviewQuestions(reqDTO);
    }

    /**
     * 提交面试答案：统一走 AnswerPipeline 执行完整的答题链路。
     * 避免控制逻辑分散在多个 service 中，保持编排的单一入口。
     */
    public InterviewAnswerRespDTO answerInterviewQuestion(String sessionId, InterviewAnswerReqDTO requestParam) {
        // 编排入口：答题链路统一走 pipeline，避免控制逻辑分散在多个 service。
        return interviewAnswerPipeline.execute(sessionId, requestParam);
    }

    /**
     * 获取下一题：实际复用 getCurrentQuestion，返回当前流程游标指向的题目。
     * 注意：此方法不推进游标，只是获取当前题目。
     */
    public InterviewAnswerRespDTO getNextQuestion(String sessionId) {
        return getCurrentQuestion(sessionId);
    }

    /**
     * 获取当前题目（不推进流程游标）。
     *
     * 三级容灾恢复链：
     *   1) Redis 缓存直接命中 flow → 返回当前题号对应的题目
     *   2) flow 丢失时从最近对话轮次（turns）恢复题号 → 重建 flow 并推进到目标题
     *   3) turns 也为空 → 重新初始化 flow，回到第一题
     *
     * 典型场景：用户刷新页面、断线重连时需要知道当前面试进行到第几题。
     */
    public InterviewAnswerRespDTO getCurrentQuestion(String sessionId) {
        InterviewAnswerRespDTO response = InterviewAnswerRespDTO.init();

        if (StrUtil.isBlank(sessionId)) {
            return response.fail("sessionId cannot be empty");
        }

        try {
            runtimeRehydrateService.ensureRuntime(
                    sessionId,
                    InterviewRuntimeLoadMode.READ_WRITE_REQUIRED,
                    InterviewRuntimeRehydrateScope.FLOW_ONLY
            );
            // 1) 先保证题库可用（缓存 miss 时从 DB 回补）。
            Map<String, String> questions = getOrLoadQuestions(sessionId);
            if (questions == null || questions.isEmpty()) {
                return response.fail("interview questions not found");
            }

            InterviewFlowState flowState = interviewQuestionCacheService.getInterviewFlow(sessionId);
            if (flowState == null) {
                // 2) flow 丢失时按 turns 恢复；恢复失败再兜底重建 flow。
                CurrentQuestionState recoveredState = recoverCurrentQuestionFromTurns(sessionId, questions);
                if (recoveredState.finished) {
                    Metrics.counter("flow_restore_source_total", "source", "turn_finished").increment();
                    response.setTotalScore(interviewQuestionCacheService.getSessionTotalScore(sessionId));
                    return response.finish().success();
                }
                if (recoveredState.hasQuestion()) {
                    Metrics.counter("flow_restore_source_total", "source", "turn_recovered").increment();
                    flowState = restoreFlowToQuestion(sessionId, recoveredState.questionNumber, questions.size());
                    fillCurrentQuestionResponse(sessionId, response,
                            recoveredState.questionNumber, recoveredState.questionContent, flowState);
                    return response.success();
                }

                Metrics.counter("flow_restore_source_total", "source", "flow_reinit").increment();
                interviewQuestionCacheService.initInterviewFlow(sessionId, questions.size());
                flowState = interviewQuestionCacheService.getInterviewFlow(sessionId);
            } else {
                Metrics.counter("flow_restore_source_total", "source", "flow_cache").increment();
            }

            if (flowState == null) {
                return response.fail("interview flow not initialized");
            }
            if (flowState.isCompleted() || interviewFlowStateMachine.isOutOfRange(flowState)) {
                response.setTotalScore(interviewQuestionCacheService.getSessionTotalScore(sessionId));
                return response.finish().success();
            }

            // 3) 在当前 flow 上解析题号并回填给前端，不推进游标。
            String questionNumber = interviewFlowStateMachine.currentQuestionNumber(flowState);
            String questionContent = resolveQuestionByNumber(sessionId, questionNumber, questions);
            if (StrUtil.isBlank(questionContent)) {
                return response.fail("question does not exist or expired");
            }

            fillCurrentQuestionResponse(sessionId, response, questionNumber, questionContent, flowState);
            return response.success();
        } catch (Exception e) {
            log.error("Failed to get current question, sessionId: {}", sessionId, e);
            return response.fail("failed to get current question: " + e.getMessage());
        }
    }

    /**
     * 仪态评估入口：对面试过程中用户的仪态（表情、动作等）进行评估。
     * 直接委托给 InterviewDemeanorService 处理。
     */
    public String evaluateDemeanor(DemeanorEvaluationReqDTO reqDTO) {
        return interviewDemeanorService.evaluateDemeanor(reqDTO);
    }

    /**
     * 组装当前题目响应体：填充当前题目内容、下一题信息、追问题标志、总分。
     */
    private void fillCurrentQuestionResponse(
            String sessionId,
            InterviewAnswerRespDTO response,
            String questionNumber,
            String questionContent,
            InterviewFlowState flowState) {
        response.withCurrentQuestion(questionNumber, questionContent);
        response.withNextQuestion(
                questionNumber,
                questionContent,
                isFollowUpQuestion(questionNumber),
                resolveFollowUpCount(flowState, questionNumber)
        );
        response.setTotalScore(interviewQuestionCacheService.getSessionTotalScore(sessionId));
    }

    /**
     * 获取面试题目列表：缓存优先，缓存 miss 时从 DB 回补并加载到缓存。
     *
     * @return 题目 Map（题号 → 题目内容），可能为空
     */
    private Map<String, String> getOrLoadQuestions(String sessionId) {
        Map<String, String> questions = interviewQuestionCacheService.getSessionInterviewQuestions(sessionId);
        if (questions == null || questions.isEmpty()) {
            interviewQuestionCacheService.loadInterviewQuestionsFromDatabase(sessionId);
            questions = interviewQuestionCacheService.getSessionInterviewQuestions(sessionId);
        }
        return questions;
    }

    /**
     * 按题号解析题目内容：先查本地 Map，再查 Redis 缓存，支持归一化前后的题号双重查找。
     *
     * @param sessionId      会话ID
     * @param questionNumber 原始题号（如 "3-F2"）
     * @param questions      题目 Map（题号 → 题目内容）
     * @return 题目内容，找不到返回 null
     */
    private String resolveQuestionByNumber(String sessionId, String questionNumber, Map<String, String> questions) {
        String normalizedQuestionNumber = normalizeQuestionNumber(questionNumber);
        if (StrUtil.isBlank(normalizedQuestionNumber)) {
            return null;
        }

        String questionContent = questions == null ? null : questions.get(normalizedQuestionNumber);
        if (StrUtil.isBlank(questionContent)) {
            questionContent = interviewQuestionCacheService.getQuestionByNumber(sessionId, normalizedQuestionNumber);
        }
        if (StrUtil.isBlank(questionContent) && !normalizedQuestionNumber.equals(questionNumber)) {
            questionContent = questions == null ? null : questions.get(questionNumber);
            if (StrUtil.isBlank(questionContent)) {
                questionContent = interviewQuestionCacheService.getQuestionByNumber(sessionId, questionNumber);
            }
        }
        return questionContent;
    }

    /**
     * 从对话轮次恢复当前题目（Redis flow 丢失时的降级策略）。
     *
     * 按优先级尝试：
     *   1) latestTurn.finished == true → 面试已结束
     *   2) latestTurn.nextQuestionNumber 有值 → 直接恢复该题
     *   3) latestTurn.nextQuestion 有内容 → 通过内容匹配题号后恢复
     *   4) lastTurn 的主问题号 + 1 → 恢复下一题
     *   5) 以上都失败 → 返回 empty，走 getCurrentQuestion 的第三级兜底（reinit flow）
     */
    private CurrentQuestionState recoverCurrentQuestionFromTurns(String sessionId, Map<String, String> questions) {
        List<InterviewTurnLog> turns = interviewQuestionCacheService.getInterviewTurns(sessionId);
        if (turns == null || turns.isEmpty()) {
            return CurrentQuestionState.empty();
        }

        InterviewTurnLog latestTurn = turns.get(turns.size() - 1);
        if (latestTurn == null) {
            return CurrentQuestionState.empty();
        }
        if (Boolean.TRUE.equals(latestTurn.getFinished())) {
            return CurrentQuestionState.finished();
        }

        String nextQuestionNumber = normalizeQuestionNumber(latestTurn.getNextQuestionNumber());
        String nextQuestionContent = resolveQuestionByNumber(sessionId, nextQuestionNumber, questions);
        if (StrUtil.isNotBlank(nextQuestionNumber) && StrUtil.isNotBlank(nextQuestionContent)) {
            return CurrentQuestionState.of(nextQuestionNumber, nextQuestionContent);
        }

        String nextQuestionFromTurn = latestTurn.getNextQuestion();
        if (StrUtil.isNotBlank(nextQuestionFromTurn)) {
            String matchedQuestionNumber = matchQuestionNumberByContent(sessionId, nextQuestionFromTurn, questions);
            if (StrUtil.isNotBlank(matchedQuestionNumber)) {
                String matchedQuestionContent = resolveQuestionByNumber(sessionId, matchedQuestionNumber, questions);
                if (StrUtil.isNotBlank(matchedQuestionContent)) {
                    return CurrentQuestionState.of(matchedQuestionNumber, matchedQuestionContent);
                }
            }
        }

        Integer answeredQuestionNo = extractMainQuestionNo(normalizeQuestionNumber(latestTurn.getQuestionNumber()));
        if (answeredQuestionNo != null) {
            int candidateQuestionNo = answeredQuestionNo + 1;
            if (questions != null && !questions.isEmpty() && candidateQuestionNo <= questions.size()) {
                String candidateQuestionNumber = String.valueOf(candidateQuestionNo);
                String candidateQuestionContent = resolveQuestionByNumber(sessionId, candidateQuestionNumber, questions);
                if (StrUtil.isNotBlank(candidateQuestionContent)) {
                    return CurrentQuestionState.of(candidateQuestionNumber, candidateQuestionContent);
                }
            }
        }

        return CurrentQuestionState.empty();
    }

    /**
     * 重建 flow 状态并推进到目标题号（分布式锁保护）。
     *
     * 步骤：
     *   1) 按 sessionId + questionNumber 获取分布式锁，防并发重建
     *   2) 双重检查：其他线程可能已重建完成
     *   3) 初始化 flow → 循环推进主问题游标到目标题 → 补齐 followUp 次数
     *   4) 如果获取锁失败，直接返回当前 flow 状态
     */
    private InterviewFlowState restoreFlowToQuestion(String sessionId, String questionNumber, int totalQuestions) {
        if (StrUtil.isBlank(sessionId) || StrUtil.isBlank(questionNumber) || totalQuestions <= 0) {
            return null;
        }
        RLock lock = null;
        try {
            // 1) 先按题号加锁恢复，防止并发恢复互相覆盖。
            lock = interviewQuestionLockService.acquire(sessionId, questionNumber);
            if (lock == null) {
                return interviewQuestionCacheService.getInterviewFlow(sessionId);
            }
            InterviewFlowState existingFlowState = interviewQuestionCacheService.getInterviewFlow(sessionId);
            if (existingFlowState != null) {
                return existingFlowState;
            }

            // 2) 先初始化 flow，再把主问题游标推进到目标主问题。
            interviewQuestionCacheService.initInterviewFlow(sessionId, totalQuestions);
            Integer mainQuestionNo = extractMainQuestionNo(questionNumber);
            if (mainQuestionNo == null || mainQuestionNo <= 0) {
                return interviewQuestionCacheService.getInterviewFlow(sessionId);
            }

            int targetMainQuestionNo = Math.min(mainQuestionNo, totalQuestions);
            for (int currentMainQuestionNo = 1; currentMainQuestionNo < targetMainQuestionNo; currentMainQuestionNo++) {
                InterviewFlowState advancedFlow = interviewFlowStateMachine.advanceMainQuestion(sessionId);
                if (advancedFlow == null || interviewFlowStateMachine.isCompleted(advancedFlow)) {
                    return advancedFlow;
                }
            }

            // 3) 如果目标是追问题，再补齐 followUp 次数，保证题号与 flow 状态一致。
            if (isFollowUpQuestion(questionNumber)) {
                int followUpCount = extractFollowUpCount(questionNumber);
                for (int index = 0; index < followUpCount; index++) {
                    interviewFlowStateMachine.startFollowUpQuestion(sessionId, questionNumber);
                }
            }
            return interviewQuestionCacheService.getInterviewFlow(sessionId);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return interviewQuestionCacheService.getInterviewFlow(sessionId);
        } finally {
            interviewQuestionLockService.release(lock);
        }
    }

    /**
     * 通过题目内容匹配题号：先匹配主问题，再匹配追问题。
     */
    private String matchQuestionNumberByContent(String sessionId, String questionContent, Map<String, String> questions) {
        String matchedMainQuestionNumber = matchQuestionNumberByContent(questionContent, questions);
        if (StrUtil.isNotBlank(matchedMainQuestionNumber)) {
            return matchedMainQuestionNumber;
        }
        Map<String, String> followUpQuestions = interviewQuestionCacheService.getSessionFollowUpQuestions(sessionId);
        return matchQuestionNumberByContent(questionContent, followUpQuestions);
    }

    /**
     * 通过题目内容精确匹配题号（重载版本，不区分主问题/追问题）。
     *
     * @param questionContent 题目内容文本
     * @param questions       题目 Map
     * @return 匹配到的题号，未匹配返回 null
     */
    private String matchQuestionNumberByContent(String questionContent, Map<String, String> questions) {
        if (StrUtil.isBlank(questionContent) || questions == null || questions.isEmpty()) {
            return null;
        }
        String target = questionContent.trim();
        for (Map.Entry<String, String> entry : questions.entrySet()) {
            if (entry == null || StrUtil.isBlank(entry.getValue())) {
                continue;
            }
            if (target.equals(entry.getValue().trim())) {
                return normalizeQuestionNumber(entry.getKey());
            }
        }
        return null;
    }

    /**
     * 安全解析正整数字符串，为空/非数字/非正数返回 null。
     */
    private Integer parsePositiveInt(String value) {
        if (StrUtil.isBlank(value)) {
            return null;
        }
        try {
            int parsed = Integer.parseInt(value);
            return parsed > 0 ? parsed : null;
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * 提取主问题编号：如 "3-F2" → 3，"5" → 5。
     */
    private Integer extractMainQuestionNo(String questionNumber) {
        String mainQuestionNumber = resolveMainQuestionNumber(questionNumber);
        return parsePositiveInt(mainQuestionNumber);
    }

    /**
     * 解析追问题次数：优先从题号中提取（如 "3-F2" → 2），提取不到则从 flowState 获取。
     */
    private Integer resolveFollowUpCount(InterviewFlowState flowState, String questionNumber) {
        if (!isFollowUpQuestion(questionNumber)) {
            return 0;
        }
        int parsedFollowUpCount = extractFollowUpCount(questionNumber);
        if (parsedFollowUpCount > 0) {
            return parsedFollowUpCount;
        }
        if (flowState != null && flowState.getFollowUpCount() != null) {
            return Math.max(flowState.getFollowUpCount(), 0);
        }
        return 0;
    }

    /**
     * 判断题号是否为追问题格式（如 "3-F2" 匹配 \\d+-F\\d+）。
     */
    private boolean isFollowUpQuestion(String questionNumber) {
        return StrUtil.isNotBlank(questionNumber) && questionNumber.trim().matches("\\d+-F\\d+");
    }

    /**
     * 从追问题号中提取追问题序号（如 "3-F2" → 2，"5-F1" → 1）。
     */
    private int extractFollowUpCount(String questionNumber) {
        if (!isFollowUpQuestion(questionNumber)) {
            return 0;
        }
        int separatorIndex = questionNumber.indexOf("-F");
        if (separatorIndex < 0 || separatorIndex + 2 >= questionNumber.length()) {
            return 0;
        }
        try {
            return Math.max(Integer.parseInt(questionNumber.substring(separatorIndex + 2).trim()), 0);
        } catch (Exception ex) {
            return 0;
        }
    }

    /**
     * 解析主问题号：从题号中提取主问题部分（"3-F2" → "3"，"5" → "5"）。
     */
    private String resolveMainQuestionNumber(String questionNumber) {
        if (StrUtil.isBlank(questionNumber)) {
            return null;
        }
        String normalized = questionNumber.trim();
        int separatorIndex = normalized.indexOf("-F");
        if (separatorIndex > 0) {
            return normalized.substring(0, separatorIndex);
        }
        return normalized;
    }

    private String normalizeQuestionNumber(String questionNumber) {
        if (StrUtil.isBlank(questionNumber)) {
            return null;
        }
        String normalized = questionNumber.trim();
        if (normalized.matches("\\d+")) {
            try {
                return String.valueOf(Integer.parseInt(normalized));
            } catch (Exception ex) {
                return normalized;
            }
        }
        return normalized;
    }

    private static final class CurrentQuestionState {
        private final boolean finished;
        private final String questionNumber;
        private final String questionContent;

        private CurrentQuestionState(boolean finished, String questionNumber, String questionContent) {
            this.finished = finished;
            this.questionNumber = questionNumber;
            this.questionContent = questionContent;
        }

        private static CurrentQuestionState finished() {
            return new CurrentQuestionState(true, null, null);
        }

        private static CurrentQuestionState of(String questionNumber, String questionContent) {
            return new CurrentQuestionState(false, questionNumber, questionContent);
        }

        private static CurrentQuestionState empty() {
            return new CurrentQuestionState(false, null, null);
        }

        private boolean hasQuestion() {
            return StrUtil.isNotBlank(questionNumber) && StrUtil.isNotBlank(questionContent);
        }
    }
}

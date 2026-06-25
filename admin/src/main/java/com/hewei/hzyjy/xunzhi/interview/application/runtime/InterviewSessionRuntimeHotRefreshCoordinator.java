package com.hewei.hzyjy.xunzhi.interview.application.runtime;

import cn.hutool.core.util.StrUtil;
import com.hewei.hzyjy.xunzhi.interview.config.InterviewHotRefreshConfiguration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 提供面试会话热快照合并写与防抖刷新的本地协调能力，
 * 用于按 session 聚合热刷新意图、延迟普通中间态刷盘并保证关键检查点立即落盘。
 *
 * @author 程序员牛肉
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InterviewSessionRuntimeHotRefreshCoordinator {

    private final InterviewHotRefreshConfiguration hotRefreshConfiguration;

    private final InterviewSessionRuntimeSnapshotService runtimeSnapshotService;

    @Qualifier("scheduledExecutorService")
    private final ScheduledExecutorService scheduledExecutorService;

    private final Map<String, InterviewSessionRuntimeHotRefreshBucket> pendingBuckets = new ConcurrentHashMap<>();

    /**
     * 提交热刷新请求的入口方法。
     * 根据配置和请求类型决定刷新策略：
     * - 功能未启用：直接同步刷盘
     * - 强制刷新（forceFlush）：立即刷盘并等待完成
     * - 普通请求：延迟防抖合并
     */
    public void submit(InterviewSessionRuntimeHotRefreshRequest request) {
        if (request == null || StrUtil.isBlank(request.getSessionId())) {
            return;
        }
        if (!Boolean.TRUE.equals(hotRefreshConfiguration.getEnable())) {
            runtimeSnapshotService.flushHotRefreshRequest(request);
            return;
        }
        if (request.isForceFlush()) {
            submitForceFlush(request);
            return;
        }
        submitDeferred(request);
    }

    /**
     * 延迟提交：将请求合并到对应 session 的桶（bucket）中，
     * 如果当前没有正在进行的刷新任务，则调度一个延迟刷新。
     * 多个请求在防抖窗口内会被合并，减少数据库写入次数。
     *
     * 详细流程：
     * 1. 以 sessionId 为 key，在 pendingBuckets 中查找或创建一个桶（InterviewSessionRuntimeHotRefreshBucket）
     * 2. 同步块内对桶进行操作，保证线程安全：
     *    a. 将传入的请求合并到桶中（bucket.merge()），会合并以下字段：
     *       - trigger（触发类型，高优先级覆盖低优先级）
     *       - snapshotLevel（快照级别）
     *       - requestId（请求ID）
     *       - committedTurn（提交的轮次日志）
     *       - persistTurnArchive（是否持久化轮次归档）
     *       - forceFlush（是否强制刷新标志）
     *    b. 如果桶正在刷新（flushInProgress=true），则直接返回，不调度新任务
     *    c. 否则，计算防抖延迟时间并调度刷新任务
     */
    private void submitDeferred(InterviewSessionRuntimeHotRefreshRequest request) {
        // 以 sessionId 为 key 获取或创建一个桶，用于聚合该 session 的所有刷新请求
        InterviewSessionRuntimeHotRefreshBucket bucket = pendingBuckets.computeIfAbsent(
                request.getSessionId(),
                InterviewSessionRuntimeHotRefreshBucket::new
        );
        // 使用桶对象作为锁，保证对该桶的并发访问安全
        synchronized (bucket) {
            // 将当前请求的数据合并到桶中，多个请求的数据会被累加/覆盖（如高优先级 trigger 会替换低优先级）
            bucket.merge(request);
            // 如果桶正在执行刷新任务，则不再调度新的刷新，等待当前任务完成后再处理后续请求
            if (bucket.isFlushInProgress()) {
                return;
            }
            // 计算防抖延迟时间并调度刷新任务
            // 防抖逻辑：如果在 debounceWindow 时间窗口内有新请求到达，会推迟刷新时间
            // 最大聚合时间：不超过 maxAggregateWindow，防止请求被无限推迟
            scheduleFlushLocked(bucket, computeDebounceDelayLocked(bucket));
        }
    }

    /**
     * 强制提交：合并请求后立即触发刷盘，并等待刷新完成（最多等待 forceFlushWaitMillis）。
     * 适用于关键检查点（如面试终结）需要立即落盘的场景。
     */
    private void submitForceFlush(InterviewSessionRuntimeHotRefreshRequest request) {
        InterviewSessionRuntimeHotRefreshBucket bucket = pendingBuckets.computeIfAbsent(
                request.getSessionId(),
                InterviewSessionRuntimeHotRefreshBucket::new
        );
        boolean flushNow = false;
        synchronized (bucket) {
            bucket.merge(request);
            bucket.clearScheduledFuture();
            long waitDeadline = System.currentTimeMillis() + positive(hotRefreshConfiguration.getForceFlushWaitMillis(), 600L);
            while (bucket.isFlushInProgress() && System.currentTimeMillis() < waitDeadline) {
                try {
                    long remaining = waitDeadline - System.currentTimeMillis();
                    bucket.wait(Math.min(Math.max(remaining, 1L), 50L));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            flushNow = !bucket.isFlushInProgress();
        }
        if (flushNow) {
            flushBucket(request.getSessionId(), bucket);
        }
    }

    /**
     * 执行桶刷新：将合并后的请求写入数据库。
     * 刷新完成后：
     * - 如果还有待刷新数据且是强制请求或上次失败，则立即重试（最多 maxImmediateFlushAttempts 次）
     * - 否则重新调度延迟刷新
     * - 如果无待处理数据则清理桶
     */
    private void flushBucket(String sessionId, InterviewSessionRuntimeHotRefreshBucket bucket) {
        int immediateAttempts = 0;
        while (true) {
            InterviewSessionRuntimeHotRefreshRequest flushRequest;
            synchronized (bucket) {
                flushRequest = bucket.startFlush();
                if (flushRequest == null) {
                    cleanupIfIdle(sessionId, bucket);
                    return;
                }
            }
            boolean success = runtimeSnapshotService.flushHotRefreshRequest(flushRequest);
            boolean continueImmediately = false;
            synchronized (bucket) {
                bucket.completeFlush();
                bucket.notifyAll();
                if (!success) {
                    bucket.restoreFailedRequest(flushRequest);
                }
                if (!bucket.hasPendingRefresh()) {
                    cleanupIfIdle(sessionId, bucket);
                    return;
                }
                if ((bucket.isPendingForceFlush() || !success)
                        && immediateAttempts < positive(hotRefreshConfiguration.getMaxImmediateFlushAttempts(), 2)) {
                    continueImmediately = true;
                    immediateAttempts++;
                } else {
                    long scheduleDelay = success
                            ? computeDebounceDelayLocked(bucket)
                            : positive(hotRefreshConfiguration.getFailureRetryDelayMillis(), 200L);
                    scheduleFlushLocked(bucket, scheduleDelay);
                }
            }
            if (!continueImmediately) {
                return;
            }
        }
    }

    /**
     * 调度延迟刷新：取消已有调度任务，重新安排一次延迟执行。
     *
     * 详细流程：
     * 1. 取消桶上已有的定时任务（如果存在），避免重复刷新
     * 2. 计算安全的延迟时间（不能为负数）
     * 3. 使用 ScheduledExecutorService 调度一个延迟任务，到期后执行 flushBucket
     * 4. 将定时任务句柄和计划执行时间绑定到桶上，方便后续取消或检查状态
     */
    private void scheduleFlushLocked(InterviewSessionRuntimeHotRefreshBucket bucket, long delayMillis) {
        // 取消之前已调度但未执行的刷新任务，防止同一个桶有多个待执行的刷新
        bucket.clearScheduledFuture();
        // 确保延迟时间不为负数
        long safeDelay = Math.max(delayMillis, 0L);
        long scheduledAt = System.currentTimeMillis() + safeDelay;
        // 调度延迟任务：到期后自动调用 flushBucket 执行实际的数据库写入
        ScheduledFuture<?> future = scheduledExecutorService.schedule(
                () -> flushBucket(bucket.getSessionId(), bucket),
                safeDelay,
                TimeUnit.MILLISECONDS
        );
        // 将定时任务句柄和计划执行时间记录到桶中，用于后续的状态检查和取消操作
        bucket.attachScheduledFuture(future, scheduledAt);
    }

    /**
     * 计算防抖延迟：基于当前调度时间和窗口配置，
     * 确保在 debounceWindow 和 maxAggregateWindow 之间找到合适的延迟。
     */
    private long computeDebounceDelayLocked(InterviewSessionRuntimeHotRefreshBucket bucket) {
        long now = System.currentTimeMillis();
        long scheduledAt = bucket.computeScheduledFlushAt(
                now,
                positive(hotRefreshConfiguration.getDebounceWindowMillis(), 150L),
                positive(hotRefreshConfiguration.getMaxAggregateWindowMillis(), 500L)
        );
        return Math.max(scheduledAt - now, 0L);
    }

    /**
     * 清理空闲桶：如果桶内无待刷新数据且无进行中的任务，则从 pendingBuckets 中移除，防止内存泄漏。
     */
    private void cleanupIfIdle(String sessionId, InterviewSessionRuntimeHotRefreshBucket bucket) {
        synchronized (bucket) {
            if (bucket.isFlushInProgress() || bucket.hasPendingRefresh()) {
                return;
            }
            if (bucket.getScheduledFuture() != null && !bucket.getScheduledFuture().isDone()) {
                return;
            }
        }
        pendingBuckets.remove(sessionId, bucket);
    }

    private long positive(Long value, long defaultValue) {
        return value != null && value > 0L ? value : defaultValue;
    }

    private int positive(Integer value, int defaultValue) {
        return value != null && value > 0 ? value : defaultValue;
    }
}

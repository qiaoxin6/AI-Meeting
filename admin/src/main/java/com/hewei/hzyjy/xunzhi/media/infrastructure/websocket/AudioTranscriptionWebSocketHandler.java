package com.hewei.hzyjy.xunzhi.media.infrastructure.websocket;

import com.alibaba.fastjson2.JSON;
import com.hewei.hzyjy.xunzhi.auth.application.WebSocketAuthService;
import com.hewei.hzyjy.xunzhi.media.infrastructure.integration.XunfeiAudioService;
import com.hewei.hzyjy.xunzhi.media.infrastructure.integration.XunfeiAudioService.RealtimeTranscriptionUpdate;
import jakarta.websocket.CloseReason;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.PathParam;
import jakarta.websocket.server.ServerEndpoint;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.Closeable;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Real-time speech-to-text WebSocket endpoint.
 */
@Slf4j
@Component
@ServerEndpoint(value = "/api/xunzhi/v1/xunfei/audio-to-text/{userId}")
public class AudioTranscriptionWebSocketHandler {

    private static volatile XunfeiAudioService xunfeiAudioService;
    private static volatile WebSocketAuthService webSocketAuthService;
    private static volatile ScheduledExecutorService heartbeatExecutor;

    @Autowired
    public void setXunfeiAudioService(XunfeiAudioService service) {
        AudioTranscriptionWebSocketHandler.xunfeiAudioService = service;
    }

    @Autowired
    public void setWebSocketAuthService(WebSocketAuthService service) {
        AudioTranscriptionWebSocketHandler.webSocketAuthService = service;
    }

    @Autowired
    public void setHeartbeatExecutor(@Qualifier("scheduledExecutorService") ScheduledExecutorService scheduledExecutorService) {
        AudioTranscriptionWebSocketHandler.heartbeatExecutor = scheduledExecutorService;
    }

    private static final ConcurrentMap<String, Session> USER_SESSIONS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, String> SESSION_USER_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, TranscriptionSessionContext> TRANSCRIPTION_CONTEXTS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, ScheduledFuture<?>> HEARTBEAT_TASKS = new ConcurrentHashMap<>();

    /**
     * WebSocket 连接建立回调：鉴权 → 注册会话映射 → 发送连接成功通知 → 启动心跳（30s 间隔）。
     */
    @OnOpen
    public void onOpen(Session session, @PathParam("userId") String userId) {
        if (!isAuthorizedUser(session, userId)) {
            log.warn("WebSocket auth failed, userId={}, sessionId={}", userId, session.getId());
            closeSession(session, "Unauthorized websocket connection");
            return;
        }

        String sessionId = session.getId();
        USER_SESSIONS.put(userId, session);
        SESSION_USER_MAP.put(sessionId, userId);
        log.info("WebSocket connected, userId={}, sessionId={}", userId, sessionId);

        sendMessage(session, createResponse("connected", "WebSocket connected", userId));
        startHeartbeat(session);
    }

    /**
     * 鉴权检查：调用 WebSocketAuthService 校验当前连接是否有权限使用该 userId。
     */
    private boolean isAuthorizedUser(Session session, String pathUserId) {
        if (webSocketAuthService == null) {
            log.error("WebSocketAuthService is not injected, reject websocket connection");
            return false;
        }
        return webSocketAuthService.isAuthorized(session, pathUserId);
    }

    private void closeSession(Session session, String reason) {
        if (session == null) {
            return;
        }
        try {
            if (session.isOpen()) {
                session.close(new CloseReason(CloseReason.CloseCodes.VIOLATED_POLICY, reason));
            }
        } catch (IOException ex) {
            log.warn("Failed to close websocket session, sessionId={}", session.getId(), ex);
        }
    }

    /**
     * 处理文本控制消息（start_transcription / stop_transcription / ping / get_status）。
     */
    @OnMessage
    public void onMessage(Session session, String message) {
        String userId = SESSION_USER_MAP.get(session.getId());
        log.info("Received text message, userId={}, message={}", userId, message);

        try {
            WebSocketMessage wsMessage = JSON.parseObject(message, WebSocketMessage.class);
            handleControlMessage(session, userId, wsMessage);
        } catch (Exception ex) {
            sendMessage(session, createResponse("info", "Received text message: " + message, null));
        }
    }

    /**
     * 处理音频二进制数据：将音频块写入 PipedOutputStream，供讯飞 SDK 异步消费识别。
     */
    @OnMessage
    public void onMessage(Session session, ByteBuffer byteBuffer) {
        String sessionId = session.getId();
        String userId = SESSION_USER_MAP.get(sessionId);
        log.debug("Received audio chunk, userId={}, sessionId={}, bytes={}",
                userId, sessionId, byteBuffer.remaining());

        try {
            byte[] audioData = new byte[byteBuffer.remaining()];
            byteBuffer.get(audioData);

            TranscriptionSessionContext context = TRANSCRIPTION_CONTEXTS.get(sessionId);
            if (context == null || !context.active.get()) {
                log.warn("Audio chunk received before transcription session started, userId={}, sessionId={}",
                        userId, sessionId);
                sendMessage(session, createResponse("error",
                        "Transcription session is not started. Send start_transcription first.", null));
                return;
            }

            context.audioOutputStream.write(audioData);
            context.audioOutputStream.flush();
        } catch (Exception ex) {
            log.error("Failed to process audio chunk, userId={}, sessionId={}", userId, sessionId, ex);
            sendMessage(session, createResponse("error", "Failed to process audio chunk: " + ex.getMessage(), null));
        }
    }

    /**
     * WebSocket 关闭回调：停止转写会话 → 取消心跳 → 清理用户/会话映射。
     */
    @OnClose
    public void onClose(Session session, CloseReason closeReason) {
        String sessionId = session.getId();
        String userId = SESSION_USER_MAP.get(sessionId);

        stopTranscriptionSession(sessionId);
        cancelHeartbeat(sessionId);

        if (userId != null) {
            USER_SESSIONS.remove(userId);
            SESSION_USER_MAP.remove(sessionId);
        }
        String reason = closeReason != null ? closeReason.getReasonPhrase() : "unknown";
        log.info("WebSocket closed, userId={}, sessionId={}, reason={}",
                userId, sessionId, reason);
    }

    @OnError
    public void onError(Session session, Throwable error) {
        String sessionId = session != null ? session.getId() : null;
        String userId = sessionId != null ? SESSION_USER_MAP.get(sessionId) : null;
        log.error("WebSocket error, userId={}, sessionId={}", userId, sessionId, error);

        if (sessionId != null) {
            stopTranscriptionSession(sessionId);
            cancelHeartbeat(sessionId);
        }
        sendMessage(session, createResponse("error", "WebSocket error: " + error.getMessage(), null));
    }

    /**
     * 路由控制命令到具体处理方法。
     */
    private void handleControlMessage(Session session, String userId, WebSocketMessage message) {
        String type = message != null ? message.getType() : null;
        if (type == null) {
            sendMessage(session, createResponse("unknown_command", "Missing command type", null));
            return;
        }

        switch (type) {
            case "ping" -> sendMessage(session, createResponse("pong", "pong", String.valueOf(System.currentTimeMillis())));
            case "start_transcription" -> startTranscriptionSession(session, userId);
            case "stop_transcription" -> {
                boolean stopped = stopTranscriptionSession(session.getId());
                if (stopped) {
                    sendMessage(session, createResponse("transcription_stopped", "Transcription stopped", null));
                } else {
                    sendMessage(session, createResponse("transcription_already_stopped",
                            "Transcription is already stopped", null));
                }
            }
            case "get_status" -> sendMessage(session, createResponse("status", "Connection is healthy", userId));
            default -> sendMessage(session, createResponse("unknown_command", "Unknown command: " + type, null));
        }
    }

    /**
     * 启动 30 秒间隔的心跳任务。
     * 取消已有任务避免重复调度（如重连场景），同时防止 Nginx/负载均衡器因超时误杀连接。
     */
    private void startHeartbeat(Session session) {
        // 注入失败则跳过心跳（优雅降级）
        if (heartbeatExecutor == null) {
            log.warn("scheduledExecutorService is not injected, skip heartbeat, sessionId={}", session.getId());
            return;
        }
        String sessionId = session.getId();
        // 先取消旧任务：防止同 session 多次启动心跳导致任务堆叠
        ScheduledFuture<?> oldTask = HEARTBEAT_TASKS.remove(sessionId);
        if (oldTask != null) {
            oldTask.cancel(true);
        }
        // scheduleAtFixedRate(任务, 首次延迟, 间隔, 单位)：首次 30s 后执行，之后每 30s 一次
        ScheduledFuture<?> task = heartbeatExecutor.scheduleAtFixedRate(() -> {
            if (session.isOpen()) {
                sendMessage(session, createResponse("heartbeat", "heartbeat", String.valueOf(System.currentTimeMillis())));
            }
        }, 30, 30, TimeUnit.SECONDS);
        // 保存任务句柄到全局 Map，用于 onClose 时取消
        HEARTBEAT_TASKS.put(sessionId, task);
    }

    private void cancelHeartbeat(String sessionId) {
        ScheduledFuture<?> task = HEARTBEAT_TASKS.remove(sessionId);
        if (task != null) {
            task.cancel(true);
        }
    }

    /**
     * 启动语音转写会话：创建 PipedInputStream/PipedOutputStream 管道 → 调用讯飞 SDK →
     * 通过异步回调将识别结果实时推送给客户端。用 putIfAbsent 防止并发重复启动。
     */
    private void startTranscriptionSession(Session session, String userId) {
        String sessionId = session.getId();
        // 快速检查：已有活跃且未请求停止的转写会话 → 直接返回
        TranscriptionSessionContext existing = TRANSCRIPTION_CONTEXTS.get(sessionId);
        if (existing != null && existing.active.get() && !existing.stopRequested.get()) {
            sendMessage(session, createResponse("transcription_already_started",
                    "Transcription is already started", null));
            return;
        }
        // 清理旧会话（可能已标记停止但未完全清理）
        stopTranscriptionSession(sessionId);
        // 创建管道 + 启动讯飞 SDK 异步识别
        TranscriptionSessionContext context = createAndStartTranscriptionSession(session, userId);
        if (context != null) {
            // putIfAbsent 并发保护：如果另一个线程抢先注册了，这里会返回非 null
            TranscriptionSessionContext raced = TRANSCRIPTION_CONTEXTS.putIfAbsent(sessionId, context);
            if (raced != null && raced.active.get() && !raced.stopRequested.get()) {
                // 并发冲突：另一个线程已启动 → 清理本次创建的冗余上下文
                context.active.set(false);
                context.stopRequested.set(true);
                closeQuietly(context.audioOutputStream);
                closeQuietly(context.audioInputStream);
                sendMessage(session, createResponse("transcription_already_started",
                        "Transcription is already started", null));
                return;
            }
            // 无冲突（或旧上下文已被抢占）→ 用 put 覆盖，确保当前上下文生效
            TRANSCRIPTION_CONTEXTS.put(sessionId, context);
            sendMessage(session, createResponse("transcription_started", "Transcription started", null));
        } else {
            sendMessage(session, createResponse("error", "Failed to start transcription", null));
        }
    }

    /**
     * 创建管道并调用讯飞 SDK 启动实时语音识别，识别结果通过回调实时推送给客户端。
     * 异步完成时推送最终结果或错误信息，并清理上下文。
     */
    private TranscriptionSessionContext createAndStartTranscriptionSession(Session session, String userId) {
        String sessionId = session.getId();
        try {
            if (xunfeiAudioService == null) {
                log.error("XunfeiAudioService is not injected yet, cannot start transcription. sessionId={}", sessionId);
                return null;
            }
            // 创建管道对：PipedOutputStream 写入端（WebSocket 音频入站线程写入）
            //            PipedInputStream 读取端（讯飞 SDK 线程读取）
            // 64KB 缓冲区，解耦生产者（客户端）和消费者（讯飞）
            PipedInputStream audioInputStream = new PipedInputStream(64 * 1024);
            PipedOutputStream audioOutputStream = new PipedOutputStream(audioInputStream);
            AtomicBoolean active = new AtomicBoolean(true);
            TranscriptionSessionContext context = new TranscriptionSessionContext(audioInputStream, audioOutputStream, active);

            // 调用讯飞 SDK 启动实时语音识别，传入 audioInputStream 作为音频源
            // 回调 lambda：每次收到部分识别结果时触发，实时推送给客户端
            CompletableFuture<String> future = xunfeiAudioService.realTimeAudioToText(audioInputStream, update ->
                    {
                        // 保存最新识别快照（用于最终结果构建时合并元数据）
                        context.lastUpdate.set(update);
                        // 实时推送部分识别结果：type="transcription", updateAction="replace"
                        sendMessage(session, createResponse("transcription", "Partial snapshot", update, true));
                    }
            );
            // 异步回调：识别完成或异常时触发
            future.whenComplete((finalResult, throwable) -> {
                if (throwable != null && !isExpectedStopException(context, throwable)) {
                    // 非预期异常（非主动 stop 导致的管道关闭）→ 推送错误
                    log.error("Transcription failed, userId={}, sessionId={}", userId, sessionId, throwable);
                    sendMessage(session, createResponse("error", "Transcription failed: " + throwable.getMessage(), null));
                } else {
                    log.info("Transcription finished, userId={}, sessionId={}", userId, sessionId);
                    // 主动停止且无最终文本则跳过，否则推送最终结果
                    if (!context.stopRequested.get() && finalResult != null) {
                        sendMessage(session, createResponse("final", "Transcription completed",
                                buildFinalUpdate(finalResult, context.lastUpdate.get()), true));
                    }
                }
                // 无论成功/失败/主动停止，都要清理管道和上下文
                cleanupTranscriptionContext(sessionId, context);
            });
            return context;
        } catch (Exception ex) {
            log.error("Failed to create transcription session, userId={}, sessionId={}", userId, sessionId, ex);
            return null;
        }
    }

    /**
     * 停止语音转写会话：设置标志位 → 关闭 PipedOutputStream（触发管道中断）→ 最终会由讯飞 SDK 检测并退出。
     */
    private boolean stopTranscriptionSession(String sessionId) {
        TranscriptionSessionContext context = TRANSCRIPTION_CONTEXTS.remove(sessionId);
        if (context == null) {
            return false;
        }
        context.active.set(false);
        context.stopRequested.set(true);
        closeQuietly(context.audioOutputStream);
        return true;
    }

    private void cleanupTranscriptionContext(String sessionId, TranscriptionSessionContext context) {
        TRANSCRIPTION_CONTEXTS.remove(sessionId, context);
        context.active.set(false);
        closeQuietly(context.audioOutputStream);
        closeQuietly(context.audioInputStream);
    }

    /**
     * 判断异常是否为主动 close 导致的预期异常（Pipe closed / Stream closed），
     * 避免将正常的停止操作误报为错误。
     */
    private boolean isExpectedStopException(TranscriptionSessionContext context, Throwable throwable) {
        if (!context.stopRequested.get()) {
            return false;
        }
        Throwable cursor = throwable;
        while (cursor != null) {
            String msg = cursor.getMessage();
            if (msg != null && (msg.contains("Pipe closed") || msg.contains("Stream closed"))) {
                return true;
            }
            cursor = cursor.getCause();
        }
        return false;
    }

    private void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException ignored) {
            // no-op
        }
    }

    private void sendMessage(Session session, String message) {
        if (session != null && session.isOpen()) {
            try {
                session.getBasicRemote().sendText(message);
            } catch (IOException ex) {
                log.error("Failed to send message, sessionId={}", session.getId(), ex);
            }
        }
    }

    /**
     * 向指定用户推送消息（静态方法，可从其他 Service 直接调用），用户离线则忽略。
     */
    public static void sendMessageToUser(String userId, String type, String message, String data) {
        Session session = USER_SESSIONS.get(userId);
        if (session == null || !session.isOpen()) {
            log.warn("User is offline, userId={}", userId);
            return;
        }
        try {
            session.getBasicRemote().sendText(createStaticResponse(type, message, data));
        } catch (IOException ex) {
            log.error("Failed to send message to user, userId={}", userId, ex);
        }
    }

    /**
     * 向所有在线用户广播消息。
     */
    public static void broadcastMessage(String type, String message, String data) {
        String payload = createStaticResponse(type, message, data);
        USER_SESSIONS.forEach((userId, session) -> {
            if (session.isOpen()) {
                try {
                    session.getBasicRemote().sendText(payload);
                } catch (IOException ex) {
                    log.error("Broadcast failed, userId={}", userId, ex);
                }
            }
        });
    }

    public static Set<String> getOnlineUsers() {
        return USER_SESSIONS.keySet();
    }

    public static boolean isUserOnline(String userId) {
        Session session = USER_SESSIONS.get(userId);
        return session != null && session.isOpen();
    }

    private String createResponse(String type, String message, String data) {
        return createResponse(type, message, data, false);
    }

    private String createResponse(String type, String message, String data, boolean isSnapshot) {
        WebSocketResponse response = new WebSocketResponse();
        response.setType(type);
        response.setMessage(message);
        response.setData(data);
        response.setFullText(resolveFullText(type, data));
        response.setIsSnapshot(isSnapshot);
        response.setUpdateAction(resolveUpdateAction(type));
        response.setTimestamp(System.currentTimeMillis());
        return JSON.toJSONString(response);
    }

    private String createResponse(String type,
                                  String message,
                                  RealtimeTranscriptionUpdate update,
                                  boolean isSnapshot) {
        WebSocketResponse response = new WebSocketResponse();
        response.setType(type);
        response.setMessage(message);
        response.setData(update != null ? update.fullText() : null);
        response.setFullText(update != null ? update.fullText() : null);
        response.setDisplayText(update != null ? update.displayText() : null);
        response.setCommittedText(update != null ? update.committedText() : null);
        response.setLiveText(update != null ? update.liveText() : null);
        response.setRevision(update != null ? update.revision() : null);
        response.setResultStatus(update != null ? update.resultStatus() : null);
        response.setIsSnapshot(isSnapshot);
        response.setUpdateAction(resolveUpdateAction(type));
        response.setTimestamp(System.currentTimeMillis());
        if (update != null) {
            response.setSegmentId(update.segmentId());
            response.setSentenceSeq(update.segmentId());
            response.setSegmentText(update.segmentText());
            response.setPgs(update.pgs());
            response.setRg(update.rg());
            response.setBg(update.bg());
            response.setEd(update.ed());
            response.setIsFinalPacket(update.finalPacket());
        }
        return JSON.toJSONString(response);
    }

    /**
     * 构建最终的识别结果快照：以 finalResult 作为 fullText/committedText，保留上次更新的辅助元数据。
     */
    private RealtimeTranscriptionUpdate buildFinalUpdate(String finalResult,
                                                         RealtimeTranscriptionUpdate lastUpdate) {
        if (lastUpdate == null) {
            return new RealtimeTranscriptionUpdate(
                    finalResult,
                    finalResult,
                    "",
                    finalResult,
                    1,
                    "final",
                    0,
                    finalResult,
                    null,
                    null,
                    null,
                    null,
                    true
            );
        }
        return new RealtimeTranscriptionUpdate(
                finalResult,
                finalResult,
                "",
                finalResult,
                lastUpdate.revision() != null ? lastUpdate.revision() + 1 : 1,
                "final",
                lastUpdate.segmentId(),
                lastUpdate.segmentText(),
                lastUpdate.pgs(),
                lastUpdate.rg(),
                lastUpdate.bg(),
                lastUpdate.ed(),
                true
        );
    }

    private static String createStaticResponse(String type, String message, String data) {
        WebSocketResponse response = new WebSocketResponse();
        response.setType(type);
        response.setMessage(message);
        response.setData(data);
        response.setFullText(resolveFullText(type, data));
        response.setIsSnapshot(false);
        response.setUpdateAction(resolveUpdateAction(type));
        response.setTimestamp(System.currentTimeMillis());
        return JSON.toJSONString(response);
    }

    private static String resolveFullText(String type, String data) {
        if ("transcription".equals(type) || "final".equals(type)) {
            return data;
        }
        return null;
    }

    private static String resolveUpdateAction(String type) {
        if ("transcription".equals(type)) {
            return "replace";
        }
        if ("final".equals(type)) {
            return "archive";
        }
        return "none";
    }

    @Data
    public static class WebSocketResponse {
        private String type;
        private String message;
        private String data;
        private String fullText;
        private String displayText;
        private String committedText;
        private String liveText;
        private Integer revision;
        private String resultStatus;
        private Boolean isSnapshot;
        private String updateAction;
        private Long timestamp;
        private Integer segmentId;
        private Integer sentenceSeq;
        private String segmentText;
        private String pgs;
        private int[] rg;
        private Integer bg;
        private Integer ed;
        private Boolean isFinalPacket;
    }

    @Data
    public static class WebSocketMessage {
        private String type;
    }

    private static class TranscriptionSessionContext {
        private final PipedInputStream audioInputStream;
        private final PipedOutputStream audioOutputStream;
        private final AtomicBoolean active;
        private final AtomicBoolean stopRequested = new AtomicBoolean(false);
        private final AtomicReference<RealtimeTranscriptionUpdate> lastUpdate = new AtomicReference<>();

        private TranscriptionSessionContext(PipedInputStream audioInputStream,
                                            PipedOutputStream audioOutputStream,
                                            AtomicBoolean active) {
            this.audioInputStream = audioInputStream;
            this.audioOutputStream = audioOutputStream;
            this.active = active;
        }
    }
}

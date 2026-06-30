package com.hewei.hzyjy.xunzhi.media.infrastructure.integration;

import cn.hutool.core.util.StrUtil;
import cn.xfyun.api.IatClient;
import cn.xfyun.model.response.iat.IatResponse;
import cn.xfyun.model.response.iat.IatResult;
import cn.xfyun.model.response.iat.Text;
import cn.xfyun.service.iat.AbstractIatWebSocketListener;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.hewei.hzyjy.xunzhi.common.config.storage.ApplicationStorageProperties;
import com.hewei.hzyjy.xunzhi.common.config.xunfei.XunfeiLatProperties;
import com.hewei.hzyjy.xunzhi.common.convention.exception.ClientException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import org.apache.commons.codec.binary.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Xunfei speech recognition service.
 * Supports file transcription and real-time large-model transcription.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class XunfeiAudioService {

    private static final OkHttpClient WS_CLIENT = new OkHttpClient.Builder()
            .pingInterval(20, TimeUnit.SECONDS)
            .build();

    private static final String AST_WS_BASE_URL = "wss://office-api-ast-dx.iflyaisol.com/ast/communicate/v1";
    private static final int CHUNK_SIZE_BYTES = 1280;

    private final XunfeiLatProperties xunfeiLatPropertiesConfig;
    private final ApplicationStorageProperties storageProperties;
    @Resource(name = "queryExecutor")
    private ExecutorService queryExecutor;

    private IatClient iatClient;

    @PostConstruct
    public void init() {
        try {
            // 初始化讯飞 IAT（语音听写）客户端，dwa="wpgs" 开启动态分词
            iatClient = new IatClient.Builder()
                    .signature(
                            normalize(xunfeiLatPropertiesConfig.getAppId()),
                            normalize(xunfeiLatPropertiesConfig.getApiKey()),
                            normalize(xunfeiLatPropertiesConfig.getApiSecret())
                    )
                    .dwa("wpgs")
                    .build();

            log.info("Xunfei audio service initialized, appId={}, apiKeyLen={}, apiSecretLen={}",
                    mask(normalize(xunfeiLatPropertiesConfig.getAppId())),
                    lengthOf(normalize(xunfeiLatPropertiesConfig.getApiKey())),
                    lengthOf(normalize(xunfeiLatPropertiesConfig.getApiSecret())));
        } catch (Exception ex) {
            log.error("Failed to initialize Xunfei audio service", ex);
            throw new RuntimeException("Failed to initialize Xunfei audio service", ex);
        }
    }

    /**
     * 文件转写（离线模式）：将上传的音频文件转为文本。
     * 流程：临时保存文件 → 通过 IAT 客户端发送 → 异步等待结果 → 清理临时文件。
     */
    public CompletableFuture<String> convertAudioToText(MultipartFile audioFile) {
        CompletableFuture<String> future = new CompletableFuture<>();
        CountDownLatch latch = new CountDownLatch(1);
        List<Text> resultSegments = new ArrayList<>();

        AbstractIatWebSocketListener listener = new AbstractIatWebSocketListener() {
            @Override
            public void onSuccess(WebSocket webSocket, IatResponse iatResponse) {
                if (iatResponse.getCode() != 0) {
                    future.completeExceptionally(new RuntimeException("IAT failed: " + iatResponse.getMessage()));
                    latch.countDown();
                    return;
                }

                if (iatResponse.getData() != null && iatResponse.getData().getResult() != null) {
                    IatResult result = iatResponse.getData().getResult();
                    Text textObject = result.getText();
                    if (textObject != null) {
                        handleResultText(textObject, resultSegments);
                    }

                    if (iatResponse.getData().getStatus() == 2) {
                        future.complete(getFinalResult(resultSegments));
                        latch.countDown();
                    }
                }
            }

            @Override
            public void onFail(WebSocket webSocket, Throwable t, Response response) {
                future.completeExceptionally(t);
                latch.countDown();
            }
        };

        try {
            Path tempDir = storageProperties.getAudioTempPath();
            Files.createDirectories(tempDir);

            String fileName = "audio_" + System.currentTimeMillis() + "_" + Thread.currentThread().getId() + ".tmp";
            Path tempFilePath = tempDir.resolve(fileName);
            try (var inputStream = audioFile.getInputStream()) {
                Files.copy(inputStream, tempFilePath, StandardCopyOption.REPLACE_EXISTING);
            }

            File tempFile = tempFilePath.toFile();
            iatClient.send(tempFile, listener);

            if (!latch.await(60, TimeUnit.SECONDS) && !future.isDone()) {
                future.completeExceptionally(new RuntimeException("IAT timeout"));
            }
            cleanupTempFile(tempFilePath);
        } catch (Exception ex) {
            future.completeExceptionally(ex);
        }

        return future;
    }

    /**
     * 实时语音转写（流式模式）：通过 AST WebSocket 将音频流实时发送给讯飞大模型 ASR，
     * 识别结果通过 callback 实时推送，最终结果通过 CompletableFuture 返回。
     *
     * 数据流：audioInputStream → 1280 字节分块 → WebSocket 发送
     *        WebSocket 接收 → JSON 解析 → Assembler 拼接 → callback 推送
     */
    public CompletableFuture<String> realTimeAudioToText(InputStream audioInputStream, AudioResultCallback callback) {
        CompletableFuture<String> future = new CompletableFuture<>();

        if (audioInputStream == null) {
            future.completeExceptionally(new ClientException("audioInputStream cannot be null"));
            return future;
        }

        String appId = normalize(xunfeiLatPropertiesConfig.getAppId());
        String apiKey = normalize(xunfeiLatPropertiesConfig.getApiKey());
        String apiSecret = normalize(xunfeiLatPropertiesConfig.getApiSecret());

        if (StrUtil.isBlank(appId) || StrUtil.isBlank(apiKey) || StrUtil.isBlank(apiSecret)) {
            future.completeExceptionally(new RuntimeException(
                    "Large-model realtime ASR requires appId/apiKey/apiSecret"));
            return future;
        }

        String sessionId = UUID.randomUUID().toString();
        String wsUrl;
        try {
            wsUrl = buildAstUrl(appId, apiKey, apiSecret, sessionId);
        } catch (Exception ex) {
            future.completeExceptionally(new RuntimeException("Failed to build AST websocket URL", ex));
            return future;
        }

        AstTranscriptionAssembler assembler = new AstTranscriptionAssembler();
        AtomicInteger fallbackSn = new AtomicInteger(0);
        AtomicInteger rawPacketCounter = new AtomicInteger(0);
        AtomicInteger revisionCounter = new AtomicInteger(0);
        StringBuilder latestDisplay = new StringBuilder();
        Request request = new Request.Builder().url(wsUrl).build();
        WS_CLIENT.newWebSocket(request, new WebSocketListener() {
            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                CompletableFuture.runAsync(
                        () -> sendAudioStream(webSocket, audioInputStream, sessionId, future),
                        queryExecutor
                );
            }

            @Override
            public void onMessage(WebSocket webSocket, String text) {
                try {
                    int packetNo = rawPacketCounter.incrementAndGet();
                    log.info("[ASR_RAW_PACKET] sessionId={}, packetNo={}, payload={}", sessionId, packetNo, text);
                    JSONObject root = JSONObject.parseObject(text);
                    String action = root.getString("action");
                    if ("error".equalsIgnoreCase(action)) {
                        String code = root.getString("code");
                        String desc = root.getString("desc");
                        future.completeExceptionally(new RuntimeException(
                                "AST business failure: code=" + code + ", desc=" + desc + ", raw=" + text));
                        return;
                    }

                    JSONObject st = extractAstSt(root);
                    String partialText = extractAstText(root);
                    int segmentId = resolveSegmentId(root, st, fallbackSn);
                    String pgs = st != null ? st.getString("pgs") : null;
                    Integer bg = st != null ? st.getInteger("bg") : null;
                    Integer ed = st != null ? st.getInteger("ed") : null;
                    int[] rg = extractRg(st);
                    boolean finalPacket = isAstFinal(root);
                    if (StrUtil.isNotBlank(partialText)) {
                        assembler.apply(segmentId, pgs, rg, bg, ed, partialText, finalPacket);
                        String merged = assembler.buildSnapshot();
                        if (!merged.equals(latestDisplay.toString())) {
                            String committedText = assembler.buildCommittedText(segmentId, finalPacket);
                            String liveText = assembler.buildLiveText(committedText, merged, partialText, finalPacket);
                            latestDisplay.setLength(0);
                            latestDisplay.append(merged);
                            if (callback != null) {
                                callback.onResult(new RealtimeTranscriptionUpdate(
                                        merged,
                                        committedText,
                                        liveText,
                                        merged,
                                        revisionCounter.incrementAndGet(),
                                        finalPacket ? "final" : "partial",
                                        segmentId,
                                        partialText,
                                        pgs,
                                        rg,
                                        bg,
                                        ed,
                                        finalPacket
                                ));
                            }
                        }
                    }

                    if (finalPacket) {
                        if (!future.isDone()) {
                            String finalText = assembler.buildSnapshot();
                            if (StrUtil.isBlank(finalText)) {
                                finalText = latestDisplay.toString();
                            }
                            future.complete(finalText);
                        }
                        webSocket.close(1000, "completed");
                    }
                } catch (Exception ex) {
                    if (!future.isDone()) {
                        future.completeExceptionally(new RuntimeException("Failed to parse AST response: " + text, ex));
                    }
                }
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                if (!future.isDone()) {
                    future.completeExceptionally(new RuntimeException("AST websocket failure", t));
                }
            }

            @Override
            public void onClosed(WebSocket webSocket, int code, String reason) {
                if (!future.isDone()) {
                    String finalText = assembler.buildSnapshot();
                    if (StrUtil.isBlank(finalText)) {
                        finalText = latestDisplay.toString();
                    }
                    future.complete(finalText);
                }
            }
        });

        return future;
    }

    private JSONObject extractAstSt(JSONObject root) {
        if (root == null) {
            return null;
        }

        JSONObject data = root.getJSONObject("data");
        if (data != null) {
            JSONObject cn = data.getJSONObject("cn");
            if (cn != null && cn.getJSONObject("st") != null) {
                return cn.getJSONObject("st");
            }
            if (data.getJSONObject("st") != null) {
                return data.getJSONObject("st");
            }
        }

        JSONObject cn = root.getJSONObject("cn");
        if (cn != null && cn.getJSONObject("st") != null) {
            return cn.getJSONObject("st");
        }
        return root.getJSONObject("st");
    }

    private int resolveSegmentId(JSONObject root, JSONObject st, AtomicInteger fallbackSn) {
        Integer segId = extractSegmentId(root, st);
        if (segId != null) {
            fallbackSn.set(Math.max(fallbackSn.get(), segId));
            return segId;
        }
        return fallbackSn.incrementAndGet();
    }

    private Integer extractSegmentId(JSONObject root, JSONObject st) {
        JSONObject data = root != null ? root.getJSONObject("data") : null;
        Integer segId = data != null ? data.getInteger("seg_id") : null;
        if (segId != null) {
            return segId;
        }

        segId = st != null ? st.getInteger("seg_id") : null;
        if (segId != null) {
            return segId;
        }

        return st != null ? st.getInteger("sn") : null;
    }

    private int[] extractRg(JSONObject st) {
        if (st == null) {
            return null;
        }
        JSONArray rg = st.getJSONArray("rg");
        if (rg == null || rg.size() < 2) {
            return null;
        }

        Integer start = rg.getInteger(0);
        Integer end = rg.getInteger(1);
        if (start == null || end == null) {
            return null;
        }
        if (start > end) {
            int tmp = start;
            start = end;
            end = tmp;
        }
        return new int[]{start, end};
    }

    private void applyAstSegment(TreeMap<Integer, SegmentState> sentencePool,
                                 int sn,
                                 String pgs,
                                 int[] rg,
                                 String text,
                                 boolean finalized) {
        if (sentencePool == null || text == null) {
            return;
        }

        if ("rpl".equalsIgnoreCase(pgs)) {
            if (rg != null) {
                for (int i = rg[0]; i <= rg[1]; i++) {
                    sentencePool.remove(i);
                }
            }
            upsertAstSegment(sentencePool, sn, text, finalized);
            return;
        }

        if ("apd".equalsIgnoreCase(pgs)) {
            upsertAstSegment(sentencePool, sn, text, finalized);
            return;
        }

        upsertAstSegment(sentencePool, sn, text, finalized);
    }

    /**
     * 无 pgs 模式下的 AST 片段合并：基于 bg/ed 字符位置范围进行智能合并。
     *
     * 为什么需要无 pgs 模式？
     * 讯飞大模型 ASR 有两种结果格式：
     *   - 有 pgs（rpl/apd）：明确告诉你怎么替换/追加片段，直接按指令操作即可
     *   - 无 pgs：只给出 bg/ed（字符起止位置）和文本，需要自己判断是新增还是更新已有片段
     *
     * 决策链（按优先级）：
     *   1. bg/ed 为空 → 直接按 segmentId 插入，不做智能合并
     *   2. 精确 bg/ed 匹配 + 纯标点 → 追加标点到已有片段末尾
     *   3. 找到可复用的重叠片段（≥60% 范围重叠 + 文本相似）→ 更新该片段 + 清除被覆盖的冗余片段
     *   4. 找不到可复用 → 清除被覆盖的冗余片段 + 作为新片段插入
     */
    private void applyAstSegmentWithoutPgs(TreeMap<Integer, SegmentState> sentencePool,
                                           int sn,
                                           Integer bg,
                                           Integer ed,
                                           String text,
                                           boolean finalized) {
        if (sentencePool == null || text == null) {
            return;
        }
        // 策略1：无位置范围 → 退化为简单 upsert，按 segmentId 直接覆盖
        if (bg == null || ed == null) {
            upsertAstSegment(sentencePool, sn, text, finalized);
            return;
        }
        // 策略2：找到 bg/ed 完全一致的已有片段，且新文本只是标点 → 追加标点到已有文本末尾
        // 典型场景：讯飞先返回"你好"，再返回"。"，这是同一句话的标点补充
        SegmentState sameRange = findExactRangeState(sentencePool, bg, ed);
        if (sameRange != null && isPunctuationOnly(text) && StrUtil.isNotBlank(sameRange.text)) {
            sameRange.text = appendTrailingPunctuation(sameRange.text, text);
            sameRange.finalized = sameRange.finalized || finalized;
            sameRange.updatedAt = System.currentTimeMillis();
            return;
        }
        // 策略3：查找可复用的重叠片段（范围有重叠 + 文本高度相似）
        SegmentState reusable = findReusableRangeState(sentencePool, bg, ed, text);
        if (reusable != null) {
            // 找到可复用 → 更新文本 + 清除被该范围完全覆盖的冗余兄弟片段
            updateSegmentState(reusable, text, finalized, bg, ed);
            removeCoveredSiblingRangeStates(sentencePool, reusable.segId, bg, ed, text);
            return;
        }
        // 策略4：找不到任何可复用片段 → 清除被覆盖的旧片段，作为全新片段插入
        removeCoveredSiblingRangeStates(sentencePool, null, bg, ed, text);
        upsertAstSegment(sentencePool, sn, text, finalized);
        SegmentState state = sentencePool.get(sn);
        if (state != null) {
            state.bg = bg;
            state.ed = ed;
        }
    }

    private SegmentState findExactRangeState(TreeMap<Integer, SegmentState> sentencePool, Integer bg, Integer ed) {
        if (bg == null || ed == null || sentencePool == null || sentencePool.isEmpty()) {
            return null;
        }
        for (SegmentState state : sentencePool.values()) {
            if (state == null || state.bg == null || state.ed == null) {
                continue;
            }
            if (state.bg.equals(bg) && state.ed.equals(ed)) {
                return state;
            }
        }
        return null;
    }

    /**
     * 查找可复用的历史片段：遍历 sentencePool，找到与当前 bg/ed 范围重叠且文本高度相似（≥60% 重叠率 + 80% 公共前缀）的片段。
     * 这是无 pgs 模式下避免重复创建片段的智能合并核心。
     */
    private SegmentState findReusableRangeState(TreeMap<Integer, SegmentState> sentencePool,
                                                Integer bg,
                                                Integer ed,
                                                String text) {
        if (bg == null || ed == null || sentencePool == null || sentencePool.isEmpty() || StrUtil.isBlank(text)) {
            return null;
        }

        SegmentState best = null;
        double bestScore = -1D;
        for (SegmentState state : sentencePool.values()) {
            if (state == null || state.bg == null || state.ed == null) {
                continue;
            }
            if (!isRangeOverlapping(bg, ed, state.bg, state.ed)) {
                continue;
            }
            if (state.bg.equals(bg) && state.ed.equals(ed)) {
                return state;
            }

            double overlapRatio = calculateOverlapRatio(bg, ed, state.bg, state.ed);
            if (overlapRatio < 0.6D || !isLikelySameSegmentEvolution(state.text, text)) {
                continue;
            }

            double score = overlapRatio;
            if (containsComparableText(text, state.text)) {
                score += 1D;
            }
            if (score > bestScore) {
                bestScore = score;
                best = state;
            }
        }
        return best;
    }

    private void removeCoveredSiblingRangeStates(TreeMap<Integer, SegmentState> sentencePool,
                                                 Integer retainedSegId,
                                                 Integer bg,
                                                 Integer ed,
                                                 String text) {
        if (bg == null || ed == null || sentencePool == null || sentencePool.isEmpty()) {
            return;
        }
        Iterator<Map.Entry<Integer, SegmentState>> iterator = sentencePool.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, SegmentState> entry = iterator.next();
            SegmentState state = entry.getValue();
            if (state == null || state.bg == null || state.ed == null) {
                continue;
            }
            if (retainedSegId != null && retainedSegId.equals(entry.getKey())) {
                continue;
            }
            if (isRangeFullyCoveredBy(bg, ed, state.bg, state.ed)
                    && isLikelySameSegmentEvolution(state.text, text)) {
                iterator.remove();
            }
        }
    }

    private boolean isRangeOverlapping(int bg1, int ed1, int bg2, int ed2) {
        return bg1 <= ed2 && bg2 <= ed1;
    }

    private boolean isRangeFullyCoveredBy(int outerBg, int outerEd, int innerBg, int innerEd) {
        return outerBg <= innerBg && outerEd >= innerEd;
    }

    private double calculateOverlapRatio(int bg1, int ed1, int bg2, int ed2) {
        int overlapStart = Math.max(bg1, bg2);
        int overlapEnd = Math.min(ed1, ed2);
        int overlap = overlapEnd - overlapStart;
        if (overlap <= 0) {
            return 0D;
        }
        int span1 = Math.max(1, ed1 - bg1);
        int span2 = Math.max(1, ed2 - bg2);
        return (double) overlap / Math.min(span1, span2);
    }

    private boolean isPunctuationOnly(String text) {
        if (StrUtil.isBlank(text)) {
            return false;
        }
        String trimmed = rtrim(text);
        if (trimmed.isEmpty()) {
            return false;
        }
        for (int i = 0; i < trimmed.length(); i++) {
            char ch = trimmed.charAt(i);
            if (Character.isWhitespace(ch)) {
                continue;
            }
            if (Character.isLetterOrDigit(ch) || Character.UnicodeScript.of(ch) == Character.UnicodeScript.HAN) {
                return false;
            }
        }
        return true;
    }

    private String appendTrailingPunctuation(String baseText, String punctuation) {
        String base = rtrim(baseText);
        String suffix = rtrim(punctuation);
        if (suffix.isEmpty()) {
            return base;
        }
        if (base.endsWith(suffix)) {
            return base;
        }
        return base + suffix;
    }

    private String rtrim(String text) {
        if (text == null) {
            return "";
        }
        int end = text.length();
        while (end > 0 && Character.isWhitespace(text.charAt(end - 1))) {
            end--;
        }
        return text.substring(0, end);
    }

    private boolean isLikelySameSegmentEvolution(String existingText, String incomingText) {
        String existingComparable = toComparableText(existingText);
        String incomingComparable = toComparableText(incomingText);
        if (existingComparable.isEmpty() || incomingComparable.isEmpty()) {
            return false;
        }
        if (existingComparable.equals(incomingComparable)) {
            return true;
        }
        if (incomingComparable.contains(existingComparable) || existingComparable.contains(incomingComparable)) {
            return true;
        }
        int commonPrefix = commonPrefixLength(existingComparable, incomingComparable);
        int minLength = Math.min(existingComparable.length(), incomingComparable.length());
        return minLength > 0 && ((double) commonPrefix / minLength) >= 0.8D;
    }

    private boolean containsComparableText(String source, String candidate) {
        String sourceComparable = toComparableText(source);
        String candidateComparable = toComparableText(candidate);
        if (sourceComparable.isEmpty() || candidateComparable.isEmpty()) {
            return false;
        }
        return sourceComparable.contains(candidateComparable);
    }

    private String toComparableText(String text) {
        if (text == null) {
            return "";
        }
        StringBuilder builder = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            if (Character.isLetterOrDigit(ch) || Character.UnicodeScript.of(ch) == Character.UnicodeScript.HAN) {
                builder.append(ch);
            }
        }
        return builder.toString();
    }

    private int commonPrefixLength(String left, String right) {
        int limit = Math.min(left.length(), right.length());
        int index = 0;
        while (index < limit && left.charAt(index) == right.charAt(index)) {
            index++;
        }
        return index;
    }

    private void updateSegmentState(SegmentState state,
                                    String text,
                                    boolean finalized,
                                    Integer bg,
                                    Integer ed) {
        if (state == null) {
            return;
        }
        state.text = text;
        state.finalized = state.finalized || finalized;
        state.bg = bg;
        state.ed = ed;
        state.updatedAt = System.currentTimeMillis();
    }

    private void upsertAstSegment(TreeMap<Integer, SegmentState> sentencePool,
                                  int sn,
                                  String text,
                                  boolean finalized) {
        SegmentState state = sentencePool.get(sn);
        if (state == null) {
            state = new SegmentState(sn);
        }
        state.text = text;
        state.finalized = state.finalized || finalized;
        state.updatedAt = System.currentTimeMillis();
        sentencePool.put(sn, state);
    }

    private String buildFinalResult(Map<Integer, SegmentState> sentencePool) {
        if (sentencePool == null || sentencePool.isEmpty()) {
            return "";
        }
        StringBuilder result = new StringBuilder();
        for (SegmentState part : sentencePool.values()) {
            if (part != null && part.text != null) {
                result.append(part.text);
            }
        }
        return result.toString();
    }

    /**
     * 将音频输入流按 1280 字节分块发送到 AST WebSocket，每 40ms 发送一块以模拟实时语速。
     * 发送完毕后发送结束标记 {"end":true}。
     */
    private void sendAudioStream(WebSocket webSocket,
                                 InputStream audioInputStream,
                                 String sessionId,
                                 CompletableFuture<String> future) {
        byte[] buffer = new byte[CHUNK_SIZE_BYTES];
        try (InputStream in = audioInputStream) {
            int read;
            while ((read = in.read(buffer)) != -1) {
                byte[] chunk = read == buffer.length ? buffer : copyOf(buffer, read);
                boolean ok = webSocket.send(okio.ByteString.of(chunk, 0, chunk.length));
                if (!ok) {
                    throw new RuntimeException("AST websocket send returned false");
                }
                Thread.sleep(40);
            }
            webSocket.send("{\"end\":true,\"sessionId\":\"" + sessionId + "\"}");
        } catch (Exception ex) {
            if (!future.isDone()) {
                future.completeExceptionally(new RuntimeException("Failed to send AST audio stream", ex));
            }
            webSocket.close(1011, "send failed");
        }
    }

    /**
     * 构建 AST WebSocket 连接 URL：按字母序排列参数 → HMAC-SHA1 签名 → Base64 编码。
     * 包含 appId、sessionId、utc 时间戳等鉴权字段。
     */
    private String buildAstUrl(String appId, String apiKey, String apiSecret, String sessionId) throws Exception {
        TreeMap<String, String> params = new TreeMap<>();
        params.put("appId", appId);
        params.put("accessKeyId", apiKey);
        params.put("audio_encode", "pcm_s16le");
        params.put("lang", "autodialect");
        params.put("samplerate", "16000");
        params.put("sessionId", sessionId);
        params.put("utc", OffsetDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ")));
        params.put("uuid", UUID.randomUUID().toString().replace("-", ""));

        String signTarget = buildCanonicalQuery(params);
        String signature = hmacSha1Base64(signTarget, apiSecret);
        params.put("signature", signature);

        return AST_WS_BASE_URL + "?" + buildCanonicalQuery(params);
    }

    private String buildCanonicalQuery(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> e : params.entrySet()) {
            if (!first) {
                sb.append("&");
            }
            first = false;
            sb.append(urlEncode(e.getKey())).append("=").append(urlEncode(e.getValue()));
        }
        return sb.toString();
    }

    private String urlEncode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (Exception ex) {
            throw new RuntimeException("URL encode failed", ex);
        }
    }

    private String hmacSha1Base64(String content, String secret) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA1");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA1"));
        byte[] signBytes = mac.doFinal(content.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(signBytes);
    }

    private byte[] copyOf(byte[] src, int len) {
        byte[] dst = new byte[len];
        System.arraycopy(src, 0, dst, 0, len);
        return dst;
    }

    private String extractAstText(JSONObject root) {
        JSONObject st = extractAstSt(root);
        if (st == null) {
            return "";
        }
        JSONArray rtArr = st.getJSONArray("rt");
        if (rtArr == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        JSONObject rt = rtArr.getJSONObject(0);
        if (rt == null) {
            return "";
        }

        JSONArray wsArr = rt.getJSONArray("ws");
        if (wsArr == null) {
            return "";
        }

        for (int j = 0; j < wsArr.size(); j++) {
            JSONObject ws = wsArr.getJSONObject(j);
            JSONArray cwArr = ws.getJSONArray("cw");
            if (cwArr == null || cwArr.isEmpty()) {
                continue;
            }

            JSONObject cw = cwArr.getJSONObject(0);
            if (cw == null) {
                continue;
            }
            String w = cw.getString("w");
            if (w != null) {
                sb.append(w);
            }
        }
        return sb.toString();
    }

    private boolean isAstFinal(JSONObject root) {
        JSONObject data = root.getJSONObject("data");
        if (data != null && Boolean.TRUE.equals(data.getBoolean("ls"))) {
            return true;
        }
        JSONObject st = extractAstSt(root);
        return st != null && Boolean.TRUE.equals(st.getBoolean("ls"));
    }

    /**
     * AST 转写结果组装器：将讯飞返回的增量片段（带 segmentId/pgs/rg/bg/ed）拼装成完整文本。
     * 支持三种操作模式：
     *   - rpl（替换）：按 rg 范围删除旧片段，插入新片段
     *   - apd（追加）：直接插入/覆盖指定 segmentId
     *   - 无 pgs：按 bg/ed 字符位置范围进行智能合并（基于文本相似度和范围重叠率）
     *
     * 最终输出分层文本：
     *   - fullText/displayText：当前完整拼接结果
     *   - committedText：已确认不变的部分（finalized 或 segmentId < 当前片段）
     *   - liveText：实时变化的部分（displayText - committedText），供前端高亮展示
     */
    private final class AstTranscriptionAssembler {
        private final TreeMap<Integer, SegmentState> segments = new TreeMap<>();

        private void apply(int segmentId,
                           String pgs,
                           int[] rg,
                           Integer bg,
                           Integer ed,
                           String text,
                           boolean finalized) {
            if (StrUtil.isBlank(pgs)) {
                applyAstSegmentWithoutPgs(segments, segmentId, bg, ed, text, finalized);
                return;
            }
            applyAstSegment(segments, segmentId, pgs, rg, text, finalized);
        }

        private String buildSnapshot() {
            return buildFinalResult(segments);
        }

        private String buildCommittedText(int activeSegmentId, boolean finalPacket) {
            if (finalPacket) {
                return buildSnapshot();
            }
            if (segments.isEmpty()) {
                return "";
            }

            StringBuilder committed = new StringBuilder();
            for (SegmentState segment : segments.values()) {
                if (segment == null || segment.text == null) {
                    continue;
                }
                if (segment.segId < activeSegmentId || segment.finalized) {
                    committed.append(segment.text);
                }
            }
            return committed.toString();
        }

        private String buildLiveText(String committedText,
                                     String displayText,
                                     String segmentText,
                                     boolean finalPacket) {
            if (finalPacket) {
                return "";
            }
            if (StrUtil.isBlank(displayText)) {
                return "";
            }
            String committed = committedText != null ? committedText : "";
            if (!committed.isEmpty() && displayText.startsWith(committed)) {
                return displayText.substring(committed.length());
            }
            return segmentText != null ? segmentText : displayText;
        }
    }

    private static final class SegmentState {
        private final int segId;
        private String text;
        private boolean finalized;
        private Integer bg;
        private Integer ed;
        private long updatedAt;

        private SegmentState(int segId) {
            this.segId = segId;
        }
    }

    private static void handleResultText(Text textObject, List<Text> resultSegments) {
        if (StringUtils.equals(textObject.getPgs(), "rpl")
                && textObject.getRg() != null
                && textObject.getRg().length == 2) {
            int start = textObject.getRg()[0] - 1;
            int end = textObject.getRg()[1] - 1;
            for (int i = start; i <= end && i < resultSegments.size(); i++) {
                resultSegments.get(i).setDeleted(true);
            }
        }
        resultSegments.add(textObject);
    }

    private static String getFinalResult(List<Text> resultSegments) {
        StringBuilder finalResult = new StringBuilder();
        for (Text text : resultSegments) {
            if (text != null && !text.isDeleted()) {
                finalResult.append(text.getText());
            }
        }
        return finalResult.toString();
    }

    private String normalize(String value) {
        return value == null ? null : value.trim();
    }

    private int lengthOf(String value) {
        return value == null ? 0 : value.length();
    }

    private String mask(String value) {
        if (StrUtil.isBlank(value)) {
            return "null";
        }
        if (value.length() <= 4) {
            return "****";
        }
        return value.substring(0, 2) + "****" + value.substring(value.length() - 2);
    }

    private void cleanupTempFile(Path tempFile) {
        if (tempFile == null || !Files.exists(tempFile)) {
            return;
        }
        try {
            Files.delete(tempFile);
        } catch (Exception ex) {
            tempFile.toFile().deleteOnExit();
            log.warn("Failed to delete temp file: {}", tempFile);
        }
    }

    /**
     * 回调接口：实时识别结果通过此接口推送给调用方。
     */
    public interface AudioResultCallback {
        void onResult(RealtimeTranscriptionUpdate result);
    }

    public record RealtimeTranscriptionUpdate(String fullText,
                                              String committedText,
                                              String liveText,
                                              String displayText,
                                              Integer revision,
                                              String resultStatus,
                                              Integer segmentId,
                                              String segmentText,
                                              String pgs,
                                              int[] rg,
                                              Integer bg,
                                              Integer ed,
                                              boolean finalPacket) {
    }
}

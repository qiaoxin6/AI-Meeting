# AI面试前后端对接说明（2026-03）

## 1. 推荐调用链路
1. 上传简历并抽题：`POST /api/xunzhi/v1/interview/sessions/{sessionId}/interview-questions`
2. 拉取当前应答题：`GET /api/xunzhi/v1/interview/sessions/{sessionId}/next-question`
3. 提交答案并拿下一题：
   文本答题推荐：`POST /api/xunzhi/v1/interview/sessions/{sessionId}/interview/answer-json`
   语音答题使用：`POST /api/xunzhi/v1/interview/sessions/{sessionId}/interview/answer`
4. 面试结束后落库：`POST /api/xunzhi/v1/interview/interview/record/save-from-redis/{sessionId}`

## 2. 答题接口请求参数

### 2.1 JSON文本答题（推荐）
接口：`POST /sessions/{sessionId}/interview/answer-json`

请求体示例：
```json
{
  "questionNumber": "1",
  "answerContent": "我会优先拆解问题边界，再给出可观测和回滚方案。",
  "agentId": 11,
  "interviewerAgentId": 12,
  "requestId": "ans_1742620001000"
}
```

字段说明：
1. `agentId`：评分官Agent ID（你当前是 11）
2. `interviewerAgentId`：提问官Agent ID（你当前是 12）
3. `requestId`：每次提交唯一，后端用它做幂等防重
4. `questionNumber`：可传可不传，后端最终以会话状态机为准

### 2.2 multipart语音答题
接口：`POST /sessions/{sessionId}/interview/answer`

表单字段：
1. `audioFile` 或 `answerContent` 二选一
2. `agentId`
3. `interviewerAgentId`
4. `requestId`
5. `questionNumber`（可选）

## 3. 答题接口响应字段（前端渲染核心）
```json
{
  "questionNumber": "1",
  "questionContent": "请介绍你最近一次性能优化项目",
  "score": 82,
  "totalScore": 164,
  "isSuccess": true,
  "errorMessage": null,
  "feedback": "问题拆解较好，但压测指标不完整",
  "nextQuestion": "请补充压测指标和瓶颈定位过程。",
  "nextQuestionNumber": "1",
  "isFollowUp": true,
  "followUpCount": 1,
  "finished": false
}
```

渲染规则：
1. `finished=true`：显示“面试完成”，禁用输入框
2. `isFollowUp=true`：前端标记“追问”，但题号用 `nextQuestionNumber`
3. `isFollowUp=false`：显示下一道主问题
4. `isSuccess=false`：弹出 `errorMessage`，并允许重试同一 `requestId` 之外的新请求

## 4. React调用示例（axios）
```ts
import axios from "axios";

type AnswerReq = {
  questionNumber?: string;
  answerContent?: string;
  agentId?: number;
  interviewerAgentId?: number;
  requestId: string;
};

export async function answerInterviewJson(sessionId: string, payload: AnswerReq) {
  const { data } = await axios.post(
    `/api/xunzhi/v1/interview/sessions/${sessionId}/interview/answer-json`,
    payload,
    { headers: { "Content-Type": "application/json" } }
  );
  return data?.data; // 统一Result封装下的业务体
}
```

## 5. 前端状态建议
1. 维护 `currentQuestion`、`isFollowUp`、`followUpCount`、`finished`
2. 每次提交时生成新 `requestId`（例如 `ans_${Date.now()}`）
3. 刷新页面时先调用 `next-question` 恢复当前题面
4. 结束后调用 `save-from-redis/{sessionId}` 固化记录

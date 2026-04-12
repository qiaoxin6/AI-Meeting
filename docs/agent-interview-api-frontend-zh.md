# Agent 面试后端接口文档（前端对接）

更新时间：2026-03-23

## 1. 基本信息
- Base URL：`/api/xunzhi/v1/interview`
- 鉴权：需要登录态（`@CurrentUser`），并且大多数接口会校验 `sessionId` 是否属于当前用户。
- 统一返回结构：

```json
{
  "code": "0",
  "message": null,
  "data": {},
  "requestId": null
}
```

说明：`code="0"` 表示成功，非 `0` 表示失败。

## 2. 前端推荐调用链路
1. 上传简历并生成面试题。
2. 拉取当前应答题（首题或断点恢复）。
3. 提交回答并获取下一题（主问题或追问）。
4. 面试结束后保存面试记录。
5. 需要图表时拉取雷达图（该接口也会尝试自动落库）。

## 3. 会话与消息（可选）
### 3.1 分页会话列表
- 方法：`GET /conversations`
- Query：`current`、`size` 等（见 `AgentConversationPageReqDTO`）

### 3.2 会话消息历史
- 方法：`GET /conversations/{sessionId}/messages`

### 3.3 结束会话
- 方法：`PUT /conversations/{sessionId}/end`

## 4. 核心面试流程接口

### 4.1 上传简历 + 生成题目
- 方法：`POST /sessions/{sessionId}/interview-questions`
- Content-Type：`multipart/form-data`
- 表单字段：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `agentId` | Long | 是 | 抽题 Agent ID |
| `resumePdf` | File | 是 | 简历 PDF 文件 |

返回 `data` 类型：`InterviewQuestionRespDTO`

关键字段：
- `sessionId`
- `questions`：`Map<String,String>`，例如 `{ "1": "...", "2": "..." }`
- `suggestions`：`Map<String,String>`
- `interviewType`
- `resumeScore`
- `questionCount`
- `suggestionCount`
- `isSuccess`（1 成功，0 失败）
- `errorMessage`

示例响应：
```json
{
  "code": "0",
  "data": {
    "sessionId": "sess_001",
    "agentId": 1345345,
    "questions": {
      "1": "请介绍一下你最近做过的缓存优化项目",
      "2": "说说你如何处理高并发下的数据一致性"
    },
    "suggestions": {
      "1": "回答时补充量化指标",
      "2": "突出故障排查思路"
    },
    "interviewType": "backend",
    "resumeScore": 82,
    "questionCount": 2,
    "suggestionCount": 2,
    "isSuccess": 1,
    "errorMessage": null
  }
}
```

### 4.2 拉取当前应答题（推题/断点恢复）
- 方法：`GET /sessions/{sessionId}/next-question`
- 用途：
- 首次抽题后获取当前题。
- 页面刷新后恢复当前应答位置。
- 重复提交被幂等拦截时获取当前状态。

返回 `data` 类型：`InterviewAnswerRespDTO`。

### 4.3 提交回答（JSON，纯文本推荐）
- 方法：`POST /sessions/{sessionId}/interview/answer-json`
- Content-Type：`application/json`
- Body 字段：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `questionNumber` | String | 否 | 可传可不传，后端以流程状态为准 |
| `answerContent` | String | 建议必填 | 回答文本 |
| `agentId` | Long | 否 | 评分官 Agent ID，默认 1345345 |
| `interviewerAgentId` | Long | 否 | 提问官 Agent ID，默认同 `agentId` |
| `requestId` | String | 强烈建议 | 幂等键，避免重复计分 |

请求示例：
```json
{
  "questionNumber": "1",
  "answerContent": "我会先做容量评估，再做压测和降级方案设计。",
  "agentId": 1345345,
  "interviewerAgentId": 1345345,
  "requestId": "ans_1742620001000"
}
```

### 4.4 提交回答（multipart，语音/混合）
- 方法：`POST /sessions/{sessionId}/interview/answer`
- Content-Type：`multipart/form-data`
- 表单字段：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `questionNumber` | String | 否 | 同上 |
| `answerContent` | String | 否 | 与 `audioFile` 二选一 |
| `audioFile` | File | 否 | 与 `answerContent` 二选一 |
| `agentId` | Long | 否 | 同上 |
| `interviewerAgentId` | Long | 否 | 同上 |
| `requestId` | String | 强烈建议 | 同上 |

### 4.5 回答接口返回字段（`InterviewAnswerRespDTO`）

| 字段 | 类型 | 说明 |
|---|---|---|
| `questionNumber` | String | 本次作答对应主问题题号 |
| `questionContent` | String | 本次作答题目内容 |
| `score` | Integer | 本题得分 |
| `totalScore` | Integer | 当前累计得分 |
| `feedback` | String | 评分反馈 |
| `nextQuestion` | String | 下一题内容（主问题或追问） |
| `nextQuestionNumber` | String | 下一题题号（追问场景可能仍是当前主问题号） |
| `isFollowUp` | Boolean | 下一题是否追问 |
| `followUpCount` | Integer | 当前主问题追问次数 |
| `finished` | Boolean | 面试是否结束 |
| `isSuccess` | Boolean | 业务处理是否成功 |
| `errorMessage` | String | 失败原因 |

成功示例：
```json
{
  "code": "0",
  "data": {
    "questionNumber": "1",
    "questionContent": "请介绍一下你最近做过的缓存优化项目",
    "score": 85,
    "totalScore": 85,
    "feedback": "核心思路正确，但缺少异常回退策略。",
    "nextQuestion": "如果 Redis 故障，你的降级和一致性保障方案是什么？",
    "nextQuestionNumber": "1",
    "isFollowUp": true,
    "followUpCount": 1,
    "finished": false,
    "isSuccess": true,
    "errorMessage": null
  }
}
```

## 5. 面试结果与记录

### 5.1 获取面试题缓存
- 方法：`GET /sessions/{sessionId}/interview/questions`
- 返回：`Map<String,String>`

### 5.2 获取累计面试分
- 方法：`GET /sessions/{sessionId}/interview/score`
- 返回：`Integer`

### 5.3 获取面试建议
- 方法：`GET /sessions/{sessionId}/interview/suggestions`
- 返回：`Map<String,String>`

### 5.4 获取简历分
- 方法：`GET /sessions/{sessionId}/resume/score`
- 返回：`Integer`

### 5.5 雷达图数据
- 方法：`GET /sessions/{sessionId}/radar-chart`
- 返回：`RadarChartDTO`
- 字段：`resumeScore`、`interviewPerformance`、`demeanorEvaluation`、`professionalSkills`、`potentialIndex`
- 备注：该接口内部会尝试调用 `saveInterviewRecordFromRedis(sessionId)` 自动落库一次。

### 5.6 保存面试记录（推荐）
- 方法：`POST /interview/record/save-from-redis/{sessionId}`
- 说明：从 Redis/会话缓存汇总并保存记录，前端结束页建议调用一次。

### 5.7 手动保存面试记录
- 方法：`POST /interview/record`
- Body：`InterviewRecordSaveReqDTO`

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `sessionId` | String | 是 | 会话 ID |
| `interviewScore` | Integer | 否 | 不传则取缓存总分 |
| `interviewSuggestions` | String | 否 | 不传则取缓存建议 |
| `interviewDirection` | String | 否 | 不传则取缓存方向 |
| `username` | String | 否 | 当前实现以登录用户为准 |

### 5.8 查询记录详情
- 方法：`GET /interview/record/{sessionId}`
- 返回：`InterviewRecordRespDTO`

### 5.9 分页查询记录
- 方法：`GET /interview/records`
- Query：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `pageNum` | Integer | 否 | 1 | 页码 |
| `pageSize` | Integer | 否 | 10 | 每页条数 |
| `sessionId` | String | 否 | - | 按会话筛选 |
| `minScore` | Integer | 否 | - | 最低分 |
| `maxScore` | Integer | 否 | - | 最高分 |
| `interviewDirection` | String | 否 | - | 面试方向 |

## 6. 神态评估（可选）
- 方法：`POST /sessions/{sessionId}/demeanor-evaluation`
- Content-Type：`multipart/form-data`
- 表单字段：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `userPhoto` | File | 是 | 用户照片 |
| `agentId` | Long | 是 | 神态评估 Agent ID |
| `sessionId` | String | 否 | 若传，必须与 path 中 `sessionId` 一致 |

## 7. 前端落地建议
1. 每次回答都生成新的 `requestId`（如 `ans_${Date.now()}`）以防重复计分。
2. 页面刷新后先调 `GET next-question` 恢复题面，再允许提交。
3. 若 `finished=true`，禁用输入并调用 `save-from-redis` 固化记录。
4. 面试结束页可再拉一次 `radar-chart` 用于展示。

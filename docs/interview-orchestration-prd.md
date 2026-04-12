# AI面试流程改造 PRD（双Agent + 后端状态机）

## 1. 背景与问题

当前面试流程已具备：
- 简历抽题并缓存到 Redis；
- 用户答题后调用 AI 评分；
- 面试结果写入记录表。

当前主要问题：
- 流程状态（问到第几题、是否追问）缺少统一状态机；
- 评分结果与下一题决策耦合不清晰，容易重复加分、乱序答题；
- `sessionId` 来源存在混用风险（路径参数与请求参数）；
- 外部 Agent 职责未分离（提问与评分混在一个流里时稳定性差）；
- 前端需要“优雅保存”，但后端缺幂等与补偿支撑。

## 2. 目标

### 2.1 业务目标
- 支持“10道主问题按顺序进行”，每题支持 0~N 次追问（默认最多2次）。
- 评分由“评分官 Agent”输出结构化结果，提问由“提问官 Agent”生成下一问。
- 后端成为流程唯一编排者（前端不参与流程决策，只负责展示与提交答案）。
- 保证面试过程可恢复、可重试、可落库。

### 2.2 工程目标
- 建立可扩展状态机，后续可支持难度自适应与技能点强化。
- 统一输出 JSON 协议，降低模型输出漂移对业务的影响。
- 关键操作幂等化，避免重复加分与脏数据。

## 3. 非目标

- 本期不做复杂多轮对话记忆（仅围绕当前题目/追问）。
- 本期不做多模型A/B策略与在线学习。
- 本期不重构全部历史接口，仅做兼容性增强与关键链路改造。

## 4. 角色与职责

- 提问官 Agent（Interviewer）
  - 输入：当前题目、评审缺口、追问次数、面试方向。
  - 输出：下一条提问（主问题或追问）。
- 评分官 Agent（Evaluator）
  - 输入：题目 + 用户答案 + 评分规则。
  - 输出：结构化评分、逻辑判定、缺失点、是否建议追问。
- 后端编排器（Interview Orchestrator）
  - 维护状态机、调用两个 Agent、写 Redis、累计分数、落库面试记录。

## 5. 流程设计（状态机）

状态：
- `INIT`：会话初始化
- `ASKING`：已下发问题，等待用户回答
- `EVALUATING`：评分中
- `FOLLOW_UP`：追问中
- `COMPLETED`：题目结束
- `SAVED`：记录已落库

状态迁移：
1. INIT -> ASKING（下发第1题）
2. ASKING -> EVALUATING（收到回答）
3. EVALUATING -> FOLLOW_UP（需要追问且未达上限）
4. EVALUATING -> ASKING（进入下一主问题）
5. EVALUATING -> COMPLETED（主问题全部结束）
6. COMPLETED -> SAVED（记录落库）

## 6. 数据结构设计（Redis）

键设计：
- `interview:flow:session:{sessionId}`（Hash）
  - `status`
  - `currentIndex`（当前主问题下标，从0开始）
  - `currentQuestionId`
  - `followUpCount`
  - `maxFollowUp`（默认2）
  - `version`（乐观并发控制）

- `interview:questions:session:{sessionId}`（Hash，已有）
  - `1` -> `题目内容`
  - `2` -> `题目内容`
  - ...

- `interview:score:session:{sessionId}`（String，已有）
  - 累计分数

- `interview:turns:session:{sessionId}`（List/Stream，新）
  - 每轮问答日志（questionId、answer、score、followUp、timestamp）

TTL：与现有缓存一致，默认 24h。

## 7. Agent 输入输出协议

### 7.1 评分官输出（严格JSON）
```json
{
  "score": 78,
  "logic_ok": true,
  "missing_points": ["事务隔离级别细节"],
  "follow_up_needed": true,
  "feedback": "关键点覆盖不完整"
}
```

### 7.2 提问官输出（严格JSON）
```json
{
  "ask_to_user": "你提到MVCC，请具体说明RR下如何避免幻读？",
  "is_follow_up": true,
  "target_skill": "mysql",
  "difficulty": "MEDIUM"
}
```

## 8. 接口改造

## 8.1 复用现有接口（主链路）
- `POST /api/xunzhi/v1/interview/sessions/{sessionId}/interview/answer`
  - 改造点：
    - 统一以路径 `sessionId` 为准；
    - 后端从状态机获取当前题，不再完全信任前端传入题号；
    - 响应新增：
      - `nextQuestion`
      - `isFollowUp`
      - `finished`
      - `followUpCount`

## 8.2 新增建议接口（可选）
- `GET /api/xunzhi/v1/interview/sessions/{sessionId}/next-question`
  - 用于断线恢复时拉取当前应答题。

## 8.3 保存接口
- 继续使用：
  - `POST /api/xunzhi/v1/interview/interview/record`
  - `POST /api/xunzhi/v1/interview/interview/record/save-from-redis/{sessionId}`
- 增强：
  - 幂等（sessionId + requestId）
  - 多次调用只更新不重复累积

## 9. 后端改造清单（按模块）

1. `AgentPropertiesLoader`
- 由“启动加载Top10”改为“支持按agentId按需查询/缓存回源”。

2. `InterviewQuestionCacheService` + `InterviewQuestionCacheServiceImpl`
- 新增流程状态机读写方法。
- 新增回合日志写入方法。

3. `AgentMessageServiceImpl#answerInterviewQuestion`
- 重构为编排入口：
  - 读取当前题 -> 评分官调用 -> 追问/下一题决策 -> 返回下一问。
- 加入幂等防重（requestId）。

4. `InterviewController#answerInterviewQuestion`
- 统一 sessionId 来源；
- DTO 增加 requestId（可选但建议）。

5. `InterviewRecordServiceImpl`
- 兼容读取流程状态与回合日志生成快照。

## 10. 前端改造要求

- 前端只负责：
  - 展示 `nextQuestion`；
  - 提交 `answer + requestId`；
  - 完成后触发保存。
- 不再自行决定“当前第几题/是否追问”。
- 异常兜底：刷新后调用 `next-question` 恢复。

## 11. 验收标准（DoD）

功能验收：
1. 能按顺序完成 10 道主问题；
2. 每题最多 2 次追问；
3. 答案重复提交不重复加分；
4. 会话中断后可恢复到当前题；
5. 完成后可稳定落库，记录与Redis一致。

质量验收：
1. 关键服务单元测试覆盖状态迁移；
2. 接口回归测试通过；
3. 异常日志可定位（sessionId/requestId）。

## 12. 实施里程碑

### M1（当前迭代）
- 落地 Redis 状态机；
- 改造 answer 接口主链路；
- 接入评分官结构化输出。

### M2
- 接入提问官追问生成；
- 增加 next-question 恢复接口；
- 完成前端联调。

### M3
- 增强记录快照与评估报表；
- 补齐自动化测试与压测。

## 13. 风险与对策

- 风险：模型输出不稳定导致JSON解析失败
  - 对策：严格JSON提示词 + 解析失败重试 + 降级默认结果。

- 风险：并发提交导致状态错乱
  - 对策：requestId 幂等 + version 乐观锁 + 原子更新。

- 风险：Agent配置查不到
  - 对策：agentId回源查询 + 启动预热仅做优化不做强依赖。

# ReAct + MCP 集成说明（当前项目）

## 已完成的代码改造

1. 引入 `spring-ai-starter-mcp-client` 依赖。
2. `UniversalAiChatHandler` 已支持注入 `ToolCallbackProvider`，并在每次请求里动态挂载工具回调：
   - `requestSpec.toolCallbacks(...)`
   - `requestSpec.toolContext(...)`
3. OpenAI/DeepSeek 选项已启用 `internalToolExecutionEnabled(true)`，支持模型自动执行“工具调用 -> 观察 -> 继续回答”回路。

## 如何启用

1. 打开 `admin/src/main/resources/application.yaml`。
2. 在 `spring.ai.mcp.client` 下配置你需要的 MCP 连接（stdio 或 sse）。
3. 将 `spring.ai.mcp.client.enabled` 从 `false` 改为 `true`。
4. 重启服务。

## 验证方式

1. 发起一个必须依赖工具的数据查询问题（例如“读取项目目录结构并总结”）。
2. 观察响应内容是否体现工具结果（不是纯模型臆测）。
3. 检查服务日志，确认没有 MCP 连接失败或工具执行异常。

## 常见问题

1. 没有触发工具调用：
   - 问题本身不需要外部信息；
   - MCP 工具没有正确注册；
   - 模型或提示词没有足够明确要求“必要时调用工具”。
2. 服务启动报错：
   - `npx` 不可用；
   - MCP server 命令路径错误；
   - 配置的 `sse` 地址不可达。

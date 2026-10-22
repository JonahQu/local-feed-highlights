# Local Feed Highlights

面向本地消息 feed 的有界摘要工作流：prepare 读取增量，人工或外部调用方撰写摘要，commit 校验来源并先保存后确认游标。支持批次哈希、重复提交恢复、版本历史和未读取正文的覆盖说明。

## 使用

Python 3.9+，macOS / Linux。此项目是 feed 消费者，不包含微信采集器，也不能单独登录或读取微信。

```sh
cp config.example.json config.json
python3 highlights.py init
python3 highlights.py status
python3 highlights.py prepare
# 根据返回批次制作 summary.json，格式见下文
python3 highlights.py commit --input summary.json
python3 highlights.py read --after-version 0 --limit 5
```

编辑 config.json：feed 为已有可执行 feed 的绝对路径；account_tag 为你期望的账户标识；conversations 只填写你希望读取正文的会话 ID。空映射只记录元数据。init 从当前时间建立基线；需要历史消息时设置 start_message_time（Unix 秒数）。日间窗口按 timezone 运行。

摘要格式：`{"batch_id":"来自prepare","highlights":[{"source_seqs":[1],"fact":"有来源的事实"}],"collection_note":"覆盖范围及未读正文说明"}`，最多 5 条，最多 5000 字符。summary.json 是本地私人文件，不要加入仓库。

## Feed 接口

可执行文件接收 status / read / get / ack 子命令并返回 JSON。read 支持 --consumer、--limit、--max-chars、--per-message-chars、--metadata-only，返回 batch、payload_sha256、has_changes。batch 包含 batch_id、after_seq、through_seq、events；SHA-256 基于 sort_keys=True、ensure_ascii=False、紧凑分隔符的 JSON。event 包含 seq、conversation、message_time、has_text 等。get 按 --seq 返回正文及一致的会话/时间。ack 接收 --batch-id、--sha256 和 consumer；必须幂等。status 包含 account_tag、collection_state、collection_stale、fault_present、has_changes。适配器须自行实现此契约。

## 状态

0.1.0 适配器预发布。运行数据在 runtime/；首次发布不附真实 feed、聊天、账号指纹或会话名单。未做端到端微信连接测试；仅声明已验证的本地摘要保存和恢复行为。

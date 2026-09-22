# 分片日志汇合

这是一个无第三方依赖的 Python 3.9+ 实现，核心在 [`segment_log.py`](segment_log.py)，固定数据测试在 [`test_segment_log.py`](test_segment_log.py)。

运行：

```bash
python3 -m unittest -v
```

## 语义摘要

- 片段携带来源、序号、负载、时间戳、协议版本、校验算法和 SHA-256 校验值。
- 只有从 `confirmed_seq + 1` 开始的连续片段才会确认；乱序片段进入待补缺口缓存，后续内容不会伪装成完整结果。
- 重复且字节一致的片段返回 `duplicate=true`；同序号内容不一致时保留两份证据并返回 `SEQUENCE_CONTENT_CONFLICT`，不会覆盖旧内容。
- 校验同时覆盖序号边界、非空/最大长度、SHA-256、来源状态、协议和校验算法。异常包含 `source`、`seq`、`stage` 和相对起始位置的 `offset`。
- 确认片段按完整批次原子写入 `batches/batch-*.json`；`.tmp` 被视为半截批次，恢复时只登记、不提交。
- 读取、回放和统计都返回快照，不改变合并状态；同一请求重复执行得到相同边界和确认号。
- 响应同时包含 `confirmed_seq`、`fragment_count` 和缺口状态；存在缺口或未结束时 `ended=false`，并返回可继续接收的序号。
- 来源暂停、替换、序号跨越和过期会分开返回可继续处理范围与人工处理范围。
- 按来源回放只遍历指定来源，不影响其他来源的确认位置和输出顺序。
- 协议迁移先把旧来源标记为 `replaced`、新来源标记为 `migrating`；完整记录迁移批次落盘后，新来源才变为 `active`。
- 所有等待时长由调用方传入确定性的 `timestamp_ns` / `now_ns`，实现和测试都不读取机器当前时间。

## 存储格式

每个批次只允许一个来源、一个协议版本和一种校验算法。批次 JSON 记录：

- `storage_version`
- `batch_id`、`source`
- `protocol_version`、`checksum_algorithm`
- Base64 片段数组
- 针对片段数组的整批 `batch_checksum`

`manifest.json` 记录来源状态、边界和容量配置。恢复时先识别 `.tmp`，再按批次号加载校验通过的完整批次。

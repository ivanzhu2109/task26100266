# VaultChat 消息搜索库

由上层直接调用的 Python 库：消息、频道成员关系与必要历史保存在 SQLite，
提供四类能力——写入消息状态、变更成员关系、开启搜索、继续翻页。
单进程多线程安全；搜索会话仅驻留内存，不跨重启保存。

## 接口

```python
from vaultchat import VaultChatStore

store = VaultChatStore("vaultchat.db")   # 或 ":memory:"

# 写入消息状态（调用方保证编号全局唯一、业务校验已完成）
store.publish(msg_id, channel_id, sent_at, body, terms)
store.edit(msg_id, new_body, new_terms)  # 产生新版本；频道/编号/发送时刻不可改
store.recall(msg_id)                     # 不可撤销
store.bulk_publish(iter_of_tuples)       # 批量/迟到导入（单事务）

# 变更成员关系
store.add_member(user_id, channel_id)
store.remove_member(user_id, channel_id)

# 开启搜索（首屏）与继续翻页
page = store.search(user_id, terms, limit=50)
page = store.next_page(page.session_id)
# page.items: tuple[Message(msg_id, channel_id, sent_at, body)]
# page.has_more / page.cursor
```

## 语义

- **冻结**：`search` 建立会话时冻结 (a) 全局写序号 `freeze_seq`、
  (b) 用户当时所属频道集合、(c) 以当时最新版本词项计算的匹配位图
  （bytearray，每会话约为消息数/8 字节）。之后发布、编辑、迟到导入的
  消息都不会进入会话；展示正文取自 `versions` 中 `seq <= freeze_seq`
  的最新版本。
- **动态可见性**：每次翻页在同一把锁内完成，对应一个完整状态——
  应用当前的频道成员关系（与冻结频道集合求交）与当前召回状态。
  已退出频道、已召回的消息立即隐藏；空页不推进游标，重新加入后
  未越过游标的消息可再次出现；已越过的消息不补发。
- **排序与翻页**：`(sent_at, msg_id)` 降序，游标前进，每页最多 50 条；
  不可见消息不占页容量，扫描持续直到凑满一页或数据耗尽。

## 存储

- `messages(msg_id, dense, channel_id, sent_at, created_seq, recalled)`
  + `(sent_at, msg_id)` 索引（翻页扫描）
- `versions(msg_id, version, seq, body)`（WITHOUT ROWID，历史版本）
- `current_terms(term, dense)`（WITHOUT ROWID，当前版本词项倒排）
- `membership(user_id, channel_id)`
- 会话状态（位图、冻结频道、游标）只在内存。

## 测试

```bash
python3 -m unittest discover tests          # 全部（含规模测试）
python3 -m unittest tests.test_correctness  # 正确性（含随机差分）
python3 -m unittest tests.test_concurrency  # 多线程交错
python3 tests/test_scale.py                 # 规模测试（可用 SCALE_MESSAGES /
                                            #  SCALE_SESSIONS 调小）
```

正确性测试用一个独立实现的"首屏快照"参考模型与库逐页对比，覆盖：
同一发送时刻大量消息、迟到发布、搜索中途编辑、撤权/重新加入/召回
造成的连续大段不可见（页面补足、正文版本正确、已越过项不补发）、
成员变更/召回/翻页交错（每页对应一个完整状态，调用开始前已失效的
访问资格立即生效）。

## 规模测试实测（300k 消息、200 会话、每会话约 14.7 万初始命中）

| 指标 | 实测 | 要求 |
| --- | --- | --- |
| 首屏 p95 | ~124 ms | ≤ 300 ms |
| 翻页 p95 | ~1 ms | ≤ 300 ms |
| 200 会话常驻内存增量 | ~7.7 MiB | ≤ 32 MiB |
| 会话落盘增量 | 0（会话不落盘） | — |

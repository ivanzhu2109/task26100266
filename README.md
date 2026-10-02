# VaultChat 消息搜索库

由上层直接调用的 Python 库：消息、频道成员关系与版本历史保存在 SQLite，
提供四类能力——写入消息状态、变更成员关系、开启搜索、继续翻页。

## 用法

```python
from vaultchat import VaultChatSearch

vc = VaultChatSearch("vaultchat.db")

# 变更成员关系
vc.join_channel("alice", "general")

# 写入消息状态（编号全局唯一；词项需已规范化；编辑不可改频道/编号/发送时刻）
vc.post_message(1, "general", 1717000000, "hello world", ["hello", "world"])
vc.post_messages([(2, "general", 1717000001, "hi", ["hi"])])   # 批量/迟到导入
vc.edit_message(1, "hello moon", ["hello", "moon"])         # 产生新版本
vc.recall_message(2)                                        # 不可撤销

# 开启搜索：冻结此刻的成员频道与每条未召回消息的最新版本，返回首屏
page = vc.open_search("alice", ["hello"])
# 继续翻页：游标由会话内部携带；每页应用当前的成员关系与召回状态
while page.has_more:
    page = vc.next_page(page.session_id)
vc.close_search(page.session_id)
```

每页最多 50 条，按发送时刻降序、同时刻按消息编号降序。单进程多线程安全：
写操作串行提交，每次翻页在独立的读事务快照中完成，因此每页结果都对应
调用开始至返回之间的某一个完整状态。

## 设计要点

- 全局单调序号 `seq`：每条消息、每个版本在写入时取序号。会话冻结打开时
  的 `seq = S`，匹配与正文展示都使用 `seq <= S` 的最新版本
  （`term_intervals` 记录每个词项在哪个序号区间内属于某条消息），
  迟到导入（`created_seq > S`）与后续编辑自然被排除。
- 会话只保存：查询词项、冻结频道集合、`S`、游标——200 个会话的状态增量
  远小于 32 MiB；召回与成员关系不冻结，每次翻页在读事务中实时求交。
- 游标是全局 `(sent_at, msg_id)` 水位，只前进到最后一条返回的命中；
  已越过的消息不因恢复权限而补发，未越过的消息在重新加入频道后可出现。

## 测试

```bash
python3 -m unittest discover -s tests   # 正确性：排序/冻结/撤权/召回/交错一致性
python3 tests/scale_test.py             # 规模测试：30 万消息、200 会话
VC_SCALE_QUICK=1 python3 tests/scale_test.py   # 快速冒烟
```

规模测试报告首屏与翻页的 p50/p95/max 耗时，以及 200 个会话带来的
内存与落盘状态增量（阈值：p95 ≤ 300ms，增量 ≤ 32 MiB）。

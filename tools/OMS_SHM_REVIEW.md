# `include/oms/OmsShm.h` + `tb/tools` 复查

> **修订记录**
> - 2026-10-05 16:08 — 首版：指出两个硬编译错误。
> - 2026-10-05 16:18 — **P0-1 / P0-2 已由使用者修复，本机复验通过**（见 §1）。
>   本文档改为**唯一的一份复查记录**，并补上"这个库的**实现**到底有没有问题"的评估（§2–§4）。
> - 2026-10-05 16:31 — **撤回 P1-3（`orderId` 变更泄漏）。** 使用者指出
>   `orderId` 只会在"本地创建时还没有 → 交易所首次回报时带上"这一步发生一次变化，
>   此后不再修改。复核代码 + 重跑真实生命周期测试，确认**没有泄漏**，是我上一版的误判。
>   详见 **§2.1**。
> - 2026-10-05 16:43 — **P1-1 补丁写好并实测通过**（7 hunk / 134 行），
>   文件 `tb/tools/OmsShm_P1-1.patch`，`--dry-run` 验证 clean apply。
> - 2026-10-05 16:54 — **P1-1 补丁已应用到 `include/oms/OmsShm.h`**（44788 → 47794 B，
>   980 → 1033 行）。应用后在**真实文件**上重跑了全部 4 项验证，结果与副本一致。
>   原始文件备份在 `/tmp/omsshm_fix/OmsShm.h.pristine-20261005-1654`；
>   撤销：`patch -R -p1 < tb/tools/OmsShm_P1-1.patch`（已验证可反向应用）。
> - 2026-10-06 13:12 — 新增 **§0.1 待改清单**（按当前 1033 行版本重算全部行号）
>   与 **§0.2 已排除的疑点**。新查出一条：**索引占用完全没有可观测性**
>   （`Stats` 里没有索引字段，`oms_query --stats` 也看不到）—— 也就是刚修的 P1-1
>   那个失败模式**仍然从工具里看不见**。同时**更正 P2-3 #1**：那不是 fd 泄漏。
>   另实测排除了"tombstone 无限累积"的疑点（§5 `tombstone.cpp`）。
> - 2026-10-06 13:34 — **A1 改法已在 `/tmp` 副本上验证完毕**，补丁
>   `tb/tools/OmsShm_A1.patch`（89 行 / 2 文件），并补 **§0.3** 记录改法、
>   设计要点、以及一个**对照实验**：把 A1 扫描加在未修 P1-1 的旧逻辑上，
>   可一眼看出「索引 99% 为空却疯狂 `alloc_failures`」。
>   **仓库源码未动**（`OmsShm.h` md5 仍为 `ab581695db773b3129f169b369bfd9ea`）。
> - 2026-10-06 15:54 — **A1 补丁已应用**（`OmsShm.h` 1033 → **1062 行**，
>   `oms_query.cpp` 236 → **262 行**）。在**真实文件**上重跑了低占用 / 饱和稳态 /
>   P1-1 回归 / 多容量回归四组验证，全部通过。详见 §0.3。
>   备份：`/tmp/omsshm_a1/*.pristine-20261006-1553`；
>   撤销：`patch -R -p1 < tb/tools/OmsShm_A1.patch`。
> - 2026-10-06 16:20 — **A4 已应用**（`OmsShm.h` 1062 → **1101 行**，`oms_query.cpp` 262 → **267 行**，
>   `oms_shm.sh` 180 → **182 行**）。回滚日志从"误导"改成"自诊断"。
>   **顺带修掉一个 A1 引入的严重回归**：A1 给 `total_alloc_failures` 行尾挂了中文提示，
>   导致 `oms_shm.sh doctor` 的 `awk '{print $NF}'` 解析失败 —— 实测真值 828 时
>   `doctor` 输出 `✓ healthy`。两处都修了（数值行不再挂提示 + 解析改为按 key 锚定）。
>   详见 **§0.4**。
> - 2026-10-06 16:34 — 新增 **§2.3 / B5**：索引的"身份判定"用 `hash + 前 16 字节`，
>   **实测发现写入路径把它当身份用，且 `update_slot` 不更新 key 副本 → slot 自相矛盾
>   → `lookup` 返回错单**（用 A 的 key 查回 B 的报单体）。
>   同时实测：前 16 字节前缀对 `orderId`（100% 重复）和长 `strategyId` 的复合
>   `clientOrderId`（完全退化）**不起区分作用**。
> - 2026-10-06 17:05 — **B5 已修复并应用**（`OmsShm.h` 1101 → **1265 行**）。
>   补丁 `tb/tools/OmsShm_B5.patch`（7 个 hunk / 295 行，`patch -p1` 干净，
>   已用"反向应用 + 前向应用"验证能逐字节还原当前文件）。
>   核心：**索引命中从"身份判定"降级为"候选"，真判据改成回读 slot 比完整 key**；
>   并让 `update_slot` 维护 "key 副本 == 报单体" 这个不变量（冷路径）。
>   实测（16 位 hash 副本，代码路径与生产逐字节相同）：4000 个 key、
>   **314 个落在碰撞桶**里，修复前 **158 张单被并掉、156 次查错单**，
>   修复后 **0 并单 / 0 查错 / 0 查不到**。
>   性能：lookup 35.2 → **35.0 ns/op**（无变化）；update 40 → **52 ns/op**
>   （+12ns，原因已逐项定位，见 §2.4）。
>   备份：`/tmp/omsshm_b5/OmsShm.h.pristine-20261006-1652`；
>   撤销：`patch -R -p1 < tb/tools/OmsShm_B5.patch`。
> - 2026-10-06 17:25 — 新增 **§2.5 / B6**（使用者提问"`kMaxProbeIndex=32` 需要改吗"）：
>   **索引稳态必然 100% 占满，插入成功全靠 32 步内碰到 tombstone。**
>   实测最长 live 连续段 **33~56 > 32**，`slot_cap=65536` 时 **388/203 万单被丢**
>   （**1.9e-4，1/5235**），且随 churn 时间恶化到 ~2.5e-4。
>   对照两个改法：**改索引为 4N 治本**（最长连 13，丢单 0，只花 24 MiB 内存，
>   速度不变），只调限值治标。**仓库源码未动**。
> - 2026-10-06 18:05 — **B6 已应用**（使用者："好的"）。索引容量 2N → `next_pow2(4N)`，
>   `kVersion` 2 → 3（老文件必须删掉重建），`slot_cap` 上界收到 2^28，
>   `open()` 加倍数校验，`Stats` 暴露 `max_live_run` / `probe_max`，
>   `oms_query --stats` / `oms_shm.sh doctor` 都能看见探测余量。
>   实测 `slot_cap=65536` / 30 轮：最长连续段 **38 → 13**、丢单 **388 → 0**、
>   平均查询探测 **2.45 → 1.44**；SHM 121.7 → **145.7 MiB**；update/lookup 耗时不变。
>   6 个文件：`OmsShm.h` 1265 → **1338 行**、`oms_query.cpp` 267 → **287**、
>   `oms_shm.sh` 182 → **199**、`oms_bench.cpp` / `oms_demo.cpp`（`--help` 措辞）、
>   `include/oms/README.md`（文档）。补丁 `tb/tools/OmsShm_B6.patch`
>   （25 个 hunk / 522 行，`patch -p1` 干净，正反向都验过）。
>   详见 §2.6。**运维动作：删掉现有 shm 文件重建。**
> - 2026-10-06 18:45 — **v4 已应用**（使用者："好的，3和2修改下吧"）。评估使用者提的三个
>   "快路径 + 兜底"提案：**③ `alloc_slot()` 128 步窗口 + 全表兜底 → 改**（实测正在丢单，
>   1/1024 ~ 1/140，且全部是"窗口空但全表有"）；**② `sync_one_key()` 检查
>   `insert_index()` 返回值 → 改**（旧顺序失败时**新旧 key 都查不到**，实测 320 次改 key
>   破坏不变量 32 次 → 0 次）；**① index 32 步 + 全表兜底 → 评估后不改**
>   （B6 已把余量做到 2.0~5.3×，加兜底会把唯一的过载信号静音）。
>   `OmsShm.h` 1338 → **1471 行**、`oms_query.cpp` 287 → **308**、`oms_shm.sh` 199 → **229**、
>   README 883 → **949**。补丁 `tb/tools/OmsShm_v4.patch`（15 hunk / 402 行，正反向都验过）。
>   **新增 3 个计数**（塞进保留 pad，`sizeof` 仍 4096 → **不需要再 bump `kVersion`**），
>   `doctor` 新增三个分支。顺带补掉 **C1**（`reset_all` 漏清 `total_stale_live_reclaims`）。
>   详见 §2.7。**运维动作：无**（v4 没改 shm 布局）。

复查对象：

- `include/oms/OmsShm.h`（当前 **1471 行**）— header-only OMS 订单 SHM 环形数组，
  计划替代 `tb` 里缓存报单的三个 `tbb::concurrent_unordered_map`。
- `tb/tools/`：`oms_bench.cpp` / `oms_demo.cpp` / `oms_query.cpp` / `oms_shm.sh`

> ⚠️ **文件位置**：被复查的文件在 **`hft/include/oms/OmsShm.h`（工作区根，不是 git 仓库）**。
> `tb/include/oms/` 是**另一个目录**，只有 `AccountManager.h` / `OrderManager.h`，**没有** `OmsShm.h`。

---

## 0. 结论（先说答案）

**实现本身的设计是合理的，代码质量也不差**——分层清楚、注释到位、写者侧的单写者契约、
reclaim 的 CAS 抢槽、reader 侧的 key 复核，这些都是对的。

**P1-1（唯一会让实盘静默丢单的那个）已于 2026-10-05 16:54 修掉并复验。**
剩下 2 类 P1 问题（另有一条是上一版的误判，已撤回）：

| 级别 | 问题 | 后果 |
|---|---|---|
| ~~P1-1~~ | ~~索引探测 `& (cap-1)` 隐含要求容量是 2 的幂~~ | ✅ **已修**（2026-10-05 16:54，`tb/tools/OmsShm_P1-1.patch`）。原来默认容量下索引在 2048 条饱和、之后所有新单静默写不进去（slot 环还是 98% 空的）。见 §2.2 |
| **P1-4** | `lookup_*` 把"订单不存在"和"写者忙、重试 16 次没读到"**返回同一个 false** | 上层无法区分，对账逻辑会把活单当死单 |
| **P1-5** | 别名索引插入失败只打 `stderr` WARN，**不进任何统计** | 丢了 `clientOrderId` 映射而运维看不到 |
| ~~P1-3~~ | ~~`orderId` 变更时索引项永久泄漏~~ | **已撤回** —— 见 §2.1。真实生命周期下**不存在**这个问题，是我上一版的误判 |
| ~~**B5**~~ | ~~索引的"身份判定"只是 `hash + 前 16 字节`，却被写入路径当身份用；`update_slot` 又不更新 key 副本~~ | ✅ **已修**（2026-10-06 17:05，`tb/tools/OmsShm_B5.patch`）。修复前实测**用 A 的 key 查回 B 的报单体**（4000 key 压力下 156 次查错单 + 158 张单被静默并掉）。见 §2.3 / §2.4 |

> **上一版把 P1-3 列成"唯一会持续恶化的问题"，是错的。** 我当时的测试构造了
> "`orderId` 从 `X` 改成 `Y`" 这种**真实生命周期里不会发生**的序列。更正见 §2.1。
> （有意思的是：B5 修完之后，那个序列**真的**被正确处理了 —— V3 的 `EXCH live`
> 从 200 变成 100，见 §2.4。）

P1-1 曾经是**唯一**会让实盘静默丢单的问题（索引饱和后 `upsert` 回滚并返回
`kInvalidSlot`，策略侧看到的是"查不到"，不是"写失败"）—— **已修**。
B5 曾经是**唯一**会让实盘拿到**错单**的问题 —— **也已修**。

现在**没有任何一条会让实盘静默丢单或拿到错单**了。剩下的是：
P1-4 / P1-5（错误不可观测）、§3 的 P1-6 / P1-7（形式正确性，x86 上不出事但实现本身是错的）、
§4 的工程 / 运维层。

### 0.1 待改清单（行号 = 2026-10-06 的 1033 行版本；A1/A4/B5 之后行号已变，当前行号见各条的 ✅ 小节）

按"值不值得先做"排序。**A 组是这次新查出来的**（上一版没有）。

**A. 可观测性 —— 最值得先做**

| # | 位置 | 问题 | 为什么重要 |
|---|---|---|---|
| ~~**A1**~~ | `Stats` `585-621`；`oms_query.cpp:214-231` | ~~**索引占用完全没有暴露**：没有 `index_capacity`、没有 live 条目数、没有 tombstone 数~~ | ✅ **已修（§0.3）**。原判断：刚修的 P1-1 就是"索引静默饱和"，而**这个失败模式当时从工具里依然看不见** —— 出了事只能靠 `alloc_failures` 事后猜。实测稳态下 `live + tomb == index_capacity`、`empty == 0`（索引 100% 占满），全靠 `kMaxProbeIndex = 32` 撑着 |
| ~~**A2**~~ ✅ | `iterate_state` `567-580` | `read_slot_snapshot` 失败（写者忙 / slot 刚被 reclaim）时**静默跳过**该 slot，返回的 `n` 也不体现跳过了多少 | 这个函数正是**重启对账枚举活单**用的。漏一条 = 对账时把活单当不存在，且无任何提示。比 P1-4 更隐蔽（P1-4 至少调用方知道查了哪个 key）。**✅ 已修 2026-10-06 19:20**：`iterate_state` / `iterate_live` / `iterate_finished` 增加可选 `size_t* skipped_out`，跳过条数不再丢失（**默认参数，老调用点零改动**） |
| **A3** | `Stats` `585-621` | 5 个计数器里没有"别名索引插入失败" | 见 C2 |
| ~~**A4**~~ | `upsert` 回滚日志 | ✅ **已修（§0.4）**。原问题：报错文案还写着 `index_capacity=2N`（P1-1 之后已是 `next_pow2(2N)`），且只说 "Consider raising index_capacity or investigating tombstone leak" —— 既不打印实际数值，也把人指向一个**实测已排除**的方向。新文案自带占用快照并给出判读 | 运维照着这句去调容量会算错 |

**B. 数据安全 / 正确性**

| # | 位置 | 问题 | 影响 |
|---|---|---|---|
| **B1** | `find_slot:985-993` | writer 侧查找**完全不复核**（reader 侧 `lookup_impl:553-562` 复核了），且返回的 `slot_idx` **没有 `>= slot_capacity()` 边界检查**（`lookup_impl:551` 有）。调用点 `730-732` 直接 `update_slot(slots()[idx])` | 配合 `insert_index:965-972` 的"命中 hash+prefix 就覆盖 `slot_idx`"，索引一旦指向别人的 slot，`upsert` 就会**静默覆盖无关活单**；索引若损坏则是 OOB 写。概率极低（~1e-9），但方向是**数据损坏** |
| **B2** | `read_slot_snapshot:528` / `536`；`write_new_slot:856` / `877`；`update_slot:883` / `900` | seqlock **两边都缺 fence**：读侧 `s2` 前缺 acquire fence，写侧首次 `fetch_add` 应是 `acq_rel` | x86-64 TSO 天然禁止这两类重排 → **目标机 Ubuntu 实际不出问题**；ARM（含本机开发机）会读到撕裂快照 |
| **B3** | `read_slot_snapshot:525-540` + `lookup_impl:553` | `lookup_*` 把"订单不存在"和"写者忙、重试 16 次没读到"返回**同一个 false** | 实测 0.036%（人工高压下）；对账逻辑会把活单当死单 |
| ~~**B4**~~ ✅ | `insert_all_indices:929-938`；`update_slot:901-903` | 别名索引插入失败只打 `stderr` WARN 后 `return true`；`update_slot` 连返回值都不看 | 丢了 `clientOrderId` / `orderId` 映射而**运维看不到**（实测 `alloc_failures=0` 的同时打出 5 行 WARN）。**✅ 已修 2026-10-06 19:20**：新增 `total_alias_insert_failures` 计数（塞进保留 pad，`sizeof`/`kVersion` 不变）+ 日志限流；`doctor` 增加对应分支；`update_slot` 那条已在 ② 里修掉（`sync_one_key` 现在看返回值） |
| ~~**B5**~~ ⭐ ✅ | `index_probe_find:261-268`；`insert_index` 命中分支；`update_slot:946-970`；`lookup_impl:572` | **索引的"身份判定"其实只是提示**（`fnv1a(key)` + 前 16 字节，不覆盖 tail、不覆盖长度），而写入路径把它当身份用；更关键的是 `update_slot` **不更新 `s.orderSysId`/`s.clientOrderId`**，碰撞后 slot 的 key 副本与报单体**自相矛盾**，而 `lookup_impl:572` 复核的是 **key 副本** → 复核**通过** | **实测：用 A 的 key 查，返回 B 的报单体。** 这是"查错单"，比"查不到"严重得多 —— 详见 **§2.3**。**✅ 已修 2026-10-06 17:05（`tb/tools/OmsShm_B5.patch`），改法与实测见 §2.4** |
| ~~**B6**~~ ⭐⭐ ✅ | `kMaxProbeIndex:64`；`index_capacity = next_pow2(2*slot_cap)`（`OmsShm.h:421`）；`index_probe_find:308`；`insert_index:1162` | **索引稳态下必然 100% 占满**（`live + tomb == index_capacity`、`empty == 0`），此后插入能否成功**完全取决于 32 步内能否找到 tombstone**。实测最长 live 连续段 **33~56**，已经超过 32 | **实测正在丢单**：`slot_cap=65536` 时 203 万次插入丢 **388 张**（**1.9e-4，1/5235**）；churn 越久越差（2/5/10/30/60 轮 → 1/5/12/70/126 次）。后果与 P1-1 同类：`upsert` 回滚返回 `kInvalidSlot`，单**没进 SHM**。**详见 §2.5**。**✅ 已修 2026-10-06 18:05（`tb/tools/OmsShm_B6.patch`）—— 索引改 4N、`kVersion` 2→3，改法与实测见 §2.6** |
| ~~**B7**~~ ⭐ ✅ | `alloc_slot():995-1061`；`upsert():922-926` | **`alloc_slot` 只扫 `next_slot_hint` 起连续 128 个 slot 就放弃** —— 环里别处有可回收 slot 也返回 `kInvalidSlot`；而失败时 hint 已前进 128，下一次调用看的是**别的区域**，**这张单没有第二次机会**。`upsert` 拿到失败只 `total_alloc_failures++`，**一行日志都没有** | **实测正在丢单**：连续 LIVE 块 512 / 1024 / 2048 时丢单率 **1/1024 ~ 1/140**，且**全部**是"窗口内空、全表有"（`real_exhaust=0`）→ 本可避免。失效前提是"在途单 ≥ 128 张"，**没有设计上界**。**详见 §2.7**。**✅ 已修 2026-10-06 18:45（`tb/tools/OmsShm_v4.patch`）—— 快路径 128 步 + 全表兜底** |
| ~~**B8**~~ ⭐ ✅ | `sync_one_key():1164-1184` | 顺序是 `tombstone_index(旧)` → `memcpy(新)` → `insert_index(新)`，且**丢弃返回值** | insert 失败时变成"旧条目已删、副本已改、新条目没挂上" → **新旧两个 key 都查不到**，slot 彻底不可达（不只是"新 key 查不到"），而日志照样打 "已迁移索引"。实测（`kMaxProbeIndex=1` 副本，320 次改 key）**破坏不变量 32 次**。**详见 §2.7**。**✅ 已修 2026-10-06 18:45** |
| ~~**B9**~~ ✅ | `oms_query.cpp` `--stats` 的 `probe_max` 行 | 数值行**行尾挂了说明**（`probe_max : 32 (索引 open-addressing 探测步数上限)`），违反 A1/§0.4 定下的"数值行必须以数字结尾"约定。`doctor` 用 `grep -oE '[0-9]+' \| head -1` 取**第一个**数字，这里只是**碰巧**那段中文里没有数字才没出事 | 一旦有人把括号里的文案改成含数字的内容（或把提示挪到数字前面），`doctor` 就会取到错的数 → 健康检查静默失真。**✅ 已修 2026-10-06 19:20**：提示挪到独立的一行，并把该约定加进 `oms_test`/`oms_shm.sh test` 的解析契约断言 |
| ~~**B10**~~ ✅ | `alloc_slot_scan()` 开头 | `cap == 0` 时 `mask = cap - 1` 得到 `0xFFFFFFFF`，且 `(cap & (cap-1)) == 0` 判为 `true` → `h & mask` 直接当 `idx` 用 → **越界访问 `slots[]`**；若走 `h % cap` 那条则是**除零** | 正常路径下 `open()` 已保证 `cap >= 1`，所以是**防御性**问题；但 `slot_capacity()` 在 header 为空时返回 0，属于"以后某个改动就可能踩到"的地雷。**✅ 已修**：函数开头 `if (cap == 0) return kInvalidSlot;` |

**C. 小问题（都不影响正确性）**

| # | 位置 | 问题 |
|---|---|---|
| ~~C1~~ | `reset_all():711-716` | ✅ **已修（§2.7.7）**。重置了 4 个计数器，**漏了 `total_stale_live_reclaims`** → `--reset` 后 `doctor` 会误报。v4 一并补上（含 3 个新计数），`verify_v4.cpp` 有断言 |
| C2 | `stats():606` | `slot.last_update_time_ns` 非原子读 → 撕裂值可能把 `live` 误分类成 `live_stale`（best-effort，影响小） |
| C3 | `now_ns():624` | 用 `steady_clock`（Linux = `CLOCK_MONOTONIC`），跨重启无意义。默认 `/dev/shm` 是 tmpfs、重启即清空，所以没事；改指磁盘路径就是个坑 |
| C4 | `write_new_slot:864` | `compose_client_key` 的返回值被忽略；`strategyId` 超过 43 字符会被**静默截断**（`485-500`），两个长 strategyId 前缀相同就会撞车 |
| C5 | `632` | `read_only_` 成员**写了从来没用过**（`319` 赋值，全文件无读取）→ 死字段 |
| C6 | `745` / `836` / `931` / `936` | 热路径 `fprintf(stderr)`。索引满时会刷屏（实测 5 万次迭代打出 9.6 万行）→ 会淹日志 / 写满磁盘。**v4 给新增的两处日志加了限流（前 8 条 + 每 4096 条一条），但这几条旧的热路径 `fprintf` 还没处理** |
| C7 | `open()` `313-420` | 没有 `flock` / `owner_pid` 校验 —— **挡不住第二个 writer**。两个 writer 会让 `insert_index` 的非原子 RMW 破坏索引 |
| C8 | `268` / `187` | 注释写错：`capacity * 512 B`（实际 `kSlotSize = 1024`）；`RCommand 本身 ~656B`（实际 **536**） |
| C9 | `212-217` | `_pad1` 每个 slot 浪费 **256 B** → 100000 slot = **25.6 MB**。`kSlotSize` 可考虑降到 768 |
| C10 | `915-918` | `is_terminal` 的终结集合建议确认：`OS_CANCEL`（请求态）不在里面，只有 `OS_CANCELED`。若某交易所把 `OS_CANCEL` 当终态回报，slot 会一直 LIVE 到 24h 后被强制回收 |

**D. 不在这个文件里（`tb/tools`）**

| # | 位置 | 问题 |
|---|---|---|
| D1 | `oms_demo.cpp` | 与 `oms_bench.cpp` **逐字节相同**，功能测试不存在 |
| D2 | `tb/CMakeLists.txt` | `tb/tools/*.cpp` 不在构建里 → 编译错误不会被 CI 发现 |
| ~~D3~~ | `oms_shm.sh` `cmd_doctor` | ✅ **已修（§0.4）**。`awk '{print $NF}'` 取到行尾中文提示 → `total_stale_live_reclaims` **永远报 0**；A1 之后 `total_alloc_failures` 也中招（实测真值 828 时 `doctor` 报 `✓ healthy`）。现改为按 key 锚定行 + 取行内第一个数字 |
| ~~D4~~ | `oms_bench.cpp` / `oms_demo.cpp` `--help`；`include/oms/README.md` | ✅ **已修（§2.6）**。原文 `must be power of 2 **for speed**`（性能问题）→ 实际是**正确性**问题（`index_capacity` 必须是 2 的幂，B6 之后由 `next_pow2(4N)` 自动保证，`slot_cap` 本身**不需要**是 2 的幂）。README 的"建议 2^N"、`2N × 32B`、`~120MB`、`shm_size: 256m` 一并改正 |
| D5 | `oms_query.cpp:216-220` | `100.0 * s.x / s.capacity`，`capacity == 0` 时除零 |

### 0.2 已排除的（查过，**没问题**，不要再翻）

| 疑点 | 结论 |
|---|---|
| **tombstone 会不会无限累积** | **不会**。实测 40000 次 insert / 39744 次 reclaim（capacity 256 / index 512），占用从第 20 轮起**完全持平**：`256 live / 256 tomb / 0 empty`，一直到最后。`alloc_failures=0`。见 §5 |
| `orderId` 变更导致索引泄漏 | **不存在**，见 §2.1 |
| `OmsShmHeader` pad / `index_probe_find` 前向声明 | 已修，见 §1 |
| 索引 2 的幂 | 已修，见 §2.2 |

---

### 0.3 A1 具体改法（✅ **已应用** 2026-10-06 15:54，并在真实文件上复验通过）

补丁：`tb/tools/OmsShm_A1.patch`（89 行，`patch -p1` 干净应用）。
应用后：`OmsShm.h` 1033 → **1062 行**，`oms_query.cpp` 236 → **262 行**。
改动分三处，**全部只碰诊断路径，热路径一行没动**。

**① `OmsShm.h` · `Stats` 结构体（`585-621` 段末）加 3 个成员**

```cpp
struct IndexOccupancy {
    uint32_t live  = 0;   // 有效条目
    uint32_t tomb  = 0;   // tombstone (已删, 仍占 bucket)
    uint32_t empty = 0;   // 从未用过
    uint32_t used() const noexcept { return live + tomb; }
};
uint32_t       index_capacity = 0;              // 每个索引的 bucket 数
IndexOccupancy index_occupancy[IDX_COUNT] = {}; // 下标 = IndexKind
```

**② `OmsShm.h` · `stats()` 里加一段扫描**（`O(index_capacity × 3)`，纯诊断）

```cpp
s.index_capacity = index_capacity();
if (s.index_capacity) {
    for (uint32_t k = 0; k < IDX_COUNT; ++k) {
        IndexEntry* arr = index(k);
        if (!arr) continue;
        auto& occ = s.index_occupancy[k];      // 注意: 必须 auto&, 嵌套类型不能省
        for (uint32_t i = 0; i < s.index_capacity; ++i) {
            uint64_t h = arr[i].key_hash.load(std::memory_order_relaxed);
            if (h == kHashEmpty)          ++occ.empty;
            else if (h == kHashTombstone) ++occ.tomb;
            else                          ++occ.live;
        }
    }
}
```

**③ `tb/tools/oms_query.cpp` · `--stats` 打印**

```
  index_capacity             : 2048 (每个索引; probe 上限 32)
  index orderSysId           : live=10       tomb=0        empty=2038     占用 0.49%
  index clientOrderId        : live=10       tomb=0        empty=2038     占用 0.49%
  index orderId              : live=10       tomb=0        empty=2038     占用 0.49%
```

并在 `total_alloc_failures != 0` 时补一句 `← 非零! 有单没写进 SHM, 先看上面的 index empty / index_capacity`。

**设计要点**

1. **按索引分开存，不做总数** —— 三个索引**各自独立饱和**。实测 `orderId` 索引的 `live`
   会比另外两个少几条（`orderId` 为空时不入索引），合并成总数就把这个差异抹掉了。
2. **`empty == 0` 是稳态正常现象，不是告警。** slot 环填满后 `live ≈ slot_cap`、
   `tomb ≈ slot_cap`，于是 `live + tomb == index_capacity`、`empty → 0`。
   此时插入能否成功全靠 `kMaxProbeIndex = 32` 步内找到 tombstone。
   所以这里只打一条**说明性**提示，真正要盯的信号是 `total_alloc_failures`。
3. **扫描代价**：默认 100000 slots → `index_capacity = 262144`，扫描 786432 个 bucket
   ≈ 3 MB 内存读，亚毫秒级。全仓库 `stats()` 只在 3 处调用，全是**每进程一次**的冷路径
   （`oms_bench.cpp:319`、`oms_demo.cpp:319`、`oms_query.cpp:213`），所以直接放进
   `stats()` 是安全的。若将来有人要在循环里调，再拆成独立的 `index_occupancy()` 方法。
4. **`kHashEmpty` / `kHashTombstone` 之外一律算 live** —— 与 `insert_index` 的判定口径一致。

**这个指标确实能抓住 P1-1（对照实验）**

把 A1 扫描加在**旧的**（未修 P1-1 的）容量/探测逻辑上，跑 P1-1 的原工况
（capacity 100000，插入 50000 单）：

```
capacity=100000  index_capacity=200000   (OLD sizing: 2N, no pow2 rounding)

slots : empty=97952 live=2048 finished=0
inserts=2048  alloc_failures=47952   <-- 47952 of 50000 orders never reached the ring

index_capacity : 200000
index orderSysId    : live=2048     tomb=0        empty=197952   占用 1.02%
index clientOrderId : live=1945     tomb=0        empty=198055   占用 0.97%
index orderId       : live=1943     tomb=0        empty=198057   占用 0.97%
```

**索引 99% 是空的，却在疯狂 `alloc_failures`** —— 一眼就能看出不是容量不够，
而是探测序列的问题。而没有这个指标时，`upsert` 回滚日志只会说
`Consider raising index_capacity`，**把人往完全错误的方向带**（A4）。

**改动点在应用后的行号**

`include/oms/OmsShm.h`（1062 行）：

| 行 | 内容 |
|---|---|
| `598-603` | 新增 `struct IndexOccupancy { live / tomb / empty / used() }` |
| `604` | 新增 `uint32_t index_capacity = 0;` |
| `605` | 新增 `IndexOccupancy index_occupancy[IDX_COUNT] = {};` |
| `629` | `stats()` 里 `s.index_capacity = index_capacity();` |
| `630-642` | 新增 `O(index_capacity × 3)` 扫描循环（`634` `auto& occ = ...`，`637-639` 三分类） |

`tb/tools/oms_query.cpp`（262 行）：

| 行 | 内容 |
|---|---|
| `226-227` | 打印 `index_capacity` |
| `229-246` | 每索引一行 `live/tomb/empty` + 占用 %，以及 `empty == 0` 的说明性提示 |
| `253-257` | `total_alloc_failures` 非零时追加 `← 非零! 有单没写进 SHM, 先看上面的 index empty / index_capacity` |

**验证清单（✅ 全部在**真实文件**上复验通过）**

| 项 | 结果 |
|---|---|
| `patch -p1` 干净应用 | ✅ 两个文件，exit 0 |
| 应用后 md5 与验证副本一致 | ✅ `OmsShm.h` `d05453d1…`、`oms_query.cpp` `ed2622bb…` |
| `-Wall -Wextra` 编译三个驱动 | ✅ 无告警 |
| 低占用场景（capacity 1024 / 10 单） | ✅ `live=10 tomb=0 empty=2038 占用 0.49%` |
| 饱和稳态（capacity 256 / 40000 insert） | ✅ `live=256 tomb=256 empty=0 占用 100.00%` + 说明性提示 |
| P1-1 回归（capacity 100000 / 50000 insert） | ✅ `idx_cap=262144`、`alloc_failures=0`、占用 `19.07%` |
| 多容量回归（65536 / **65537** / 100000 / 1024 / 3） | ✅ 全部 `fail=0`，且 `live + tomb + empty == index_capacity` 恒成立 |
| P1-1 对照实验（旧逻辑 + A1 扫描） | ✅ `empty=197952` 与 `alloc_failures=47952` 同时出现，一眼可辨 |
| 反向撤销 | ✅ `patch -R -p1 < tb/tools/OmsShm_A1.patch` 干净反向，两个文件 md5 精确回到应用前 |

**应用前备份**：`/tmp/omsshm_a1/OmsShm.h.pristine-20261006-1553`、
`/tmp/omsshm_a1/oms_query.cpp.pristine-20261006-1553`。

---

### 0.4 A4：回滚日志重写（✅ 已应用 2026-10-06 16:20）+ 顺带修掉 A1 引入的 `doctor` 回归

补丁：`tb/tools/OmsShm_A4.patch`（193 行 / 3 文件）。
应用后：`OmsShm.h` 1062 → **1101 行**，`oms_query.cpp` 262 → **267 行**，
`oms_shm.sh` 180 → **182 行**。

#### ① `upsert` 回滚日志：从"误导"改成"自诊断"

原文案（**已删除**）：

```
[OmsShm][ERROR] insert_index full, rollback slot=%u orderSysId=%s.
Consider raising index_capacity or investigating tombstone leak.
```

三个毛病：**(a)** 注释里 `index_capacity=2N` 是 P1-1 之前的算法；
**(b)** 不打印任何实际数值，运维不知道现在的容量是多少；
**(c)** "investigating tombstone leak" 指向一个**实测已排除**的方向（§0.2）。

新文案：

```
[OmsShm][ERROR] index insert failed: kind=orderSysId slot=0 orderSysId=SYS_FULL
  slot_cap=16  index_capacity=32 (=next_pow2(2*slot_cap))  probe_max=1
  index occupancy: live=32 tomb=0 empty=0  (used=100.00%)
  索引已近满 (可复用 bucket tomb+empty < 10%) → 加大 slot_cap, index_capacity 会随之变成 next_pow2(2*slot_cap)
  → 该单未写入 SHM, total_alloc_failures 已 +1, 上层会当作写失败处理
```

另一分支（稳态 churn，`live=40 tomb=88 empty=0`）：

```
  仍有可复用 bucket (tomb+empty ≥ 10%) 但 probe_max 步内没找到 → 探测序列/聚集问题, **不是容量不够**
```

**判据是 `tomb + empty` 而不是 `empty`。** 第一版我用的是 `empty > icap/2`，但
`index_capacity = next_pow2(2*slot_cap)` 让满载负载率恰好是 0.5，这个阈值永远处在边界上，
判不出东西。真正的区分是"**还有没有可复用的 bucket**"：环满稳态下 `empty` 本来就是 0，
但 tombstone 是可复用的，那种情况属于探测问题而非容量不够。

顺带做的两件事：
- `insert_all_indices` 增加 `IndexKind* failed_kind` 出参，日志才能报出**是哪个索引**失败。
- 新增 `index_kind_name()`（`OmsShm.h:80`）作为索引名的**唯一来源**，
  `oms_query.cpp` 里那份重复的 `kIdxName[3]` 删掉了。

#### ② 修掉 A1 引入的 `doctor` 回归（**这一条比较严重**）

`oms_shm.sh cmd_doctor` 原来这样取值：

```bash
alloc_fail=$(echo "$out" | grep total_alloc_failures | awk '{print $NF}')
```

A1 在 `total_alloc_failures` 那行**行尾追加了中文提示**，于是 `$NF` 取到的是提示文字。
更糟的是 A1 的说明文字里也出现了 `total_alloc_failures` 这个词，`grep` 会**同时命中两行**。

实测（`alloc_failures` 真值 828）：

```
tools_pre/oms_shm.sh: line 144: [: 它增长才说明索引容量该加了
index_capacity: integer expression expected
✓ alloc_failures = 0          ← 假 OK
✓ healthy                     ← 结论完全错误
```

修好后：

```
✗ alloc_failures = 828  → RING EXHAUSTED, tb 无法新单! 立刻检查活单数 / 加 capacity
✗ found 1 issue(s)
```

**两处都改了，缺一不可：**
1. `oms_query.cpp`：**数值行一律以数字结尾**，提示另起一行。这同时修掉了
   `total_stale_live_reclaims` 的旧 P2-2 问题。
2. `oms_shm.sh`：取值改成"**只认以 key 开头的那一行**，再取行内第一个数字"：
   ```bash
   grep -E '^[[:space:]]*total_alloc_failures[[:space:]]*:' | grep -oE '[0-9]+' | head -1
   ```

> **通用规则（值得写进代码规范）**：给机器读的数值行，行尾**不要**挂任何人类可读的文字。
> 这类"提示语吃掉解析"的 bug 是静默的 —— 检查会一直输出"正常"，直到真的出事那天。

#### 验证（✅ 全部在**真实文件**上）

| 项 | 结果 |
|---|---|
| `patch -p1` 干净应用 / 反向 | ✅ 3 文件；应用后与验证副本逐字节一致 |
| `-Wall -Wextra` 编译 `oms_bench` / `oms_demo` / `oms_query` | ✅ 全部 exit 0 |
| `bash -n oms_shm.sh` | ✅ OK |
| 回滚日志 — 探测问题分支（`probe_max=1` churn） | ✅ `live=40 tomb=88 empty=0` → 判定"探测序列问题" |
| 回滚日志 — 索引真满分支（索引数组直接填满） | ✅ `live=32 tomb=0 empty=0` → 判定"已近满, 加 slot_cap" |
| 正常 `probe_max=32` 下回滚分支是否可达 | ✅ **不可达**（churn 2560 次 `fail=0`）—— 说明它只在真正异常时触发 |
| `doctor` 对 `alloc_failures=828` | ✅ 检出并报 `found 1 issue(s)`（修复前是 `✓ healthy`） |
| A1 回归（多容量 65536/65537/100000/1024/3） | ✅ 全部 `fail=0`，`live+tomb+empty == index_capacity` 恒成立 |

**应用前备份**：`/tmp/omsshm_a4/*.pristine-20261006-1620`。

---

## 1. 已修复并复验（原 P0）

### P0-1 · `OmsShmHeader` 的 pad ✅ 已修

`:144` 现为 `char pad[4096 - 112];`（原 `4096 - 96`）。pad 之前的成员实际占 **112 B**：

| 成员 | 字节 |
|---|---|
| 5 × `uint32_t`（magic/version/slot_capacity/index_capacity/index_kinds） | 20 |
| 对齐填充 | 4 |
| 2 × `uint64_t`（min_reclaim_age_ns / max_live_stale_ns） | 16 |
| `next_slot_hint` / `global_seq` / `created_ts_ns` | 24 |
| `owner_pid` / `_pad0` | 8 |
| 5 × `atomic<uint64_t>`（4 个统计 + `total_stale_live_reclaims`） | 40 |
| **小计** | **112** |

### P0-2 · `index_probe_find` 先用后声明 ✅ 已修

函数定义已前移到 `:204`（`IndexEntry` 之后、`OmsShmSegment` 之前），三个调用点
（`:497` / `:937` / `:954`）都在定义之后，不再有 `use of undeclared identifier`。

### 复验（本机，用**未打补丁的原始文件**）

```bash
clang++ -std=c++17 -I /Users/lawson/Documents/hft/include -I <stub> -o probe probe.cpp   # 0 error
```

```
sizeof(OmsShmHeader) = 4096      ← 正确
sizeof(IndexEntry)    = 32
sizeof(OmsSlot)       = 1024
sizeof(RCommand)      = 536
offsetof(order)       = 232
```

`tb/tools/` 的三个工具也都能编过。**这两个 P0 确认关闭。**

---

## 2. P1 级问题（P1-1 已修，其余待办）

### P1-1 · 索引探测隐含要求「容量是 2 的幂」，而**默认容量不是** ✅ 已修

**修复前**三处索引探测都用同一套写法：

```cpp
uint32_t mask = cap - 1;
uint32_t b = static_cast<uint32_t>((hash + i) & mask);   // index_probe_find:236
                                                         // insert_index:950
                                                         // (tombstone_index 走 index_probe_find)
```

`& mask` **只在 `cap` 是 2 的幂时才是一个合法（满射）的探测序列**。
有意思的是 `alloc_slot:788-789` 反而**正确处理**了非 2 的幂：

```cpp
bool pow2 = (cap & (cap - 1)) == 0;
uint32_t idx = pow2 ? static_cast<uint32_t>(h & mask)
                    : static_cast<uint32_t>(h % cap);
```

→ 作者想到了 **slot 环**，漏了**索引数组**。而：

- `kDefaultCapacity = 100'000`（`:59`）→ `index_capacity = 200'000`
- `oms_bench` 的默认 `--capacity=100000`（`:261`）也是非 2 的幂
- bench 的 `--help` 只写 "must be power of 2 **for speed**" —— 这不是性能问题，是**正确性**问题
- README 也只写"建议 2^N"

`mask = index_capacity − 1 = 199999 = 0x30D3F`，二进制里**只有 11 个 bit 是 1**，
所以 `x & mask` 最多只能产生 **2¹¹ = 2048** 个不同 bucket。`kMaxProbeIndex = 32`
的探测上限救不了：2048 个桶一旦用满，任何 32 步内的线性探测都找不到空位。

**实测（当前文件重新编译，`--iters=50000`）**

| 容量 | 2 的幂 | `total_inserts` | `alloc_fail` | slot 环状态 |
|---|---|---|---|---|
| **100000（默认）** | ✗ | **2048** | **95905** | `empty=97952 / 100000` ← **98% 是空的** |
| 131072（2¹⁷） | ✓ | 50000 | 0 | — |
| 65536（2¹⁶） | ✓ | 50000 | 0 | — |

最后一行是最刺眼的：**slot 环 98% 空着，索引却已经满了，每条新单都失败。**
`total_inserts = 2048` 与"最多 2048 个 bucket"的算术**完全吻合**。

失败路径是静默的：`insert_all_indices` 返回 false → `upsert:743-757` 回滚 slot →
返回 `kInvalidSlot`。**策略侧看到的是"查不到"，不是"写失败"。**

**修法（已实测验证，补丁见 §2.2）**

两个动作，缺一不可：

1. **`index_capacity` 改成 `next_pow2(2 × slot_cap)`** —— 这是治本的。
   索引里每个 slot 每个索引最多贡献 **1 项**，所以负载上界是
   `slot_cap / index_capacity`。向上取整到 2 的幂后负载只会**更低**，不会更高：
   `slot_cap=100000` → `next_pow2(200000) = 262144` → 最大负载 **0.38**（原来是 0.5 的意图）。
   于是 `& mask` 永远合法，**热路径一行都不用改**。
2. **三处探测改成跟 `alloc_slot` 一样分 `pow2 / 非 pow2` 两条路** —— 这是防未来的。
   万一有人手搓一个非 2 的幂的段、或者将来 `index_capacity` 的计算方式变了，
   也不会静默饱和。代价是一个**可预测的分支**（实测汇编里 0 条 `cmov`，
   新增的 `udiv` 都在分支后面，pow2 路径上不执行）。

外加：`open()` 打开**已有**文件时校验 `is_pow2(hdr_idx)`，不合法就带文件名抛错 ——
因为老版本建的文件（`index_capacity = 2 × slot_cap`，如 200000）是**带病**的，
必须拒绝而不是继续跑。注意 `OmsShmReader::open` 故意传 `slot_cap = 0`（容量从 header 读），
所以 `slot_cap` 的范围校验只能放在**新建**分支里。

> ⚠️ 不要用"把默认容量改成 131072"当修法。那样虽然也能跑，但内存从 **121.6 MB**
> 涨到 **159.4 MB**（+37.8 MB，+31%），而上面第 1 条只涨到 **127.6 MB**
> （`100000` slot + `262144` 索引 = 127,569,920 B，+6.0 MB）—— **同样的正确性，
> 多花 32 MB 常驻内存**。见 §2.2 的对照。

### 2.1 撤回：`orderId` 变更**不会**泄漏（上一版误判）

**结论：`update_slot` 的 `orderId` 索引逻辑在真实生命周期下是正确的，没有泄漏。**
上一版把它列为"唯一会持续恶化的问题"是错的，原因是我的测试构造了一个
**真实流程里不会发生**的序列。

#### 为什么上一版的测试是错的

我当时的测试对每单调了两次 `upsert`，`orderId` 从 `X<i>` 变成 `Y<i>` ——
也就是**从非空改成另一个非空**。而真实流程是：

| # | 事件 | `orderId` | `orderId_changed` | EXCH 索引 |
|---|---|---|---|---|
| 1 | 本地创建报单，还没发到交易所 | `""` | — | **不插入**（`insert_all_indices:934` 有 `if (s.orderId[0])` 守卫） |
| 2 | 交易所回报，**首次**带上 `orderId` | `""` → `"E1"` | **true（仅此一次）** | 插入 |
| 3 | 后续成交流 / 撤单流 | `"E1"` 不变 | false（`strncmp` 相等） | 不动 |

关键在第 2 步：**被覆盖的旧值是空串，而空串从来没有被索引过** ——
所以"没 tombstone 旧项"在这里**不产生孤儿**。而这个分支**一单只触发一次**
（`orderId` 一旦分配就不再变化），所以也就不存在"漏一个再漏一个"。

#### 实测：真实生命周期下 EXCH 精确归位

```
1) 100 单本地创建（还没有 orderId）   slots=100  ORD_SYS=100 CLIENT=100 EXCH live=0
   ← 空 orderId 被正确跳过，这正是上面那条能成立的原因
2) 100 单交易所回报（orderId 首次落库）slots=100  ORD_SYS=100 CLIENT=100 EXCH live=100
3) 3×100 次成交回报（orderId 不变）    slots=100  ORD_SYS=100 CLIENT=100 EXCH live=100
4) 全部 FINISHED + 压入 128 条新单      slots=128  ORD_SYS=128 CLIENT=128 EXCH live=128
   reclaims=100  alloc_failures=0
```

**第 4 步三个索引都精确等于 128（= 占用的 slot 数）。没有泄漏。**

另外两个对照：

```
V2: orderId 从第一次 upsert 就带上            EXCH live=100   （正常）
V3: orderId 从非空改成另一个非空（A->B）      EXCH live=200   ← 只有这种情况才泄漏
```

V3 就是上一版测出来的东西。**它需要 `orderId` 发生第二次"非空→不同非空"的变化，
而现在的 OMS 不会这么做**（你也确认了：orderId 分配之后不再修改）。

> 顺带说明：第 4 步里 `tomb=60/74/79` 不是问题。tombstone 是正常状态，
> 会被后续插入复用；判断泄漏要看 **`live`**，不是 `tomb`。

#### 真正该做的（不是修泄漏，是把这个前提写下来）

代码现在依赖一条**没有写在任何地方的隐含前提**：

> `orderId` 一旦从空变成非空，就**再也不会变**。

这条前提是对的，但它现在只存在于你的脑子里。建议：

1. **在 `update_slot` 的 `orderId_changed` 分支加注释**，写明"本分支一单只应触发一次；
   旧值为空，因此无需 tombstone。若将来某个交易所在 amend / cancel-replace 时重发
   orderId，这里就会开始泄漏索引桶"。这是**一行注释的成本，防一次未来的事故**。
2. **在真正的 `oms_demo` 里把这条不变量钉成测试**（§4 P1-2）：真实生命周期三步走之后
   断言 `EXCH live == occupied_slots`；再加一个 V3 用例断言"两次非空变更会泄漏"，
   这样一旦有人改了语义，测试会**主动失败**而不是静默退化。
3. （可选加固）真要做成对 orderId 变更鲁棒的，就是 `copy_key` 覆盖前先
   `tombstone_index(IDX_EXCHANGE_ID, old_orderId)` —— 但**今天不需要**，别为不存在的
   场景增加 tombstone churn。

> 这条更正同时说明 P1-1 更值得优先：**唯一**会让实盘静默丢单的还是索引 2 的幂问题。

### 2.2 P1-1 补丁（**已应用**，实测通过）

补丁文件：**`tb/tools/OmsShm_P1-1.patch`**（7 个 hunk，134 行）。
**2026-10-05 16:54 已应用到 `include/oms/OmsShm.h`**（44788 → 47794 B，980 → 1033 行）。

```bash
cd /Users/lawson/Documents/hft
patch -p1          < tb/tools/OmsShm_P1-1.patch   # 已执行
patch -p1 -R       < tb/tools/OmsShm_P1-1.patch   # 撤销（已验证可反向应用）
# 原始文件备份：/tmp/omsshm_fix/OmsShm.h.pristine-20261005-1654
```

#### 改动点在**新文件**里的行号

| 新行号 | 内容 |
|---|---|
| `112` | `is_pow2()` 新增 |
| `117` | `next_pow2()` 新增 |
| `131` | `index_probe_bucket()` 新增（三处探测的共用助手） |
| `145` | `OmsShmHeader::index_capacity` 注释改成 `next_pow2(2N)` |
| `237` / `240` | `index_probe_find`：加 `pow2` 判定 + 改用共用助手 |
| `341-342` | `open()`：`use_idx = next_pow2(use_cap * 2)` |
| `353` | `open()` 新建分支：`slot_cap` 范围校验 |
| `380` / `385` | `open()` 新建初始化：`map_from` / `header->index_capacity` 用 `use_idx` |
| `411-416` | `open()` 打开已有文件：`is_pow2(hdr_idx)` 校验 |
| `951` / `954` | `insert_index`：同 `index_probe_find` |

#### 补丁做了什么

| hunk | 位置 | 改动 |
|---|---|---|
| 1 | `strnlen_max` 之后 | 新增 `is_pow2()` / `next_pow2()` / `index_probe_bucket()` 三个 inline 助手 |
| 2 | `OmsShmHeader::index_capacity` 注释 | 改成 `next_pow2(2N)`，写明**必须是 2 的幂** |
| 3 | `index_probe_find` | 探测改用 `index_probe_bucket`；加 `cap == 0` 保护 |
| 4 | `open()` | `use_idx = next_pow2(use_cap * 2)`；`slot_cap` 范围校验**放进新建分支** |
| 5 | `open()` 新建初始化 | `map_from` / `header->index_capacity` 都用 `use_idx` |
| 6 | `open()` 打开已有文件 | `is_pow2(hdr_idx)` 校验，不合法就带文件名抛错 |
| 7 | `insert_index` | 同 hunk 3 |

#### 实测结果（**应用后又在真实文件 `include/oms/OmsShm.h` 上完整重跑了一遍**，结论一致）

**① 索引饱和消失（`--iters=50000`，`ERROR` 行数 / `reader open failed` 行数都是 0）**

| `--capacity` | `total_inserts` | `alloc_fail` | 文件大小 | 说明 |
|---|---|---|---|---|
| **100000（默认）** | **50000** | **0** | 127,569,920 B | 修好了（原来 2048 / 95905） |
| 131072 | 50000 | 0 | 159,387,648 B | |
| 65537（**非 2 的幂**） | **50000** | **0** | 92,279,808 B | 任意容量都能跑 |
| 1000 | 1000 | 49000 | 1,224,704 B | 正常：slot 环本身只有 1000 个，不是索引问题 |
| 12345 | 12345 | 37655 | 15,791,104 B | 同上 |

**② 老文件被正确拒绝**（用一个由**原版头文件**建的 `capacity=100000` 文件测）：

```
header 里读到: slot_capacity=100000  index_capacity=200000
REJECTED: OmsShm: index_capacity=200000 is not a power of two; this file was created by
an older build whose index probe saturates early. Delete /tmp/omsshm_fix/legacy.dat
and let it be recreated.
```

→ 运维必须**删掉旧的 shm 文件**再重启。因为是 `/dev/shm` 下的，重启机器就没了；
如果是磁盘路径，记得手动删。

**③ 没有性能回归**（同容量 131072、同文件大小、交替跑 5 轮）：

| 轮次 | 原版 INSERT / LOOKUP | 补丁后 INSERT / LOOKUP |
|---|---|---|
| 1 | 367 ns / 131 ns | 401 ns / 136 ns |
| 2 | 388 ns / 135 ns | 355 ns / 137 ns |
| 3 | 364 ns / 133 ns | 365 ns / 133 ns |
| 4 | 351 ns / 140 ns | 358 ns / 146 ns |
| 5 | 374 ns / 167 ns | 353 ns / 146 ns |

**两组完全重叠 —— 没有回归。** 原因：`slot_cap=100000` 时 `index_capacity=262144` 是 2 的幂，
探测走的还是原来那条 `& mask` 分支。汇编核对：新增的 `udiv` 都在分支之后
（整个编译单元 **0 条 `cmov`**），pow2 路径上不执行除法。

**④ 功能测试无变化**：`real_lifecycle` 三个索引仍然精确归位（128/128/128）；
`race` 的假阴性仍是 0.0362%（P1-4 是另一个问题，这个补丁不碰它）。

#### 顺带要改的两处文字（不在补丁里，因为不在这个文件）

- `tb/tools/oms_bench.cpp` 的 `--help`：`must be power of 2 **for speed**`
  → 改成 `索引容量会向上取整到 2 的幂（这不是性能建议，是索引探测的正确性要求）`。
- `include/oms/README.md` 的"建议 2^N" → "**必须** 2 的幂"。

#### 这个补丁**没有**碰的东西

- `kDefaultCapacity` 保持 `100'000`（不需要改，见上面的内存对照）。
- seqlock（P1-6）、`find_slot` 校验（P1-7）、别名失败计数（P1-5）、
  `open()` 里 read-only-on-empty 的 fd 泄漏（P2-3 #1）—— 都是独立的问题，留给后续。

#### ⚠️ 上线动作

`index_capacity` 变了，所以**已有的 shm 文件必须删掉重建**。不删也不会跑错数据 ——
`open()` 会直接抛错并打印要删的路径（见上面 ② 的报错信息）。默认路径是
`/dev/shm/tb_oms.dat`，重启机器也会自动清空。

### P1-4 · `lookup_*` 把「不存在」和「写者忙」混为一谈（实测）

`read_slot_snapshot:525-540` 在 `kMaxReadRetry = 16` 次之后放弃，`lookup_impl` 直接
`return false` —— 跟"索引里没有这个 key"**返回同一个值**。调用方无法区分。

**实测**：1 个写线程对同一单反复重报（模拟正常 fill / partial-fill 流），3 个读线程并发查：

```
writes=3569808  reads=13003005  spurious_misses=4711  (0.0362%)
```

**约 1/2800 的查询会把一条活单报成"不存在"。**

诚实的说明：我的写线程跑了 ~240 万次/秒，比真实 OMS 处理 execution report 的速率高
约 3 个数量级，所以生产环境的实际比率会低得多。但这是**真实的假阴性路径**，
而且 `iterate_live` / `lookup_*` 正是重启对账要用的接口——对账把活单当死单，
后果比"漏一个查询"严重。

**修法**：把返回类型改成三态（`FOUND` / `ABSENT` / `BUSY`），或让调用方传入重试预算。
最省事的止血：`read_slot_snapshot` 失败时不要当成 absent，而是抛/返回一个可识别的错误。

### P1-5 · 别名索引插入失败被静默吞掉

`insert_all_indices:929-938`：主索引（`orderSysId`）失败 → 整体失败、上层回滚；
**别名（`clientOrderId` / `orderId`）失败只打一行 `stderr` WARN，然后 `return true`**。

```cpp
if (!insert_index(IDX_CLIENT_ORDER, s.clientOrderId, idx)) {
    std::fprintf(stderr, "[OmsShm][WARN] clientOrderId alias insert full slot=%u\n", idx);
}
...
return true;   // ← 上层以为成功了
```

`update_slot:901-903` 更是**连返回值都不看**：

```cpp
if (orderId_changed && s.orderId[0]) {
    insert_index(IDX_EXCHANGE_ID, s.orderId, idx);   // 返回值丢弃
}
```

后果：`lookup_by_client` / `lookup_by_orderId` 静默失效，而
- `Stats` 里没有任何别名失败计数（`total_alloc_failures` 不增，实测确认为 0）；
- `oms_query --stats` 看不出来；
- `doctor` 也看不出来。

**修法**：加 `total_alias_insert_failures`（至少按索引分 2 个），并在 `upsert` 返回值里
体现"主键成功但别名缺失"。

---

### 2.3 索引的"身份判定"：`hash + 前 16 字节`（B5，2026-10-06 16:34 新增）

> 起因：使用者提问"index 仍然用 hash + 前 16 字节判断 key，这个有问题吗"。
> 结论：**有问题，但问题不在碰撞概率，而在这个判定被当成了身份判定。**

#### 判定长什么样

```cpp
if (h == hash) {                                          // index_probe_find:261 / insert_index
    size_t cmp_n = key.size() < 16 ? key.size() : 16;
    if (std::memcmp(e.key_prefix, key.data(), cmp_n) == 0) { /* 认定为同一个 key */ }
}
```

即：**`fnv1a(key)` 相等 且 前 16 字节相等**。它不覆盖 key 的第 17 字节以后，也不把长度
作为独立判据。

#### ① 碰撞概率：**不是问题**

64 位 FNV-1a，N 个 key 的期望碰撞数 ≈ `N²/2^65`：

| N | 期望碰撞数 |
|---|---|
| 4 000 | 4.3e-13 |
| 100 000 | 2.7e-10 |
| 1 000 000 | 2.7e-8 |

实测：4000 个 key 无碰撞。**靠随机撞上基本不会发生。**

#### ② 前 16 字节前缀：对两类真实 key **等于没有**

实测（`/tmp/omsshm_prefix/prefix_test.cpp`，N = 100000）：

```
orderId (自增 int64)     与前 16 字节重复: 100000 (100.0%)   不同前缀数=101
client (sid=9, cid 7位)  与前 16 字节重复:      0 (  0.0%)   不同前缀数=100000
client (sid=20)          与前 16 字节重复: 100000 (100.0%)   不同前缀数=1
```

- **`orderId`（交易所自增 int64）**：100000 个 key 只有 **101 个不同前缀** ——
  交易所 id 连号时，前缀完全不起区分作用。
- **`clientOrderId` 当 `strategyId` ≥ 16 字符**：100000 个 key 只有 **1 个前缀** ——
  前缀**完全退化**。（现网 `sss_test1` 是 9 字符，所以这一条暂时没踩到，但只要改个长
  `strategyId` 就会踩到；`compose_client_key` 允许 `strategyId` 长达 43 字符。）
- 只有 `strategyId` 短、且 cid 位数少（整个复合 key ≤ 16 字节）时，前缀才**恰好等于整个
  key**，此时才是精确比较。

**所以"hash + 前 16 字节"在两个真实场景里实际退化成"只有 hash"。**

#### ③ 真正的缺陷：写入路径不复核 **+ slot 会自相矛盾**（本次新发现）

碰撞概率虽小，但一旦发生，后果比"查不到"严重得多。原因是三件事叠加：

1. **`find_slot` 不复核**（`1053-1061`）—— 直接把 bucket 里的 `slot_idx` 返回。
2. **`upsert` 信任它**（`786-790`）—— `existing != kInvalidSlot` 就走"更新已有 slot"分支。
3. **`update_slot` 不更新 key 副本**（`946-970`）—— 它只覆盖 `s.order`（报单体）和
   `s.orderId`，**从不碰 `s.orderSysId` / `s.clientOrderId`**。
4. **`lookup_impl:572` 复核的是 slot 的 key 副本**（`snap.orderSysId`），
   而不是报单体里的 key（`snap.order.body.orderResponse.orderSysId`）。

第 3 + 4 条合起来意味着：碰撞后 slot 的 **key 副本（还是 A）与报单体（已是 B）不一致**，
而复核比的是 key 副本 → **复核通过**。

#### 实测复现

用 16 位 hash 的头文件副本（`/tmp/omsshm_prefix/weak16/oms/OmsShm.h`，只改了 hash 宽度，
代码路径与生产**逐字节相同**）搜到一对真实碰撞 key：

```
A = "COMMONPREFIX12341188"
B = "COMMONPREFIX12341294"
hash = 0x75d2      前 16 字节相同? 是     两个 key 本身不同? 是

upsert(A) -> slot=0    A 查得到吗: 是
upsert(B) -> slot=0    (B 是新单, 本应占一个新 slot)

slot 占用数 = 1 (插了 2 张单)
sa == sb ?   是   ← 两张不同的单落进同一个 slot
查 A: 命中      查 B: 查不到

---- 用 A 的 key 去查, 拿回来的是什么? ----
查 "COMMONPREFIX12341188"
→ orderSysId    = "COMMONPREFIX12341294"   ★ 不是我要的那张单!
→ clientOrderId = 222222   (A=111111, B=222222)
→ orderId       = "EX_B"
```

对照（两个 hash 不同的 key，同一份代码）：`slot 占用数 = 2`，两个都查得到 —— 行为正确。

**对 OMS 来说"查错单"是最坏的一类错误**：你会拿另一张单的状态去驱动后续动作
（判断是否成交、是否该撤、是否该平）。而"查不到"至少是保守失败。

#### 修法（优先级高于原 B1）→ ✅ 已实施，见 §2.4

1. **必修：索引命中只当"候选"，真判据改成回读 slot 比完整 key。**
2. **同时：让 `update_slot` 维护"key 副本 == 报单体"这个不变量。**
3. **可选加固（本次未做）**：扩 `key_prefix` 或换 128 位 hash ——
   这只是**降概率，不修原理**，概率本来就够低。

---

### 2.4 B5 改法（✅ **已应用** 2026-10-06 17:05）

补丁 `tb/tools/OmsShm_B5.patch`（7 个 hunk / 295 行）。`OmsShm.h` 1101 → **1265 行**
（md5 `104c8ca6…` → `d296efc4f36ac808ed52ff86f51cca30`）。
只动了一个文件，`tb/tools/*.cpp` / `*.sh` **一行没改**（工具只是消费者，接口没变）。

#### 一、身份判定：从"hash + 前缀"改成"hash 是桶提示，slot 是权威"

新增三个东西（`OmsShm.h:251-291`）：

```cpp
enum class EntryKeyMatch : uint8_t {
    kMatch,   // slot 确实持有这个 key
    kOther,   // slot 持有**别的** key —— 真碰撞, 必须继续探测, 绝不能当成命中
    kStale,   // 条目指向的 slot 越界 / 已空 —— 条目陈旧: 不是命中, 但该 bucket 可复用
};

// 按 kind 取 slot 的 key 副本 buffer
inline const char* slot_key_ptr(const OmsSlot*, uint32_t slot_cap, uint32_t slot_idx,
                                IndexKind, size_t& cap_out) noexcept;

// ★ 必须比**完整 key**。代价敏感: memcmp(key.size()) + 一个终止符检查, 不做 strlen 扫描
inline EntryKeyMatch entry_key_match(const IndexEntry&, const OmsSlot*, uint32_t slot_cap,
                                     IndexKind, std::string_view key) noexcept;
```

然后**三处探测/写入都过这道判定**：

| 位置 | 改法 |
|---|---|
| `index_probe_find:297` | `hash == hash && 前缀相等` 之后，**再回读 slot 比完整 key**。`kMatch` → 命中；`kOther` → **继续探测**；`kStale` → 记作可复用位置、继续探测 |
| `insert_index:1151` | 命中分支改成按 `EntryKeyMatch` 分派：`kMatch` 才覆盖 `slot_idx`；`kOther` **绝不覆盖**（覆盖就等于把别人的单顶掉）；`kStale` 记作可复用 |
| `find_slot:1212` | 靠 `index_probe_find` 的 `found`（true ⟺ `kMatch`），并补上 `slot_idx` 越界检查（**顺带把 B1 修了**） |

> **一句话**：`key_prefix[16]` 从"身份判据"降级为**快速否定过滤器**（16 字节比较比回读
> slot 便宜）。判定改读 slot 里的完整 key —— 那本来就是唯一权威的一份。

#### 二、不变量：`update_slot` 维护 "key 副本 == 报单体"

`lookup_impl:622` 复核的是 **key 副本**。要让这个复核有意义，副本就必须与报单体同源。
新增 `sync_key_copies:1068` + `sync_one_key:1091`，并在 `update_slot:1017` 里调用：

```cpp
const bool need_sync =                       // ★ 热路径: 4 个短比较, 全部为假就直接跳过
    (resp.orderSysId[0] && strncmp(s.orderSysId, resp.orderSysId, 64) != 0) ||
    (resp.orderId[0]    && strncmp(s.orderId,    resp.orderId,    64) != 0) ||
    (resp.strategyId[0] && (s.clientOrderId[0] == '\0' ||
                            s.order.body.orderResponse.clientOrderId != resp.clientOrderId ||
                            strncmp(s.order.body.orderResponse.strategyId,
                                    resp.strategyId, 32) != 0));
...
if (need_sync) sync_key_copies(idx, rcmd);   // 冷路径
```

三个必须踩对的地方：

1. **顺序**：`sync_one_key` 里必须**先 `tombstone_index(旧 key)`、再改副本、最后
   `insert_index(新 key)`**。反过来的话 `tombstone_index` 拿旧 key 去比 slot 的副本会比不中，
   旧条目就永远留在索引里了。
2. **"字段为空" ≠ "值变了"**：报单体里 `orderId`/`strategyId` 为空只表示**这次没带**，
   不能拿它去覆盖已建立的副本（否则会把 `clientOrderId` 别名索引写坏）。
   所以 `need_sync` 每个子句都以 `resp.xxx[0] &&` 开头 —— 上层不回传该字段时直接短路，
   不会掉进冷路径。
3. **`strategyId` 为空时不重算复合 key**：重算会得到一个与创建时不同的 key。

#### 三、实测：碰撞压力（16 位 hash 副本，代码路径与生产逐字节相同）

4000 个 key，**全部共享同一个 16 字节前缀** `"COMMONPREFIX1234"`，
其中 **314 个 key 落在真实 hash 碰撞桶里**：

| | 修复前 | 修复后 |
|---|---|---|
| `upsert` 后的 slot 占用 | **3842**（4000 张单，**158 张被静默并掉**） | **4000** ✅ |
| 查不到的 key | 158 | **0** ✅ |
| **查到但返回的是别人的单** | **156** | **0** ✅ |
| 更新一半后再查，查错/查不到 | 115 | **0** ✅ |

单对碰撞的端到端复现（`A="COMMONPREFIX12341188"` / `B="…1294"`，hash 都是 `0x75d2`）：

```
修复前: upsert(A)->slot=0  upsert(B)->slot=0   slot 占用=1   查 B: 查不到
        查 A → orderSysId=B / clientOrderId=222222 / orderId=EX_B   ★ 不是我要的单
修复后: upsert(A)->slot=0  upsert(B)->slot=1   slot 占用=2   查 A: 命中  查 B: 命中
        查 A → orderSysId=A / clientOrderId=111111 / orderId=EX_A   一致 ✅
```

**顺带修掉一个索引泄漏**（`real_lifecycle` 的 V3：同一单 `orderId` A→B）：
`IDX_EXCHANGE_ID` 的 live 条目从 **200（100 张单，旧条目全留着）** 变成
**100（+65 tombstone）**。旧代码只 insert 不 tombstone，旧条目会永久累积。

#### 四、性能：逐项定位过了，不是拍脑袋

`update` 热路径（`upsert` 打到同一张单）与 `lookup` 路径的单线程实测：

| 变体 | update ns/op | 说明 |
|---|---|---|
| 修复前 | **40** | — |
| + `need_sync` 判定 | 41 | **+0.6** —— 4 个短比较，几乎免费 |
| + 探针里的 slot 复核 | 50 | **+9.3** —— 回读 slot 比完整 key |
| + `find_slot` 事后复核 | 54 | **+3.9** |
| + `sync_key_copies` 调用点 | 60 | **+5.5** |
| **最终**（去掉冗余的事后复核 + 换掉 `strnlen`/`string_view` 写法） | **52** | **+12** |

| 路径 | 修复前 | 修复后 |
|---|---|---|
| `lookup`（reader 侧主力） | 35.2 ns/op | **35.0 ns/op** ✅ 无变化 |
| `update` | 40 ns/op | **52 ns/op**（+30%，绝对量 12 ns） |

**两个中途踩到的坑，都记一下**（都属于"改完必须量一遍"）：

1. 第一版把 `compose_client_key`（里面有 `snprintf`，约 100ns）放进了**每次** `update` 的路
   → update 直接从 40 冲到 **74 ns/op**，`oms_bench` 的写吞吐掉了 28%。
   改法：先用一次 `strncmp(32)` + int64 比较判断"源字段变了没"，只有变了才重算。
2. `find_slot` 里我一开始又 `entry_key_match` 了一次 —— 但 `index_probe_find` 的
   `found_out` 已经保证是 `kMatch`（`kOther`/`kStale` 都不会置位），这次是纯冗余，删掉。

> 12 ns 的绝对代价 = 单线程 1900 万次 update/秒。实盘 OMS 的成交回报是千级 msg/s，
> 这个量级完全无所谓；而 reader 侧（会被高频轮询的那条路）**一点没变**。

#### 五、回归验证（全部在**真实文件**上重跑）

| 用例 | 结果 |
|---|---|
| 碰撞复现（单对） | 2 个 slot / 都查得到 / 各返回各的报单体 ✅ |
| 碰撞压力 4000 key / 314 碰撞（16 位 hash） | 0 并单 / 0 查错 / 0 查不到 ✅ |
| 碰撞压力 4000 key（真 64 位 hash） | 干净 ✅ |
| 生命周期 + 不变量（含 20 轮 update、reclaim、remove） | 全过，**stderr 0 行**（正常路径不该有 WARN）✅ |
| A1/P1-1 多容量扫描（65536 / 65537 / 100000 / 1024 / 3） | `all cases OK` ✅ |
| tombstone 稳态（200 轮 × 200 单） | 仍为 `256/256/0`，reclaims=39744，fail=0 ✅ |
| `orderId` 生命周期 4 阶段 + V2/V3 | 与修复前一致；V3 的 `EXCH live` 200 → **100**（泄漏修掉）✅ |
| 并发 race（1 写 3 读，1.5 s） | 假 miss **0.0049%**（修复前 0.008–0.011%）✅ |
| `oms_query` / `oms_bench` / `oms_demo` 编译 | `-Wall -Wextra` 零告警 ✅ |
| `oms_shm.sh doctor`（健康文件 / 有 alloc_failures 的文件） | `✓ healthy` / `✗ alloc_failures = 68936` ✅ |

撤销：`patch -R -p1 < tb/tools/OmsShm_B5.patch`（已验证能逐字节还原）。

---

### 2.5 `kMaxProbeIndex = 32` 够不够？（B6，2026-10-06 17:25 新增）

> 起因：使用者提问"`kMaxProbeIndex=32` 这个需要修改吗"。
> 结论：**要改，而且不是"余量偏紧"，是已经在丢单了。**
> 但**要改的不是这个常数，是 `index_capacity`** —— 见下面的实测对照。
> **✅ 已应用 2026-10-06 18:05，改法 / 实测 / 验证见 §2.6。**

#### 一、为什么这个 32 是索引的**唯一**余量

先看清结构。索引里有三种桶：`empty`（从未用过）、`tombstone`（已删，可复用）、`live`。
- 每次插入消耗一个可复用桶（empty 或 tombstone）；
- 每次 reclaim 产生一个 tombstone（`tombstone_all_indices_of_slot`）；
- 插入数 == reclaim 数（稳态），所以每个循环净效果是 **`empty` −1、`tombstone` +1**。

于是 `empty` **必然单调排干到 0**，而且**跟索引开多大无关** —— 开到 4 倍、8 倍只是排干得慢一点，
最后一样是 100% 占满。实测稳态永远是：

```
live + tomb == index_capacity        empty == 0
```

一旦 `empty == 0`，**每一次插入能否成功，完全取决于 32 步之内能不能碰到一个 tombstone**。
`kMaxProbeIndex` 就是索引仅剩的那点余量。

#### 二、实测：最长 live 连续段已经超过 32

`index_probe_bucket` 是线性探测，所以"插入要走的步数" = "home 桶到下一个可复用桶的距离"，
上界就是**最长的连续 live 段**。测了五个容量档（每档灌满后再 churn 10~30 轮）：

| `slot_cap` | `index_capacity`（=2N） | log₂ | **最长 live 连** | 最坏插入探测 | `alloc_failures` | **丢单率** |
|---|---|---|---|---|---|---|
| 1 024 | 2 048 | 11 | 19 | 20 | 0 | 0 |
| 8 192 | 16 384 | 14 | 35 | 36 | 12 | 1.3e-4 |
| 65 536 | 131 072 | 17 | **38 ~ 55** | 39 ~ 56 | 388 / 203 万 | **1.9e-4（1/5235）** |
| 262 144 | 524 288 | 19 | **41 ~ 49** | 42 ~ 50 | 77 | 3.3e-5 |
| 1 048 576 | 2 097 152 | 21 | **38 ~ 43** | 39 ~ 44 | 301 | 3.2e-5 |

**最长连续段 33~56，全部超过 32。** 而且它随容量缓慢增长（19 → 35 → 38~55），
所以**任何固定常数都只是把门槛往后挪**，不是修好。

**还会随时间恶化**（`slot_cap = 8192` 固定，只加 churn 轮数）：

| churn 轮数 | 插入总数 | `alloc_failures` | 丢单率 |
|---|---|---|---|
| 2 | 24 575 | 1 | 4.1e-5 |
| 5 | 49 147 | 5 | 1.0e-4 |
| 10 | 90 100 | 12 | 1.3e-4 |
| 30 | 253 882 | 70 | 2.8e-4 |
| 60 | 499 586 | 126 | **2.5e-4（1/3965）** |

→ 稳定在 **~2.5e-4，约 1/4000 的单会被丢掉**。跑的越久越接近这个值。

> **排除伪因**：三族 key 形状完全不同（`K…` / `sss_test1…` / `E…`），都出现同样现象；
> 且 hash 低 8 位的卡方值 140.9（df=255）—— 不但不偏斜，还比随机更均匀。
> 所以这是**线性探测的聚集效应**，不是测试 key 把 hash 打歪了。

#### 三、失败意味着什么

`insert_index(IDX_ORDER_SYS_ID, …)` 返回 false → `insert_all_indices` 返回 false →
`upsert` 回滚 slot、`total_alloc_failures++`、返回 `kInvalidSlot`。
**这张单没有进 SHM** —— 后续 query / 对账都找不到它。与 P1-1 同一类后果（静默丢单），
只是成因不同。A4 之后日志会喊出来，但单还是丢了。

#### 四、两个改法，实测对照

| 方案 | `index_capacity` | 最长 live 连 | 丢单 | SHM 大小（10 万 slot） | lookup ns/op |
|---|---|---|---|---|---|
| 现状（限 32，索引 2N） | 131 072 | 38 | **388** | 121.7 MiB | 35.1 |
| **A：只把限值调到 128** | 131 072 | 38 | 0 | 121.7 MiB | ~35 |
| **B：索引改成 4N（限值不动）** | 262 144 | **13** | **0** | **145.7 MiB（+24）** | 35.1 |
| A + B | 262 144 | 13 | 0 | 145.7 MiB | ~35 |

**建议 B，不建议 A。** 理由：

1. **A 治标**：限值只是门槛，最长连续段还在随容量长；把 32 改成 128 之后，
   下一个容量档又要重估一次，永远说不清余量。B 治本：把**可复用桶的密度**从 0.5 提到 0.75，
   聚集效应被压下去，最长连续段直接塌到 13。
2. **B 的余量是量出来的**，而且**不随容量恶化**：
   `slot_cap` = 65 536 / 262 144 / 1 048 576 → 最长连续段 13 / 15 / 19~20。
   一百万个 slot 时仍然只有 20，对 32 有 **1.6 倍余量**。
3. **B 几乎不花钱**：内存 +24 MiB（+20%，索引从 24 MiB 到 48 MiB）；
   **速度完全没变**（update 51.5 → 51.6、lookup 35.1 → 35.1 ns/op）。
   顺带平均查询探测从 2.45 降到 1.44。
4. 想更稳可以 A+B 一起做（余量 ~2.5 倍），但 **B 是必做的那一半**。

#### 五、改 B 的注意事项

- `next_pow2(4N)` **仍然是 2 的幂**，所以 `open()` 里那条"`index_capacity` 必须是 2 的幂"
  的老文件检查**挡不住它** —— 旧文件（2N）会被照常接受，于是修复**对旧文件静默失效**。
  必须把 `kVersion` 2 → 3：现有的 `tmp_hdr->version != kVersion` 检查会直接拒绝旧文件，
  报 `magic/version mismatch`。**运维动作：删掉现有 shm 文件重建。**
- 建议顺手加两条断言 / 可观测量：
  ① `open()` 时校验 `index_capacity >= 4 * slot_capacity`（新文件）并给出明确报错；
  ② 把"实测最长 live 连续段"或"探测步数分布"放进 `Stats` / `oms_query --stats`，
  让余量像 A1 的占用率一样**看得见** —— 否则下次还会以同样的方式踩到。

#### 六、复现

```bash
# /tmp/omsshm_probe/headroom.cpp  —— 只用公开 API, 不改头文件
clang++ -std=c++17 -I include -I /tmp/omsshm_check/stub headroom.cpp -o headroom
./headroom 65536 30      # 现状: 最长连 38, alloc_failures=388
./headroom 262144 8
# 变体: /tmp/omsshm_probe/{lim128,idx4x,both}/oms/OmsShm.h
```

---

### 2.6 B6 改法（✅ **已应用** 2026-10-06 18:05）

补丁：`tb/tools/OmsShm_B6.patch`（**25 个 hunk / 522 行**，`patch -p1` 干净，
正反向都验证过：正向应用后 6 个文件与仓库逐字节一致，反向能干净还原）。
备份：`/tmp/omsshm_b6/OmsShm.h.pristine-20261006-1746`（+ `oms_query.cpp` / `oms_shm.sh`
的 pristine 副本，README 的 pristine 用"逐条反向替换"重建后 diff 校验过）。

| 文件 | 行数 | 改了什么 |
|---|---|---|
| `include/oms/OmsShm.h` | 1265 → **1338** | 常量、`open()`、`Stats`、`index_occupancy_of()`、回滚日志 |
| `tb/tools/oms_query.cpp` | 267 → **287** | `--stats` 打印 `probe_max` / 每个索引的 `max_live_run` + 余量倍数 + `worst_live_run` 汇总行 |
| `tb/tools/oms_shm.sh` | 182 → **199** | `doctor` 增加探测余量检查（err / warn 两级）；`check` 的 /dev/shm 阈值 256 → 384 MB |
| `tb/tools/oms_bench.cpp` | 330 | `--help` 措辞（D4） |
| `tb/tools/oms_demo.cpp` | 330 | 同上 |
| `include/oms/README.md` | 883 | 索引 2N → 4N、内存 120 → 146 MB、`shm_size`、Q2 改写、新增 Q7 |

#### 一、六处代码改动

1. **`kVersion` 2 → 3**（`OmsShm.h:58`）。**这一条不能省** —— `next_pow2(4N)` 仍然是 2 的幂，
   所以 `open()` 里那条"`index_capacity` 必须是 2 的幂"的老文件检查**挡不住 2N 的老文件**，
   修复会对已有 shm **静默失效**。靠版本号让现成的 `tmp_hdr->version != kVersion` 拒绝它。
2. **`use_idx = next_pow2(use_cap * 4)`**（原 `* 2`），注释写明为什么是 4 而不是 2。
3. **`slot_cap` 上界 `2^30` → `2^28`**。原因：`index_capacity = next_pow2(4N)` 必须能放进
   `uint32_t`，`4 × 2^28 = 2^30` 是上限；再大时 `use_cap * 4` 先溢出成 0，`next_pow2(0)`
   返回 **1** —— 索引变成 1 个 bucket 的**静默写坏**。报错文案里直接写出这个约束。
4. **打开已有文件时加倍数校验**：`if (hdr_idx < (uint64_t)hdr_cap * 4) throw ...`，
   用 64 位比较避免 `4 * hdr_cap` 溢出。这是版本号之外的**第二道**防线（防手搓 / 损坏文件）。
5. **`Stats::IndexOccupancy` 加 `max_live_run`，`Stats` 加 `probe_max`**，
   `index_occupancy_of()` 在**同一个循环里**顺手统计最长连续 live 段，不额外扫一遍。
   两处实现细节：
   - 探测是环形的（`(hash+i) & mask` 会绕回 0），所以**尾部那段和从 bucket 0 开始的那段
     在探测意义上是连续的，必须合并**，否则会低估。用 `head_run + tail_run` 合并。
   - `probe_max` 单独放进 `Stats`，是为了让工具不用去解析 `kMaxProbeIndex` 或括号里的文字。
6. **回滚日志判读从三段简化成两段**。原来写了"可复用 bucket 太少 → 加大 slot_cap"这一支，
   但按 §2.5 的机制它**基本不可达**：`max_live_run < probe_max` 时任何插入都能在
   `probe_max` 步内碰到可复用桶，就不会失败。留一个不可达的分支 = 留一句会误导人的建议
   （和 A4 修掉的 "tombstone leak" 是同一类错误）。现在只剩：
   - `max_live_run >= probe_max` → **聚集**，加大 `slot_cap` 没用，要加大 `index_capacity` 倍数；
   - 否则 → "本不该失败，请连同快照上报"。

#### 二、实测（`slot_cap = 65536`，灌满 + churn 30 轮，同一份 `headroom.cpp`）

| | 最长 live 连 | 最坏插入探测 | 丢单 | 稳态占用 | 查询探测 mean | SHM |
|---|---|---|---|---|---|---|
| 改前（2N） | 38 / 33 / 34 | 39 / 34 / 35 | **388 / 203 万（1.9e-4）** | 65531 live + 65541 tomb + 0 empty | 2.45 | 121.7 MiB |
| **改后（4N）** | **13 / 13 / 10** | **14 / 14 / 11** | **0** | 65536 live + 196598 tomb + 10 empty | **1.44** | **145.7 MiB** |

余量随容量的表现（改后）：`slot_cap` = 1 024 / 8 192 / 65 536 / 100 000 / 262 144 / 524 288
→ 最长连续段 **6 / 10 / 13 / 10 / 14 / 16**，全部远低于 32。
（`100 000` 那一档反而更小，因为 `next_pow2(400000) = 524288` 相当于 5.24N。）
速度：update 51.5 → 51.6、lookup 35.1 → 35.1 ns/op，**没有变化**；内存 +24 MiB。

#### 三、验证（全部在**真实文件**上重跑）

| 用例 | 结果 |
|---|---|
| `/tmp/omsshm_b6/verify.cpp`（新写，23 条断言） | **23 passed / 0 failed** —— 容量算式、`version==3`、`probe_max`、`max_live_run` 与独立暴力实现逐项一致、多容量丢单 0、老文件三种拒绝路径、`slot_cap` 上界 |
| `/tmp/omsshm_b5/b5_stress.cpp`（16 位 hash 副本，碰撞压力） | 4000 个 key / 314 个落碰撞桶 → **0 并单 / 0 查错 / 0 查不到**，`索引 live = 4000` |
| `/tmp/omsshm_b5/b5_lifecycle.cpp`（B5 不变量） | 全 OK，**stderr 0 行** |
| `/tmp/omsshm_impl/real_lifecycle.cpp`（orderId 生命周期 + EXCH 泄漏） | V3 `EXCH live = 100`（不是 200），`alloc_failures = 0` |
| `/tmp/omsshm_impl/race.cpp`（seqlock 并发） | 假 miss **0.0050 ~ 0.0084%**（改前 0.0049 ~ 0.011%，同一条带；一次 0.030% 是机器负载下的离群值，连跑 3 次复现不出） |
| `oms_query --stats` + `oms_shm.sh doctor` 端到端 | 健康分支 `✓ worst_live_run = 12 (probe_max = 32)` / `✓ healthy`；把 `kMaxProbeIndex` 改成 1 的头文件副本 → `✗ 索引已饱和, 正在丢单`；改成 14 → `⚠ 余量不足 1.5 倍`。**三个分支都命中** |
| 回滚日志（`/tmp/omsshm_b6/log_test2.cpp`，手工把主索引填满 live 条目） | 打印 `probe headroom: max_live_run=256 probe_max=1`、`used_pct=100.00`，判读命中"聚集"那一支 |
| 三个工具 `-Wall -Wextra` | **零告警**（`oms_query` / `oms_bench` / `oms_demo`） |

> 两个坑记一下：① `verify.cpp` 第一版把 `reopen` 断言写错了（以为能靠入参改容量，
> 实际打开已有文件一律从 header 读）—— **是测试错，不是代码错**。
> ② `doctor` 的 warn 阈值第一版定成"余量不足 2 倍"，但 `slot_cap ≥ 512K` 时最长连续段
> 本来就会到 16，会在**健康**的大容量部署上误报；改成 **1.5 倍**（`3*run >= 2*probe_max`）。
> 断言/阈值也得跟着容量档重估，不能拿一个常数套到底 —— 和 B6 本身是同一个教训。

#### 四、上线动作

1. **删掉现有 shm 文件重建**（`CONFIRM=1 ./oms_shm.sh reset` 或直接 `rm`）——
   `kVersion` 变了，老文件会被 `magic/version mismatch` 拒绝；即使不删也起不来。
   重建会丢掉 shm 里的活单状态，所以**先撤单**。
2. `/dev/shm` 容量要求从 ~120 MB 提到 **~146 MB**（100k slot），
   Docker / k8s / fstab 的 size 建议给到 384 MB。
3. `doctor` 从下一版起会多打一行 `worst_live_run`，接入监控时把它和 `total_alloc_failures`
   一起看：`worst_live_run >= probe_max` 就是"正在丢单"。

#### 五、撤销

```bash
patch -R -p1 < tb/tools/OmsShm_B6.patch
```

（注意：撤销后 `kVersion` 回到 2，此时**必须**把 shm 文件删掉重建，否则新文件会被当老文件拒。）

### 2.7 v4 改法（✅ **已应用** 2026-10-06 18:45）—— `alloc_slot` 窗口丢单 + `sync_one_key` 丢弃返回值

使用者提了三个"快路径 + 兜底"提案，逐个评估后：

| # | 提案 | 结论 |
|---|---|---|
| ③ | `alloc_slot()` 128 次 fast path + full scan fallback | **要改** —— 实测正在丢单 |
| ② | `sync_one_key()` 检查 `insert_index()` 返回值 | **要改** —— 一行，且失败后果比预想严重 |
| ① | `index` 32 次 fast path + full-table fallback | **不改** —— 见 §2.7.4 |

#### 一、③ `alloc_slot()` 只扫 128 步就放弃

`alloc_slot()` 只扫 `next_slot_hint` 起**连续** `kMaxProbeSlots`(=128) 个 slot，扫不到就
`return kInvalidSlot`；而每失败一次 hint 都 `fetch_add(1)`，所以失败时 hint 已经前进 128
—— **下一次调用看的是别的区域，这张单没有第二次机会**。更糟的是 `upsert` 拿到
`kInvalidSlot` 只 `total_alloc_failures++` 就返回，**一行日志都没有**
（对比索引插入失败那条路径有完整快照）→ 丢单在日志里完全静默。

失效前提是"**连续 128 张未 finalize 的在途单**"，这是**常规输入、没有设计上界**。

实测（`/tmp/omsshm_b7/alloc_scan.cpp`，cap=4096，`min_reclaim_age=0`，6 轮；
先造一段连续 LIVE 块挡在 hint 前面，其余 slot 走"建单 → 立即 FINISHED"的 churn；
每次 `upsert` 前把"128 步窗口扫"和"全表扫"各模拟一遍，再和实际结果对照）：

| 连续 LIVE 块 | 丢单 | 丢单率 | 性质 |
|---|---|---|---|
| 64 | 0 | — | 窗口够用 |
| 128 | 6 | 2.44e-4（1/4096） | 全部 *本可避免* |
| 129 / 200 | 6 | 2.44e-4 | 全部 *本可避免* |
| 512 | 24 | 9.77e-4（1/1024） | 全部 *本可避免* |
| 1024 | 56 | 2.28e-3（1/439） | 全部 *本可避免* |
| 2048 | 176 | 7.16e-3（**1/140**） | 全部 *本可避免* |

`real_exhaust` 全程 = **0**：每一次都是"窗口内空、全表却有可回收 slot"。阈值精确落在 128
（64→0，128→6）。hint 序列 `4224 → 4352 → 4480` 每步 +128，直接证实失败**不重试同一区域**。

#### 二、③ 改法：快路径 128 步 + 全表兜底

```cpp
uint32_t alloc_slot() {
    uint32_t idx = alloc_slot_scan(kMaxProbeSlots);   // 快路径: 热路径唯一成本
    if (idx != kInvalidSlot) return idx;
    const uint32_t cap = slot_capacity();
    if (cap > kMaxProbeSlots) {                       // cap <= 128 时快路径已扫过全表
        idx = alloc_slot_scan(cap);                   // 慢路径: 全表再扫一遍
        if (idx != kInvalidSlot) { /* WARN(限流) + total_alloc_slowpath++ */ return idx; }
    }
    /* total_alloc_exhausted++ + alloc_exhausted_report(n) */;
    return kInvalidSlot;
}
```

四个设计要点：

1. **`alloc_slot_scan(n)` 只是把原循环参数化**，快路径的指令序列逐条不变 —— 热路径零成本
   （`oms_bench` 实测吞吐不变，FINAL STATS 逐字节相同）。
2. **慢路径不重置 `next_slot_hint`** —— 重置会让并发 writer 反复扫同一段。
3. **两处日志都限流**（前 8 条必打，之后每 4096 条一条）。环满之后**每一张单**都会走到
   `alloc_exhausted_report`，不限流就是刷屏：第一版忘了限流，实测 4096 次丢单打出 **4096 行**。
   计数永远准，日志只是采样。
4. **真·环满补上诊断**：再走一遍 slot 统计 live / finished / reclaiming / empty，并算出
   "最年轻的 FINISHED 距现在多久"，用来区分"全是 LIVE 未终态"和"FINISHED 没过 TTL"。
   这条路径以前在 `upsert` 里是**完全静默**的。

#### 三、② `sync_one_key` 丢弃 `insert_index` 返回值

旧顺序：`tombstone_index(旧)` → `memcpy(新)` → `insert_index(新)`（**返回值被丢弃**）。
一旦 insert 失败就是"旧条目已删、副本已改、新条目没挂上" → **新旧两个 key 都查不到**，
slot 彻底不可达（不只是"新 key 查不到"）。而且它照样打 "已迁移索引" 的 WARN，
日志上看不出出过事。（`insert_all_indices` 对别名失败是"WARN 但仍 `return true`"，
这里连 WARN 都没有 —— 与既有约定不一致。）

新顺序：`insert_index(新)`（看返回值）→ 失败就 WARN + 计数 + **整体不动**
→ 成功才 `tombstone_index(旧)` → `memcpy(新)`。

顺序约束（`sync_key_copies` 的注释里写着）：`tombstone_index` 是拿 slot 里的**当前副本**
去比对的，所以必须先 tombstone 旧 key 再改副本。把 insert 提到最前面正好两全 ——
插入成功时副本仍是旧值，tombstone 比得中旧 key。

实测（`/tmp/omsshm_b8/sync_invariant.cpp`，`kMaxProbeIndex=1` 的头文件副本 ——
和验 `doctor` 阈值同一个手法，只要 home bucket 被别的 key 占了 insert 必失败；
64 个 LIVE 单 / 256 个 bucket，改 5 轮 orderId，共 320 次）：

| | 旧代码 | 新代码 |
|---|---|---|
| round 1/2/3/4/5 不变量破坏 | 2 / 4 / 12 / 2 / 12 | 0 / 0 / 0 / 0 / 0 |
| **不变量破坏合计** | **32 / 320（10%）** | **0 / 320** |
| insert 失败保持旧 key | 0（旧代码没这个概念） | 40 |
| 旧 key 残留（应为 0） | 0 | 0 |
| `total_key_sync_failures` | — | 40（与"保持旧 key"完全一致） |

"旧 key 残留 = 0" 同时证明**成功路径没被改坏**：tombstone 仍然发生在 memcpy 之前。
（两轮之间 40 vs 32 的差异是因为处理方式不同 → 后续 key 落桶不同 → 碰撞模式发散，正常。）

#### 四、① index 32 步 —— 评估后**不改**

形状和 ③ 一样（N 步窗口 + 放弃），但触发前提不同：

- ③ 的失效条件是"**连续 128 个不可回收 slot**"，等价于"在途单 ≥ 128 张" ——
  **无设计上界、随时可达的常规输入**。
- ① 的失效条件是"**连续 32 个 live 桶**"（hash 聚集）—— B6 已用 `index_capacity = 4N`
  把它压到 6~16，余量 **2.0× ~ 5.3×**（1024→5.3×，8192→3.2×，65536→2.5×，100000→3.2×，
  262144→2.3×，524288→2.0×）。**有设计上界**，且已被 `max_live_run` / `probe_max` /
  `doctor` 监控着。
- 加兜底会把 `total_alloc_failures` **静音** → 失去"容量不够"这个唯一信号。
  要加也只该加在 `insert_index`（丢数据路径）；**不要**加在 `index_probe_find` ——
  那是查找路径，回退一次就是 O(index_capacity)（100k slot → 524288 桶），
  而且会把"索引过载"这个事实藏起来。

#### 五、v4 新增的三个计数

`OmsShmHeader` 加了 `total_alloc_slowpath` / `total_alloc_exhausted` /
`total_key_sync_failures`，**全部落在原来的保留 `pad` 里**（`pad[4096-112]` →
`pad[4096-112-24]`，`sizeof` 仍是 4096，有 `static_assert`），所以**不需要再 bump
`kVersion`**：旧文件这几个字段读出来就是 0，而 0 的语义恰好是"从未发生"。

关系：`total_alloc_failures = total_alloc_exhausted + 索引插入失败次数`；
`total_alloc_slowpath` 是**被救回来**的次数，**不计入失败**。

`oms_query --stats` 三个数值行都以数字结尾（`doctor` 靠这个取值），`doctor` 新增三个分支：
`slowpath > 0` → ⚠ 预警；`exhausted > 0` → ✗；`key_sync_failures > 0` → ✗。

#### 六、验证

| 用例 | 结果 |
|---|---|
| `alloc_scan` A/B（cap=4096，5 种 LIVE 块） | 丢单 **6 / 24 / 56 / 176 → 全 0**；`total_alloc_slowpath` == 模拟的慢路径次数（独立交叉验证） |
| 真·环满（cap=4096 全 LIVE，1 轮 4096 次） | `total_alloc_exhausted=4096`、`total_alloc_failures=4096`、诊断行数 **9**（限流生效） |
| 慢路径限流（live_block=2048） | 11 次事件 → **8** 条日志，`total_alloc_slowpath=11` |
| `sync_invariant` A/B（320 次改 key） | 不变量破坏 **32 → 0**；旧 key 残留 0 |
| `verify_v4.cpp`（新增） | **12 passed / 0 failed** |
| B6 回归 `verify.cpp` | **23 passed / 0 failed** |
| B6 回归 `b5_stress` / `b5_lifecycle` / `real_lifecycle` / `probe_len` / `headroom` / `log_test2` | 全部 all-OK / 0 失败；`real_lifecycle` V3 仍是 `EXCH live=100`；`headroom` 丢单率 0 |
| `race`（8 次） | 假 miss 0.0046% ~ 0.0378%，**低端与 B6 基线（0.0049%）一致** → 无回归（差异是机器负载） |
| `oms_bench` 性能 A/B（fresh 文件，cap=16384） | 吞吐 3.08 / 5.23 / 7.62M vs 2.99 / 5.47 / 8.21M ops/s；**FINAL STATS 逐字节相同**（`live=10923 finished=5461`） |
| `doctor` 四个 fixture（`clean` / `slowpath` / `exhausted` / `keysync`） | ✓ healthy / ⚠ / ✗×2 / ✗ —— 四个分支全命中 |
| `doctor` B6 探测余量三支 | 仍然全部命中（`probe_max=1` → ✗ 饱和；`=14` → ⚠ 余量不足；`=32` → ✓） |
| 三个工具 `-Wall -Wextra` | **零告警** |

#### 七、顺带修掉的

- **C1**：`reset_all()` 漏了 `total_stale_live_reclaims`（`--reset` 后 `doctor` 会误报卡单）
  —— 本次一并补上，`verify_v4.cpp` 有断言。
- **C6 的一部分**：新加的两处日志都带限流；`doctor` 也能从计数看出问题，不必靠日志刷屏。

#### 八、上线动作

**无额外运维动作。** v4 没有改 shm 布局（`sizeof(OmsShmHeader)` 仍是 4096、`kVersion`
仍是 3），所以**不需要再删 shm 文件**；B6 那次要删的原因（`kVersion` 2→3）依然成立，
两件事合并成一次即可。

#### 九、撤销

```bash
patch -R -p1 < tb/tools/OmsShm_v4.patch
```

### 2.8 v4.1 改法（✅ **已应用** 2026-10-06 19:22）—— 构建 `-pthread` + B4 别名计数 + A2 跳过计数 + 测试补齐

本轮起点是用户报的一个**构建错误**，顺带做了一次复查，并把散在 `/tmp` 的一次性 harness
收敛成 `tb/tools/oms_test.cpp`（可在仓库里长期跑的断言集）。

#### 一、用户报的构建错误：`undefined reference to pthread_create`

**根因**：`oms_shm.sh` 的 `cmd_build` 只给 `oms_bench` 加了 `-pthread`，
`oms_query` / `oms_demo` 没有。而**真正需要 pthread 的是构造了 `std::thread` 的 TU**。

先纠正一个容易搞错的判断：`OmsShm.h:40` 有 `#include <thread>`、`:645` 有
`std::this_thread::yield()`，但那一行在 `#if defined(__x86_64__)` 的 **`#else` 分支里**
—— 目标机 x86-64 根本不编译它。**"头文件 include 了 `<thread>`" 本身不会产生任何
`pthread_create` 引用**。用 `nm -u` 数一下每个 TU 的未定义符号，结论是确定的：

| TU | 引用 `pthread_create`？ | 改之前有 `-pthread`？ |
|---|---|---|
| `oms_query.cpp` | **0** | 无 —— 而且**本来就不需要** |
| `oms_bench.cpp` | 1 | 有 |
| `oms_demo.cpp` | **1** | **无** ← **报错的就是它** |
| `oms_test.cpp`（本轮新增） | 1 | 新增 |

复现命令（macOS 上也能跑，看的是符号而不是链接行为）：

```bash
cd tb/tools
for t in oms_query oms_bench oms_demo; do
  g++ -std=c++17 -O2 -pthread -I../../include -I../include -c $t.cpp -o /tmp/$t.o
  echo "$t: $(nm -u /tmp/$t.o | grep -c pthread_create)"
done
```

**为什么会报出来**：`cmd_build` 按 query → bench → demo 顺序编译，前两个成功、
`oms_demo` 链接失败 → 脚本 `err "build oms_demo failed"; return 1`，用户看到的就是
那一行 `undefined reference to 'pthread_create'`。能不能复现还取决于 glibc 版本：
**glibc ≥ 2.34（Ubuntu 22.04+）把 pthread 并进了 libc，不加 `-pthread` 也能链上**；
**glibc < 2.34（Ubuntu 20.04 / glibc 2.31）就会失败** —— 这也解释了为什么
"以前没发现"。

**改法**：`cmd_build` 三行编译命令**统一**加 `-pthread`（`oms_test` 同样加），
不依赖 `CXXFLAGS`（用户覆盖 `CXXFLAGS` 时也不会丢）。

```bash
$CXX $CXXFLAGS -pthread $INCLUDE_DIRS oms_query.cpp -o oms_query \
    && ok "oms_query"  || { err "build oms_query failed"; return 1; }
```

#### 二、本轮复查新发现的问题

| # | 位置 | 问题 | 后果 | 状态 |
|---|---|---|---|---|
| **B9** | `oms_query.cpp` `--stats` 的 `probe_max` 行 | 数值行行尾挂说明 | `doctor` 取"第一个数字"只是碰巧对；改文案就静默失真 | ✅ 已修 |
| **B10** | `alloc_slot_scan()` 开头 | `cap == 0` 时 `mask` 全 1 → 越界 / 除零 | 目前不可达，属防御性 | ✅ 已修 |
| **A2** | `iterate_state()` | 快照失败**静默跳过** | 对账把活单当不存在 | ✅ 已修（`skipped_out`） |
| **B4** | `insert_all_indices()` 别名分支 | 只打日志、无计数、**不限流** | 别名丢失运维看不到 + 环满时刷屏 | ✅ 已修（新计数 + 限流 + `doctor` 分支） |
| **D1** | `oms_demo.cpp` | 与 `oms_bench.cpp` **md5 完全相同**（`3e634ae85dc2310b2ab359a2d7525b00`） | README 却写着它演示 "create → insert × 5 → … → crash recover"；`./oms_shm.sh demo` 实际跑的是**性能测试** | ⚠ 仅更正文档，**未实现真 demo** |
| **C12** | `oms_shm.sh` `cmd_doctor` | 8 处 `((issues++))` | `((x++))` 在 `x==0` 时返回 **1**；本机 bash 3.2 不触发 `set -e`，但这是**版本相关**行为 | ✅ 改为 `issues=$((issues + 1))` |
| **C13** | `oms_shm.sh` `cmd_doctor` | 唯一**没有** auto-build 的子命令 | 没编译过时只报一句 "No such file or directory"（被 `2>&1` 吞进 `$out`），看不出是没编译 | ✅ 已加 |
| **C14** | `oms_shm.sh` `cmd_test` | 只判"二进制是否存在" | 改了源码后跑**陈旧二进制** → 测试全绿但验的是旧行为 | ✅ `cmd_test` 改为**每次强制重建** |
| **D3** | `OmsShm.h` 注释 | `[Slot 数组: capacity * 512 B]`（实为 1024）、`RCommand 本身 ~656B … ≈848B`（实为 536B / 768B） | 按注释估容量会算错一倍 | ✅ 已修 |

**C12 的实测**（值得记一笔，因为结论和直觉相反）：

```bash
# bash 3.2.57: ((x++)) 返回 1, 但**不**触发 set -e (顶层 / then-body / 函数体内均不触发)
set -eo pipefail; x=0; ((x++)); echo "survived x=$x"     # → survived x=1, exit 0
# 但同一表达式作为函数的**最后一条**命令时, 函数返回 1 → 调用点触发 set -e
f() { ((x++)); }; set -eo pipefail; x=0; if true; then f; echo "after"; fi   # → 不打印 after, exit 1
```

也就是说 `cmd_doctor` 当前**能**跑到 "found N issue(s)"，但那是因为 `((issues++))`
后面还有别的语句。这种"靠后面的语句兜住"的写法太脆，直接换成赋值最稳。

#### 三、改法

**1) B4：新增 `total_alias_insert_failures`（复用保留 pad，**不动** `sizeof` / `kVersion`）**

```cpp
std::atomic<uint64_t> total_key_sync_failures;
std::atomic<uint64_t> total_alias_insert_failures;   // v4.1 新增
char pad[4096 - 112 - 32];                           // 原 4096-112-24
```

`Stats` 增字段、`stats()` 读、`reset_all()` 清零；失败路径改为：

```cpp
void report_alias_insert_failure(IndexKind kind, uint32_t idx, const char* key) {
    const uint64_t n =
        header()->total_alias_insert_failures.fetch_add(1, std::memory_order_relaxed) + 1;
    if (n <= 8 || (n % 4096) == 0) { /* WARN */ }
}
```

**语义边界**（`doctor` 的判读依据）：**别名失败不计入 `total_alloc_failures`** ——
单确实进了 SHM（`orderSysId` 查得到），丢的只是别名。

**2) A2：`iterate_state` 返回跳过条数**

```cpp
size_t iterate_state(uint32_t want_state,
                     const std::function<void(const pubsub::RCommand&)>& cb,
                     size_t* skipped_out = nullptr) const;
// iterate_live / iterate_finished 同样加默认参数 → 老调用点零改动
```

**3) B9：`probe_max` 提示另起一行**，恢复"数值行以数字结尾"的约定。

**4) B10：`alloc_slot_scan` 开头加 `if (cap == 0) return kInvalidSlot;`。**

**5) 测试补齐：新增 `tb/tools/oms_test.cpp` + `oms_shm.sh test`**

这是本轮的主要交付物。此前所有 harness（`verify` / `verify_v4` / `sync_invariant` /
`alloc_scan` / `race` / `probe_len` / `headroom` / `b5_*`）都散在 `/tmp`，**跑完就没了**。
现在收敛成一个仓库内的 18 节断言集（**182 条断言**）：

| 节 | 覆盖 |
|---|---|
| 1 | 布局常量 / `next_pow2` / **非 pow2 cap 下 `&(cap-1)` 覆盖不全**（P1-1 的根据） |
| 2 | 三层索引往返 + 复合 client key 格式 + 负数/0/int64-min cid + 缓冲区过小 |
| 3 | `orderId` 生命周期：`""` → 首次设定 → 再变 → 回报里为空**不抹掉**已有 key |
| 4 | `LIVE → FINISHED → reclaim`，含索引 tombstone 与"回收后仍 8 单" |
| 5 | **绝不覆盖 LIVE**：环满失败 + `total_alloc_exhausted` + 8 张活单全可查 |
| 6 | **③ 慢路径救回**：窗口外 slot 200 被全表扫找到，`slowpath==1`、`failures==0` |
| 7 | **② key 同步失败**：阻塞索引后改 key → 计数 +1、副本不动、**旧 key 仍可查** |
| 8 | **B4 别名失败**：主索引 OK + 别名失败 → 计数 +1、`alloc_failures==0` |
| 9 | B5 副本==索引：`strategyId` 变更后旧复合 key 不再命中、live 条数不变 |
| 10 | 卡单强制回收 + 恢复阈值后 `live_stale==0` |
| 11 | 稳态 tombstone：40 轮饱和 **0 丢单**、`live+tomb+empty==index_capacity`、`live<=slot_cap` |
| 12 | B6 探测余量：cap=1024/4096 时 `max_live_run < 32`（实测 6 / 8） |
| 13 | **A2** `iterate_live/finished` 计数 + 向后兼容的旧调用方式 |
| 14 | 版本/容量拒绝：version 不符、非 pow2 索引、`< 4*slot_cap`、`cap=0`、`cap=2^29`、只读空文件 |
| 15 | 崩溃恢复：`RECLAIMING` 残留 → `EMPTY`、副本清空、seq 偶数、索引 tombstone |
| 16 | `reset_all` **逐个**清零全部 9 个计数（含 v4 的 C1 补漏） |
| 17 | 并发 seqlock：1 writer + 4 reader，**0 次撕裂读** |
| 18 | **其余公开接口**：`remove`（不存在→false、LIVE→三个索引全清、腾出的 slot 立刻复用且 0 丢单、重复删→false）；`recover_orphan_slots()` 显式调用的**返回值**与**幂等**、且不误伤非 RECLAIMING 的 slot；`is_open` / `created_new` / `close`（幂等 + close/open 往返后数据仍在，Writer 与 Reader 各一遍） |

`oms_shm.sh test` 在跑完断言集之后，再做一件 C++ 里测不到的事：**校验
`oms_query --stats` 的输出格式仍满足 `doctor` 的解析契约**（8 个 key 逐行断言
"行尾是数字" + 用 doctor 完全一样的 grep 管道取值）。B9 就是被这条断言抓出来的。

#### 四、验证

| 项 | 结果 |
|---|---|
| `oms_test`（18 节 / 182 断言） | **PASS 182, FAIL 0**（`./oms_shm.sh test` 退出码 0） |
| 解析契约（8 个 key） | 全部 ✓（`probe_max → 32`） |
| `doctor` 七个 fixture | `clean`→✓ healthy(0)；`slowpath`→⚠×1；`exhausted`→✗×2；`keysync`→✗×1；**`alias`→✗×1（新分支）**；`stale`→✗×1；`allocfail`→✗×1 |
| `verify_v4.cpp`（重编译后） | 12 passed, 0 failed |
| `sync_invariant.cpp` | 不变量破坏 **0 / 320** |
| `alloc_scan.cpp` | 可避免丢单 **0**；`slowpath=11` == 模拟值 11 |
| `oms_bench` 吞吐 A/B（vs v4 之前的 pristine 头文件） | INSERT 1.94M→2.06M、UPDATE 4.19M→4.44M、LOOKUP 6.98M→7.03M ops/s；**FINAL STATS 逐字节相同**（`inserts=16384 updates=32766 reclaims=0 alloc_fail=283616`）→ 热路径无回退 |
| 四个 TU `-Wall -Wextra` | **零告警** |
| `nm -u` 符号表 | `oms_query` 0 个 `pthread_create`；其余三个各 1 个 → 证明 `-pthread` 的必要范围 |

**写测试过程中被自己的测试抓到两次**（都是测试自身的缺陷，记下来避免下次再犯）：

1. **第 7 节**：`plant_blockers` 会覆写目标 key 探测窗口内的 32 个 bucket。
   第一版把"旧 key"的条目正好放在窗口里 → 被顺手抹掉 → 断言"旧 key 仍可查"**误报**。
   修法：加 `pick_key_outside()` 选一个条目落在窗口外的 key。
2. **第 17 节**：writer 只 insert 不 finalize，8192 个 slot 填满 LIVE 后
   后续 191808 次 `upsert` **合法地**失败 → 断言 `alloc_failures==0` **误报**。
   修法：writer 改成 `NEW` + 立刻 `FILLED`，环才能持续回收。

#### 五、上线动作

**无额外运维动作。** v4.1 没有改 shm 布局（`sizeof(OmsShmHeader)` 仍是 4096、
`kVersion` 仍是 3），新计数落在保留 pad 里，旧文件读出来是 0 —— 语义正好是"从未发生"。
B6 那次（`kVersion` 2→3）要删 shm 文件的原因依然成立，两件事合并成一次即可。

#### 六、撤销

```bash
patch -R -p1 < tb/tools/OmsShm_v4_1.patch
```

补丁：`tb/tools/OmsShm_v4_1.patch`（**510 行 / 26396 B**，4 文件 —— `OmsShm.h` /
`README.md` / `oms_query.cpp` / `oms_shm.sh`），正反向应用均逐字节可还原。
`oms_test.cpp` 是新增文件，不在补丁里，需单独放置。

> 补丁在 19:25 之后**重建过一次**：因为追加第 18 节测试时又改了 `README.md` 的覆盖说明
> 和 `oms_shm.sh` 的 `-pthread` 注释，原来的补丁对这两个文件已过期。重建方法（值得记住）：
> ① 把旧补丁按文件切成 4 段；② 先把当前文件反向替换回 **v4.1** 状态，再用旧补丁反推到
> **v4** 基线；③ `diff -u` 出新的 4 段；④ 正向 + 反向各 apply 一次，`cmp` 逐字节比对。
> **踩到的坑**：`difflib.unified_diff` 不会输出 `\ No newline at end of file`，而 `oms_shm.sh`
> 的 v4 基线末尾**没有换行**，于是 `-esac` 和 `+esac` 被粘成 `-esac+esac` → `patch` 报
> `malformed patch`。改用系统 `diff -u` 即可。

---

## 2.9 第 23 轮：`bench` 刷 ERROR 的真因 —— 不是 store 的 bug，是 bench 的建模错误

**现象**（Ubuntu 20.04 实测）：`./oms_shm.sh bench` 跑到 MIXED 相位后，`[OmsShm][ERROR]`
以「累计 1 次 … 8 次 …」刷屏，状态恒为 `slot_cap=131072 live=87382 finished=43690 empty=0`，
`最年轻的 FINISHED 距现在 2252 ms (< min_reclaim_age_ms=60000)`。

### 一、先证明 store 的行为是对的

`live=87382` / `finished=43690` 这两个数**恰好**是 `i % 3` 的分布：`i ∈ [0, 131072)` 里
`i%3==0`（NEW→LIVE）43691 个、`i%3==1`（PARTFILLED→LIVE）43691 个、`i%3==2`（FILLED→FINISHED）
43690 个 → LIVE 87382、FINISHED 43690。这个巧合说明：**环是被 MIXED 相位自己的单填满的**。

用探针复刻整个相位顺序（`/tmp/omsshm_r23/probe.cpp`，cap=4096 / iters=20000）拿到决定性证据：

| 相位 | empty | live | finished | inserts | updates | reclaims |
|---|---|---|---|---|---|---|
| after INSERT | 1 | 4095 | 0 | 4095 | 0 | 0 |
| after UPDATE | 1 | 0 | 4095 | 4095 | 4095 | 0 |
| after MIXED | 0 | 2731 | 1365 | **4096** | **8190** | **0** |

`upd` 从 4095 涨到 8190（+4095），而 `ins` 只涨了 **1**。这就锁定了两个 bench 缺陷：

**(M1) MIXED 相位的 key 空间和前面的相位重叠了。** 它用 `make_rcmd(r, i, st)` 且 `i` 从 0 开始，
orderSysId = `"bench-<i>"` —— 而 INSERT/UPDATE 相位造的就是 `"bench-<0..capacity-2>"`。
所以 `i < capacity-1` 的那些调用全是**更新已有单**，只有 `i == capacity-1` 那一次才真的新建
（用掉唯一一个 EMPTY slot）。代码注释写的「一半 insert 一半 update (触发状态迁移 + 部分 reclaim)」
**从来没有成立过**：实际是 ~100% update + 1 次 insert + `reclaims = 0`。

**(M2) MIXED 相位新建单的数量没有上限。** `oms_shm.sh bench` 传 `--iters=1000000`，
而环只有 131072 个 slot。`i >= 131071` 之后的 `"bench-<i>"` 才是新 key，于是
**868928 次 upsert 注定失败**（87%）。v4 加的诊断 + 限流（前 8 次、之后每 4096 次）
把它们打成 ~220 段 ERROR ≈ 1300 行 —— 于是**每一次 `./oms_shm.sh bench` 都以 ERROR 刷屏收场**，
把人训练成无视 ERROR。这才是真正要修的东西。

**(M3) MIXED 相位只打 reader 的吞吐，从来不打 writer 的。** 「1 writer + 4 readers」这个小节
里，writer 的吞吐/延迟一个字都没输出 —— 而它才是这个相位真正想测的对象。

> 编号说明：本节用 **M1/M2/M3**，避免与 §0.1 的 D 系列（D1/D2/D3/D4/D5）撞号 ——
> 那里已有另一个 `D4`（`--help` 的 2 的幂措辞）和另一个 `D5`（`oms_query.cpp:216-220` 除零）。

### 二、根因：TTL 把「可持续写入速率」钉死了

环满**不是**因为 `slot_capacity` 不够，而是因为 `min_reclaim_age_ns = 60s`：

> 稳态占用 ≈ 写入速率 × min_reclaim_age ⇒ **可持续写入速率上限 = slot_cap / min_reclaim_age**

131072 / 60s ≈ **2185 单/秒**，而 bench 压到 ~70 万单/秒 → 0.2 秒写满。
`live=87382` 那部分是**永远回收不了**的（MIXED 把 1/3 的单推成 FILLED，另 2/3 停在
NEW/PARTFILLED —— 非终态，只能等 `max_live_stale_ns = 24h`），FINISHED 那部分则是「还太年轻」。
所以快慢两遍全表扫描确实找不到可回收 slot —— 诊断说的没错，只是**没把那个速率上限算出来**。

### 三、修法

**A. `oms_bench.cpp` / `oms_demo.cpp`（两者仍逐字节相同，同步修改）**

| 改动 | 说明 |
|---|---|
| `make_rcmd(..., prefix, oid_base)` | key 空间参数化：`orderSysId="<prefix>-<seq>"`、`strategyId="<prefix>"`（复合 client key 也不撞）、`orderId="<oid_base>+<seq>"` |
| MIXED 相位改用 `"mx-"` 前缀 + `oid_base=5000000` | 与 INSERT/UPDATE 相位**完全不相交**（M1） |
| 每个单走两步：偶数次 `NEW`、奇数次 `FILLED` | 真正的一半 insert / 一半 update，且每张单都会变成 FINISHED、TTL 过后可回收 |
| 新增 `--mixed-ttl-ms`（**默认 0**） | 0 = 立刻可回收 → 回收始终可用 → **任何机器/容量都不会写满**；想复现「环写满」显式给大值 |
| MIXED 打 writer 的 summary | 修 M3 |
| MIXED 末尾自检 `alloc_fail` / `reclaims` / 占用 | 直接打 `✓ 可持续: 0 丢单` 或 `✗ 不可持续 … 上限 ≈ N 单/秒` |
| `--reset` 时若文件容量 ≠ 请求容量则 unlink 重建 | 打开已存在文件时容量一律从 header 读，`--capacity` 会被**静默忽略** —— 一个遗留的 16k 文件会让 `--capacity=131072` 白传 |
| `SHM ready` 打**文件实际**容量 + `(requested=N)` | 上面那个静默忽略要看得见 |

**B. `OmsShm.h`：把那个速率上限算出来打出来**（`alloc_exhausted_report` 新增两行）

```
  → 可持续写入速率上限 ≈ slot_cap / min_reclaim_age = 2185 单/秒
     (稳态占用 ≈ 写入速率 × min_reclaim_age; 超过这个速率必然写满)
```

并把结尾的「加大 capacity, 或让上层及时 finalize 订单」补成
「加大 capacity, **调小 min_reclaim_age**, 或让上层及时 finalize 订单」——
原来那句漏掉了这次事故真正的旋钮。

**C. `oms_query --stats` 新增 `sustainable_insert_rate`**（数值行，以数字结尾），
`doctor` 解析它并打一行 `[i] 可持续写入速率上限 ≈ N 单/秒`，`oms_shm.sh test` 的解析契约
key 列表从 8 个扩到 **9 个**。

**D. `oms_test.cpp` 新增第 19 节**钉住机理：8 个 slot 全 FINISHED（年龄 ~0）时新单**必然**写不进
（`total_alloc_exhausted == 1`）；把 TTL 调成 0 后**同一张单立刻写得进去**且 `reclaims >= 1`。
断言数 182 → **191**。

### 四、验证

| 项 | 结果 |
|---|---|
| `./oms_shm.sh build` | 4 × ✓，`-Wall -Wextra` **零告警** |
| `./oms_shm.sh test` | **PASS 191, FAIL 0**；解析契约 **9/9**（`sustainable_insert_rate → 1`） |
| `oms_bench` 用户原参数（cap=131072 / iters=1000000 / 4 readers） | **`alloc_fail=0`**、`reclaims=499999`、`inserts == updates == 631071`、`✓ 可持续: 0 丢单`；**零 ERROR** |
| `oms_bench` 负向对照（`--mixed-ttl-ms=60000`, cap=4096） | 如期刷 ERROR，且新增行打出 `可持续写入速率上限 ≈ 68 单/秒`（= 4096/60） |
| `oms_bench --reset` 容量不匹配 | 打 `capacity mismatch: file=16384 requested=131072 → unlink & recreate` 后按 131072 重建 |
| `verify_v4.cpp` | 12 / 12 |
| B6 `verify.cpp` | 23 / 23 |
| `sync_invariant.cpp`（`kMaxProbeIndex=1` 头文件副本） | 不变量破坏 **0 / 320**；旧 key 残留 0 |
| `alloc_scan.cpp` | 可避免丢单 **0**；`slowpath=10` == 模拟值 10 |
| `headroom.cpp` cap=1024 / 4096 | `alloc_failures=0` |
| `doctor` 四个 fixture | `v4_exh`→✗×2、`v4_slow`→⚠×1、`v4_small`→✗×2、`parser_fixture`→✓ healthy |

### 五、上线动作

**无额外运维动作** —— 本轮没有改 shm 布局（`sizeof(OmsShmHeader)` 仍 4096、`kVersion` 仍 3），
也没有新增计数。B6 那次 `kVersion` 2→3 的「删 shm 重建」要求依然成立。

> **运维提醒（这次事故的真正教训）**：生产环境要拿**实际下单速率**和
> `slot_cap / min_reclaim_age` 比一下。默认 100000 slot + 60s TTL 只能撑 **~1666 单/秒**；
> 131072 slot 是 ~2185 单/秒。超了就会静默丢单（v4 之前是**完全静默**，现在至少计数 + 诊断）。
> `doctor` 已经把这条线打出来了。

### 六、撤销

```bash
patch -R -p1 < tb/tools/OmsShm_v5.patch
```

补丁：`tb/tools/OmsShm_v5.patch`（**833 行 / 43703 B**，**7 文件** —— `include/oms/OmsShm.h` /
`include/oms/README.md` / `tb/tools/{oms_query.cpp, oms_shm.sh, oms_bench.cpp, oms_demo.cpp,
oms_test.cpp}`），正反向应用均逐字节可还原。

**基线分两段**（因为 `include/oms/` 不在 git 里）：

| 文件 | 基线 |
|---|---|
| `include/oms/OmsShm.h`（80486 B / 1515 行）、`include/oms/README.md`（26066 B / 974 行） | 已应用 P1-1/A1/A4/B5/B6/v4/v4.1 之后的状态 |
| `tb/tools/{oms_query.cpp, oms_shm.sh, oms_bench.cpp, oms_demo.cpp, oms_test.cpp}` | tb 仓库 **git HEAD = `825ab3f` "update"**（逐文件 `cmp` 核对过） |

> ⚠️ **补丁在 20:1x 重建过一次 —— 第一版有两个真 bug，都是"artifact 声称的基线不是真基线"这一类：**
>
> 1. **`oms_bench.cpp` / `oms_demo.cpp` 两段用的是空基线**（hunk 头写成 `@@ -0,0 +1,442 @@`，
>    即"新建文件"）。第一版的说明还写着"这两个文件此前没有被任何补丁改过"—— **这是错的**，
>    `OmsShm_B6.patch` 就改过它们。从 git HEAD 的树里 apply 会因为文件已存在（330 行）而失败。
> 2. **`oms_test.cpp` 完全没进补丁**（第一版把它列为"不含，需单独放置"）。这个理由在 v4.1 成立
>    —— 那时它还是**新增文件**；但 v4.1 已把它提交进 git，本轮它是**修改**（1023 → 1073 行），
>    所以必须进补丁，否则单靠补丁复现不出验证过的状态。
>
> **为什么第一版看起来是"验过的"**：我只做了反向 apply → 正向 apply 的**自洽**回环，两边都能过
> （空基线也能自洽），却**没检查基线本身是不是它声称的那个东西**。教训见技能规则 42。
> 重建后补了三条硬校验：① 基线 `OmsShm.h` 字节数必须是 80486；② 基线里**不能**出现本轮新增的
> 字符串（`mixed-ttl-ms` / `sustainable_insert_rate` / `可持续写入速率上限`）；③ 5 个 `tb/tools`
> 文件的基线必须与 `git HEAD` **逐字节**相等。

---

## 3. 形式正确性（当前环境能用，但是错的）

### P1-6 · seqlock 两边都缺 fence

**读侧** `read_slot_snapshot:525-540`：

```cpp
uint64_t s1 = s.seq.load(std::memory_order_acquire);
... 4 个 memcpy 读数据 ...
uint64_t s2 = s.seq.load(std::memory_order_acquire);   // ← 这里缺 acquire fence
if (s1 == s2) return true;
```

`acquire` 只约束**后续**操作，不阻止**前面的数据读**被编译器下沉到 `s2` 之后。
标准写法是在数据读之后、`s2` 之前插一道 `std::atomic_thread_fence(std::memory_order_acquire)`。

**写侧** `write_new_slot:856` / `update_slot:883`：

```cpp
s.seq.fetch_add(1, std::memory_order_release);   // odd  ← 应为 acq_rel，或后跟 acquire fence
... 数据写 ...
s.seq.fetch_add(1, std::memory_order_release);   // even
```

第一次自增用 `release` 挡不住**后面**的数据写被提到它前面。标准写法是
`fetch_add(1, acq_rel)`（或 `relaxed` + 一道 `release` fence）。

**影响**：x86-64（目标机 Ubuntu）的 TSO 天然禁止这两类重排，**实际不会出问题**；
但这是"靠硬件兜底"，不是正确实现。一旦移植到 ARM，或编译器变得激进，就会读到**撕裂的快照**。

**修法**：读写各补一道 fence（各一行），成本可忽略。

### P1-7 · writer 侧 `find_slot` 完全不做校验，reader 侧做

`lookup_impl:553-562` 会用 seqlock 快照里的 key 复核一遍（很稳），
但 writer 侧的 `find_slot:985-993` **直接信任索引**：

```cpp
uint32_t bucket = index_probe_find(arr, cap, hash, key, found);
if (!found) return kInvalidSlot;
return arr[bucket].slot_idx.load(std::memory_order_acquire);   // ← 没有复核
```

而 `insert_index:965-972` 的"命中 `hash + key_prefix` 就当作同一个 key 并**覆盖 `slot_idx`**"
这个分支，会把一个 key 的索引指向**别人的 slot**。两条合起来：

> `upsert(newRcmd)` → `find_slot(orderSysId)` 命中一个属于**别的订单**的桶 →
> `update_slot(那个 slot, newRcmd)` → **把一条无关的活单静默覆盖掉。**

触发条件是两个不同 key 的 fnv1a-64 相同**且**前 16 字节相同。概率极低（~10⁻⁹ 量级），
但这是**数据损坏**而不是"查不到"，方向错了。

`key_prefix[16]` 对**复合 client key**（`strategyId + cid`）区分度还特别差：
`strategyId` ≥ 16 字符时，同一策略**所有**订单的前 16 字节完全相同（`:253-255` / `:966-967`），
`hash` 是唯一区分者 —— 这恰好把上面那个碰撞条件从"hash + prefix 双碰"降级成"只碰 hash"。
建议 `key_prefix` 取 key 的**尾部**（cid 在尾部），或干脆去掉 prefix、改成对 slot 里的 key 做复核。

**修法**：`find_slot` 复用 `read_slot_snapshot` + key 比对，跟 `lookup_impl` 一样。

---

## 4. 工程 / 运维层

### P1-2 · `oms_demo.cpp` 是 `oms_bench.cpp` 的逐字节拷贝

```
$ diff oms_demo.cpp oms_bench.cpp     # 无输出 —— 完全相同
$ head -2 oms_demo.cpp
// oms_bench.cpp — OMS SHM 性能测试
```

**真正的功能演示/测试根本不存在**（`oms_shm.sh demo` 跑的是 bench，`cmd_build` 也把 demo 当 bench 编）。
而且 bench 本身也漏了两条关键路径：

- `make_rcmd:93` **没设 `strategyId`** → 复合 client key 退化成纯 cid 数字，
  `lookup_by_client(strategyId, cid)` 这条**文档里的主入口完全没被测到**；
- `orderId` 是 `1000000+seq`，对同一 `orderSysId` 恒定 → `update_slot` 的
  `orderId_changed` 分支永远不触发。**这条路径本身是对的**（见 §2.1），
  但它从未被覆盖 —— 也就是说"orderId 只变一次"这条不变量**没有任何测试在守**。

→ 这正是"tb/tools 下是该实现的测试"指的那个缺口。

### P2-1 · `tb/tools` 没进构建

`tb/CMakeLists.txt` 只有 `TB` 和 `InputOrder` 两个 target，
`oms_bench.cpp` / `oms_demo.cpp` / `oms_query.cpp` **都不在里面**，
`oms_shm.sh build` 是唯一会编它们的地方 → 那两个 P0 编译错误永远不会被 CMake/CI 发现。

### P2-2 · `oms_shm.sh doctor` 解析错 `total_stale_live_reclaims` ✅ 已修（§0.4）

`cmd_doctor`：

```bash
stale_reclaims=$(echo "$out" | grep total_stale_live_reclaims | awk '{print $NF}' | head -1)
```

而 `oms_query --stats` 打的那一行是：

```
  total_stale_live_reclaims  : 7  ← 非零说明有卡单被强制回收, 排查!
```

`$NF` 取到的是行尾的中文提示 `排查!`。实测：

```
$ if [ "$sr" -gt 0 ]; then ...   # sr="排查!"
(eval):[:13: integer expression expected: ...
→ 走 else 分支, 打印 "ok stale_live_reclaims = 0"      ← 真值是 7, 报 0
```

**后果**：doctor 里专门用来发现"卡单被强制回收"的那条检查**永远报健康**。
（当时以为 `alloc_failures` 那条恰好对，因为那一行末尾就是数字 —— 但 A1 之后
`alloc_failures` 也中招了，而且更严重，见 §0.4。）

**已修**：数值行不再挂行尾提示（`oms_query.cpp`），解析改为按 key 锚定行 + 取第一个数字：

```bash
grep -E '^[[:space:]]*total_stale_live_reclaims[[:space:]]*:' | grep -oE '[0-9]+' | head -1
```

### P2-3 · 其它代码问题

| # | 位置 | 问题 |
|---|---|---|
| 1 | `open():346-348` | "read-only open on empty file" 分支 `throw` 前**没关 `fd_`**。**更正：这其实不是泄漏** —— `open()` 开头会先 `close()`，且 `~OmsShmSegment():304` 也会 `close()`，所以 fd 会在下次 `open()` 或析构时被关掉，不会累积。只是与其它错误分支不一致（那 5 处都显式关了）。想统一就补一行 |
| 2 | `reset_all():711-716` | 重置了 4 个统计，**漏了 `total_stale_live_reclaims`** → `--reset` 后 doctor 会误报 |
| 3 | `stats():606` | `slot.last_update_time_ns` 非原子读 → 撕裂值可能把 `live` 误分类成 `live_stale`（best-effort，影响小） |
| 4 | `:268` 注释 | 写 "Slot 数组: capacity * 512 B"，实际 `kSlotSize = 1024` |
| 5 | `:187` 注释 | 写 "RCommand 本身 ~656B"，实际 **536 B**（`_pad1` 因此有 256 B 余量） |
| 6 | `oms_query.cpp:216-220` | `100.0 * s.x / s.capacity`，`capacity == 0` 时除零 |
| 7 | `oms_shm.sh:58-59` | `stat -f -c %T` / `stat -c` 是 GNU-only —— 目标机 Ubuntu 没问题，但 macOS 上 `check` 会报错 |
| 8 | `open()` 全局 | 没有任何机制阻止**第二个 writer** 打开同一个文件。契约只写在注释里；两个 writer 会让 `insert_index` 的非原子 RMW 破坏索引。建议 `flock` 或校验 `owner_pid` |
| 9 | `now_ns()` | 用 `steady_clock`（Linux = `CLOCK_MONOTONIC`），值**跨进程可比**（同一次开机内），但**跨重启无意义**。默认路径 `/dev/shm/...` 是 tmpfs、重启即清空，所以没问题；若有人改指到磁盘路径，重启后所有时间戳比较都会因下溢而"全部过期" |
| 10 | `compose_client_key():864` | 返回值被忽略。当前 `buf_size=64` 下不会失败（`strategyId` 截到 43 + cid 最多 20 + NUL = 64），但失败时不写终止符 → 后续 `string_view(const char*)` 会越界 `strlen` |
| 11 | `upsert` 热路径 `fprintf(stderr)` | 索引满时**每次失败打一行**。实测 5 万次迭代打出 9.6 万行 —— 生产上会淹掉日志/写满磁盘 |

### 内存占用（顺带）

**修 P1-1 之后**（`index_capacity = next_pow2(2N) = 262144`）：
`100000 × 1024 + 3 × 262144 × 32 + 4096` = **127.6 MB**（实测文件 `127569920` B）。
修之前是 121.6 MB（`121604096` B）。

这是**固定占用、放在 `/dev/shm`（= RAM）里**，与活单数量无关。
`/dev/shm` 默认上限是内存的一半，127 MB 没问题；
但如果有人把容量调到 1M slot，就是 ~1.2 GB 常驻。

---

## 5. 我做的验证（可复现）

本机 macOS（**Apple Silicon / ARM64**）没有 `rapidjson` / `fmt`，所以用**桩头 + 真实的
`OmsShm.h`** 编译：

```bash
# 桩头：/tmp/omsshm_check/stub/{rapidjson/{stringbuffer,writer}.h, fmt/format.h}
clang++ -std=c++17 -I /Users/lawson/Documents/hft/include -I /tmp/omsshm_check/stub \
        -o probe probe.cpp          # 0 error
```

| 脚本 | 验证内容 | 结果 |
|---|---|---|
| `/tmp/omsshm_impl/probe.cpp` | 结构体尺寸 | 4096 / 32 / 1024 / 536 / 232 —— 全对 |
| `/tmp/omsshm_impl/oms_bench` | 容量扫描（修前：默认 vs 2 的幂） | 2048 vs 50000 inserts |
| `/tmp/omsshm_impl/real_lifecycle.cpp` | **真实生命周期下三个索引是否归位** | ORD_SYS = CLIENT = EXCH = 128（无泄漏） |
| `/tmp/omsshm_impl/leak2.cpp` | 对照：`orderId` **两次非空变更** | EXCH 停在 223（会泄漏 —— 但该序列不真实） |
| `/tmp/omsshm_impl/race.cpp` | 读写竞争下的假阴性 | 0.0354 ~ 0.0362% |
| `/tmp/omsshm_fix/apply.py` | P1-1 补丁生成器（8 处替换，每处断言只命中一次） | 7 hunk / 134 行 |
| `/tmp/omsshm_fix/verify_real/bench_real` | **修后在真实文件上重跑容量扫描** | 100000 / 131072 / 65537 全部 50000 inserts、0 alloc_fail、0 ERROR |
| `/tmp/omsshm_fix/legacy_probe.cpp` | 老文件（`index_capacity=200000`）是否被拒 | REJECTED，报错信息带文件名 |
| `/tmp/omsshm_fix/tombstone.cpp` | **tombstone 会不会无限累积** | 40000 insert / 39744 reclaim，占用从第 20 轮起**完全持平** `256/256/0` —— 不会 |
| `/tmp/omsshm_a1/a1_regress.cpp` | A1 之后：多容量下 `live+tomb+empty == index_capacity` | 65536 / 65537 / 100000 / 1024 / 3 全 `fail=0` |
| `/tmp/omsshm_a4/a4_log_test.cpp` | A4 回滚日志两个分支（`kMaxProbeIndex=1` 强制触发） | "探测序列问题" 与 "已近满" 两个判读都命中 |
| `/tmp/omsshm_prefix/prefix_test.cpp` | **B5：前 16 字节前缀的区分度 + 真实碰撞后果** | 前缀对 `orderId`/长 sid **100% 重复**；修复前：碰撞后 **用 A 的 key 查回 B 的单**（对照：hash 不同则 2 个 slot、都查得到）；修复后：**A/B 各占一个 slot、都查得到、各返回各的** |
| `/tmp/omsshm_b5/b5_stress.cpp` | **B5 修复后：4000 个 key（314 个在碰撞桶里）的压力** | 修复前 **158 张被并掉 / 156 次查错单**；修复后 **0 / 0** |
| `/tmp/omsshm_b5/b5_lifecycle.cpp` | **B5 修复后：key 副本 == 报单体的不变量** | T1 orderId set-once 无 WARN、T2/T3 不变量成立、T4 remove 清三个索引、T5 reclaim 归位、T6 不同 key 各自成 slot、T7 空 strategyId 不改写副本 —— 全过，stderr **0 行** |
| `/tmp/omsshm_b5/micro.cpp` | **B5 的代价**：update / lookup 单线程 ns/op | lookup 35.2 → **35.0**（无变化）；update 40 → **52**（+12ns，逐项归因见 §2.4） |
| `/tmp/omsshm_probe/headroom.cpp` | **B6：`kMaxProbeIndex=32` 的余量**（只用公开 API，不改头文件） | 最长 live 连续段 19 → 35 → 38~55 → 41~49 → 38~43（随容量增长，全部 > 32）；`slot_cap=65536` 丢单 **388/203 万 = 1.9e-4**；随 churn 恶化到 **2.5e-4**。变体对照：`lim128` 丢单 0 但余量未知、`idx4x` 最长连 **13** 丢单 0 且速度不变 |
| `/tmp/omsshm_b6/verify.cpp` | **B6 应用后：23 条断言**（容量算式 / `version==3` / `probe_max` / `max_live_run` 与独立暴力实现一致 / 多容量丢单 0 / 老文件三种拒绝路径 / `slot_cap` 上界） | **23 passed, 0 failed** |
| `/tmp/omsshm_b6/headroom_after`（对改后的头文件重编） | **B6 应用后：同一份 headroom 用例 A/B** | 最长连 **38 → 13**、丢单 **388 → 0**、平均查询探测 **2.45 → 1.44**；多容量 6/10/13/10/14/16 |
| `/tmp/omsshm_b6/log_test2.cpp` | **B6 应用后：回滚日志新文案**（手工把主索引填满 live 条目，`kMaxProbeIndex=1`） | 打印 `probe headroom: max_live_run=256 probe_max=1`、`used_pct=100.00`，判读命中"聚集"支 |
| `tools_run{,_1,_14,_20}/oms_shm.sh doctor` | **B6 应用后：`doctor` 三个分支** | 健康 `✓ worst_live_run = 12 (probe_max = 32)`；`probe_max=1` → `✗ 已饱和, 正在丢单`；`probe_max=14` → `⚠ 余量不足 1.5 倍` |

> 上一版只跑了 `leak2.cpp`（人工构造的两次非空变更）就下了"会持续泄漏"的结论，
> **没有跑真实生命周期做对照** —— 这是这次误判的直接原因。两个脚本现在都保留着，
> 对照着看才说明问题。

**当前仓库状态**：`include/oms/OmsShm.h` **已被八个补丁修改**
（44788 → 47794 → 49427 → 51965 → 62899 → 68079 → 77059 → 80486 → **81474 B**；
980 → 1033 → 1062 → 1101 → 1265 → 1338 → 1471 → 1515 → **1529 行**；
md5 现为 `447b1ed8bb4bee4e1d1bfbab220c1fd2`）；
`tb/tools/oms_query.cpp` **已被 A1 + A4 + B6 + v4 + v4.1 + v5 修改**（236 → 262 → 267 → 287 → 308 → 318 → **330 行**）；
`tb/tools/oms_shm.sh` **已被 A4 + B6 + v4 + v4.1 + v5 修改**（180 → 182 → 199 → 229 → 299 → **316 行**）；
`tb/tools/oms_bench.cpp` / `oms_demo.cpp`（仍逐字节相同，md5 `e274ede4c75ac0a7a98adec32ad2f95a`）
**被 v5 改了 159 行**：`make_rcmd` 加 prefix/oid_base、MIXED 相位换 `"mx-"` key 空间、
新增 `--mixed-ttl-ms`、打 writer summary + 自检、`--reset` 容量不匹配时重建（330 → **441 行**）；
`include/oms/README.md` 改了索引 2N → 4N、内存、`shm_size`、Q2、新增 Q7 / Q8、四个新指标行、
`oms_test` 说明、`oms_demo` 描述改成如实、v5 的 `sustainable_insert_rate`（883 → **974 行**）。
新增 `tb/tools/OMS_SHM_REVIEW.md`、`tb/tools/oms_test.cpp`（**1073 行 / 191 条断言**）、
`tb/tools/OmsShm_{P1-1,A1,A4,B5,B6,v4,v4_1,v5}.patch`（**均已应用**）。
除这些文件外没有其它改动。

> ⚠️ **运维**：B6 之后 `kVersion` 已到 3，**已有的 shm 文件必须删掉重建**
> （`CONFIRM=1 ./oms_shm.sh reset`）。重建前先撤单 —— 活单状态在 shm 里。

---

## 6. 建议的修复顺序

1. ~~**P1-1 索引 2 的幂**~~ ✅ **已完成**（2026-10-05 16:54，见 §2.2）。
   剩余动作：~~把 `oms_bench --help` 和 `README` 里 "for speed / 建议 2^N" 的措辞改掉~~
   ✅ **B6 一并完成**（2026-10-06 18:05，见 §2.6 / D4）；
   **删掉已有的 shm 文件**这条仍未执行 —— 而且 B6 之后**更必须删**（`kVersion` 已到 3）。
2. ~~**A1 索引占用可观测性**~~ ✅ **已完成**（2026-10-06 15:54，见 §0.3）。
   ~~**A4 回滚日志**~~ ✅ **已完成**（2026-10-06 16:20，见 §0.4）；
   ~~**D3 `doctor` 解析**~~ ✅ **一并完成**（A1 引入的回归 + 旧 P2-2 都修掉了）。
3. **A2 `iterate_state` 的静默跳过** —— 至少返回"跳过了几条"，让对账能发现。
   **这是 A 组剩下最后一条。**
4. ~~**B5 索引身份判定 + key 副本不变量**~~ ✅ **已完成**（2026-10-06 17:05，见 §2.4）。
   实测后果确认是"用 A 的 key 查回 B 的单"，比 B1 原描述的"静默覆盖"更严重；
   顺带修掉了 `orderId` 变更时的 `IDX_EXCHANGE_ID` 条目泄漏（V3: 200 → 100）。
   本次**未做**的可选加固：扩 `key_prefix` / 换 128 位 hash（只降概率）。
5. ~~**B4 别名失败计数**~~ ✅ **已完成**（2026-10-06 19:22，见 §2.8）：新增
   `total_alias_insert_failures`（复用保留 pad，`sizeof` / `kVersion` 不变）+ 日志限流 +
   `doctor` 分支。v4 已先加 `total_key_sync_failures`（`sync_one_key` 的失败计数，见 §2.7）。
   **语义边界**：别名失败**不计入** `total_alloc_failures` —— 单确实进了 SHM
   （按 `orderSysId` 查得到），丢的只是别名。
   ~~**B1 `find_slot` 边界检查**~~ ✅ **随 B5 一起做了**（`find_slot:1212`）。
6. ~~**B6 索引容量 2N → 4N**~~ ✅ **已完成**（2026-10-06 18:05，见 §2.6）。
   实测治本：最长 live 连 **38 → 13**，丢单 **388 → 0**，平均查询探测 2.45 → 1.44。
   三件事都做了：① `index_capacity = next_pow2(4 * slot_cap)`；
   ② **`kVersion` 2 → 3**；③ 加了 `index_capacity >= 4 * slot_cap` 校验，
   并把探测余量 (`max_live_run` / `probe_max`) 放进 `Stats` + `oms_query --stats` + `doctor`。
   代价：内存 +24 MiB/10 万 slot（+20%），速度不变。
   **顺带**：`slot_cap` 上界从 2^30 收到 **2^28**（防 `next_pow2(4N)` 溢出成 1）。
   **待办**：删 shm 文件重建（`kVersion` 变了，不删起不来）。
7. ~~**B7 `alloc_slot` 128 步窗口丢单** + **B8 `sync_one_key` 丢弃返回值**~~
   ✅ **已完成**（2026-10-06 18:45，见 §2.7）。使用者提的三个"快路径 + 兜底"提案里，
   ③（alloc 全表兜底）与 ②（检查返回值）已改；①（index 全表兜底）评估后**不改**（§2.7 四）。
   **无额外运维动作**（v4 没改 shm 布局，`kVersion` 仍是 3）。
8. **B3 lookup 三态** —— 重启对账用得到，别把活单当死单。
9. ✅ **测试本体已完成**（2026-10-06 19:22，见 §2.8）：新增 `tb/tools/oms_test.cpp`
   （18 节 / **182 条断言**）+ `oms_shm.sh test`（强制重建 + 解析契约检查）。
   原计划是 **D1 写真正的 `oms_demo`** + **D2 把 `tb/tools` 加进 CMake**，
   至少覆盖：① 复合 client key 往返、② LIVE→FINISHED→reclaim 迁移、
   ③ **`orderId` 真实生命周期（空→非空→不变）+ 索引归位断言**、
   ④ **任意容量都不饱和**（含 100000 / 65537 这种非 2 的幂）、
   ⑤ **tombstone 持平断言**（防止将来有人改坏 reclaim/tombstone 配对）、
   ⑥ **数值行解析断言**（`--stats` 的每个数值行必须以数字结尾，防止再出现 §0.4 那类回归）、
   ⑦ ~~**key 副本与报单体一致性断言**~~ **B5 已实现（`b5_lifecycle.cpp` T2）**，
   搬进 CMake 时把它一起带进去。
   ⑧ **新增：探测余量断言** —— 跑一轮 churn 后断言 `3 * max_live_run < 2 * probe_max`
   （即余量 ≥ 1.5 倍）。**别用 `max_live_run < probe_max / 2`**：`slot_cap ≥ 512K` 时
   最长连续段本来就会到 16，2 倍阈值会在**健康**的大容量部署上误报。
   B6 的回归防线；`/tmp/omsshm_b6/verify.cpp` 与 `/tmp/omsshm_probe/headroom.cpp`
   可以直接改造（`verify.cpp` 已经是这个断言的现成实现）。
   **落实情况（2026-10-06 19:22）**：①~⑧ 全部落进 `tb/tools/oms_test.cpp`；
   另外补了 v4 / v4.1 的新场景 —— 慢路径救回、真·环满、`key_sync_failures`、
   别名插入失败、`reset_all` 逐计数、并发撕裂读、版本与容量拒绝、崩溃恢复、
   `iterate` 跳过计数。
   **第 18 节（19:22 补）**：对着公开接口清单又过了一遍，发现 `remove()`、
   `recover_orphan_slots()`、`is_open()`、`created_new()`、`close()` 这 5 个**一个都没测**，
   补齐后断言数 148 → **182**。
   第 ⑧ 条按这里的提醒用的是 `max_live_run < probe_max`
   （实测 cap=1024 → 6、cap=4096 → 8）。
   **仍未做**：**D1**（`oms_demo.cpp` 与 `oms_bench.cpp` 逐字节相同 —— md5
   `3e634ae85dc2310b2ab359a2d7525b00`，`./oms_shm.sh demo` 实际跑的是性能测试；
   README 已先改成如实描述）、**D2**（`tb/tools` 仍未进 CMake）。
10. **B2 seqlock 的两道 fence**（各一行，顺手做掉）。
11. **§2.1 那条注释** —— 在 `update_slot` 里写明 key 副本的 set-once 假设：
    本次已由 `sync_key_copies` 的 WARN + 索引迁移兜住（V3 实测 200 → 100），
    但仍应在注释里点明"正常生命周期下这条 WARN 不该出现，出现就是上层在改 key"。
12. C 组剩下的 **9 条**小问题（C5 死字段、C6 日志刷屏、C7 无 flock、C9 `_pad1` 省 25.6 MB 等）
    —— **C1 已随 v4 修掉**。
13. ~~**构建 `-pthread`** + **B9 `probe_max` 解析** + **B10 `cap==0`** + **C12/C13/C14 `oms_shm.sh`**~~
    ✅ **已完成**（2026-10-06 19:22，见 §2.8）。这一组是使用者报的构建错误带出来的：
    `-pthread` 只给 `oms_bench` 加过，而真正构造 `std::thread` 的 `oms_demo` 没有；
    `nm -u` 确认 `oms_query` 根本不引用 `pthread_create`。
   另修 `doctor` 的 8 处 `((issues++))`、补 `doctor` 的 auto-build、
   把 `cmd_test` 改成每次强制重建（避免测到陈旧二进制）。
14. ✅ **`bench` MIXED 相位刷 ERROR**（2026-10-06 20:0x，见 §2.9）。
   使用者报的是「bench 跑出满屏 `[OmsShm][ERROR]`」——**store 没问题，是 bench 自己的建模错误**：
   MIXED 相位的 key 空间与 INSERT/UPDATE 相位重叠（`"bench-<i>"` 且 i 从 0 开始）→
   前 capacity-1 次调用其实全是 update，只有 1 次是真 insert；且新建单数量无上限
   （`--iters=1000000` vs 131072 slot）→ 868928 次注定失败。
   根因是 **TTL 把可持续写入速率钉死在 `slot_cap / min_reclaim_age`**（131072/60s ≈ 2185 单/秒），
   而 bench 压到 ~70 万单/秒。
   修：MIXED 换独立 key 空间 + 真正一半 insert/一半 update + `--mixed-ttl-ms`（默认 0）
   + 打 writer 吞吐 + 自检 `alloc_fail/reclaims`；`--reset` 容量不匹配时重建；
   `alloc_exhausted_report` 与 `oms_query --stats` 都算出并打出**可持续速率上限**；
   `oms_test` 第 19 节钉住机理。断言 182 → **191**。
   **无额外运维动作**（未改布局、未新增计数）。
15. **仍未做**（顺延）：**D1**（`oms_demo` 与 `oms_bench` 仍逐字节相同，v5 是**同步**改的
   两个文件，md5 `e274ede4c75ac0a7a98adec32ad2f95a`；`./oms_shm.sh demo` 跑的仍是 bench）、
   **D2**（`tb/tools` 仍未进 CMake）。

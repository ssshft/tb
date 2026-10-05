# `include/oms/OmsShm.h` + `tb/tools` 复查（2026-10-05）

复查对象：

- `include/oms/OmsShm.h`（978 行，header-only OMS 订单 SHM 环形数组）
- `tb/tools/`：`oms_bench.cpp` / `oms_demo.cpp` / `oms_query.cpp` / `oms_shm.sh`

**总体结论：这个头文件从来没有被编译过。** 有两个硬编译错误；修掉之后，索引层还有一个
"只在容量是 2 的幂时才对"的隐含前提，而默认容量 `100'000` 恰好不是 2 的幂 ——
实测索引在 **2048 条**就彻底饱和，之后所有新单都写不进去。

---

## P0-1 · 编译错误：`OmsShmHeader` 的 pad 算错，`static_assert` 失败

`OmsShm.h:144`

```cpp
char pad[4096 - 96];                       // ← 96 是错的
};
static_assert(sizeof(OmsShmHeader) == 4096, "OmsShmHeader must be 4 KB");
```

`pad` 之前的成员实际占 **112 B**（不是 96）：

| 成员 | 字节 |
|---|---|
| 5 × `uint32_t`（magic/version/slot_capacity/index_capacity/index_kinds） | 20 |
| 对齐填充 | 4 |
| 2 × `uint64_t`（min_reclaim_age_ns / max_live_stale_ns） | 16 |
| `next_slot_hint` / `global_seq` / `created_ts_ns` | 24 |
| `owner_pid` / `_pad0` | 8 |
| 5 × `atomic<uint64_t>`（4 个统计 + `total_stale_live_reclaims`） | 40 |
| **小计** | **112** |

于是 `sizeof` = 112 + 4000 = 4112，`alignas(64)` 向上取整 → **4160**。编译器实测：

```
error: static assertion failed due to requirement 'sizeof(oms::shm::OmsShmHeader) == 4096'
note: expression evaluates to '4160 == 4096'
```

**修法**：`char pad[4096 - 112];`

> 这个 static_assert 本身是**对的**（它挡住了漂移），只是 pad 的常量写错了。

## P0-2 · 编译错误：`index_probe_find` 先用后声明

`OmsShm.h:460` 在 `OmsShmSegment::lookup_impl`（类体内联定义）里调用 `index_probe_find`，
但它的定义在 **`:553`**（类**之后**）。它是自由函数，不是成员，所以在 `:460` 处不可见：

```
error: use of undeclared identifier 'index_probe_find'
```

`find_slot:935` / `tombstone_index:952` 在定义之后，没事；只有 `lookup_impl` 踩到。

**修法**：在 `OmsShmSegment` 之前加一行前置声明：

```cpp
inline uint32_t index_probe_find(IndexEntry* idx_arr, uint32_t cap,
                                 uint64_t hash, std::string_view key,
                                 bool& found_out) noexcept;
```

> 顺带说明 `OmsSlot` 的 `static_assert`（`:194`）**是过的**，实测
> `sizeof(OmsSlot) == 1024`、`offsetof(order) == 232`、`sizeof(RCommand) == 536`。
> 注意注释里写"RCommand 本身 ~656B"是**错的**（实际 536），`_pad1` 因此有 256 B 余量。

---

## P1-1 · 索引探测隐含要求「容量是 2 的幂」，而**默认容量不是**（实测灾难级）

三处索引探测都用同一套写法：

```cpp
uint32_t mask = cap - 1;
uint32_t b = static_cast<uint32_t>((hash + i) & mask);   // index_probe_find:561
                                                         // insert_index:899
                                                         // tombstone_index:950
```

`& mask` **只在 `cap` 是 2 的幂时才是一个合法（满射）的探测序列**。
有意思的是 `alloc_slot:736` 反而**正确处理**了非 2 的幂：

```cpp
bool pow2 = (cap & (cap - 1)) == 0;
uint32_t idx = pow2 ? static_cast<uint32_t>(h & mask) : static_cast<uint32_t>(h % cap);
```

→ 作者想到了 **slot 环**，漏了**索引数组**。而：

- `kDefaultCapacity = 100'000`（`:59`）→ `index_capacity = 200'000`
- `oms_bench` 的默认 `--capacity=100000`（`:261`）也是非 2 的幂
- README `:215` 只写"建议 2^N"，**没有写成硬要求**

### 后果可以精确算出来

`mask = index_capacity − 1 = 199999 = 0x30D3F`，二进制里**只有 11 个 bit 是 1**，
所以 `x & mask` 最多只能产生 **2¹¹ = 2048** 个不同 bucket：

| `index_capacity` | `mask` | 置位 bit 数 | 最多可用 bucket |
|---|---|---|---|
| 200000（默认） | 0x30D3F | 11 | **2048** |
| 2000 | 0x7CF | 9 | **512** |
| 262144（2¹⁸） | 0x3FFFF | 18 | 262144 ✓ |

`kMaxProbeIndex = 32` 的探测上限也救不了：2048 个 bucket 一旦用满，
任何 32 步内的线性探测都找不到空位 → `insert_index` 返回 false → 主索引失败 → `upsert` 回滚。

### 实测（`--iters=50000`，同一台机器）

| 容量 | 是 2 的幂 | `total_inserts` | `total_alloc_failures` | `insert_index full` / `alias full` 报错 |
|---|---|---|---|---|
| 100000（**默认**） | ✗ | **2048** | **95905** | **96096** |
| 131072（2¹⁷） | ✓ | 50000 | 0 | 0 |
| 1000 | ✗ | 卡在 422 | — | 满屏 |

`total_inserts = 2048` 与上面算出的"最多 2048 个 bucket"**完全吻合**。

### 修法（二选一）

1. **推荐**：`open()` 里校验/纠正容量 —— 建新文件时把 `slot_cap` 向上取整到 2 的幂，
   或至少 `if (idx_cap & (idx_cap - 1)) throw`，并在 README 把"建议 2^N"改成"**必须** 2^N"。
2. 或把三处 `& mask` 改成 `% cap`（正确但慢），或统一成 `alloc_slot` 那种 `pow2 ? ... : ...` 写法。

> ⚠️ 这是**唯一会导致实盘丢单**的问题：索引饱和后 `upsert` 静默返回 `kInvalidSlot`，
> OMS 记录不下新订单，而策略侧看到的是"查不到"。

---

## P1-2 · `oms_demo.cpp` 是 `oms_bench.cpp` 的逐字节拷贝

```
$ diff oms_demo.cpp oms_bench.cpp     # 无输出 —— 完全相同
$ head -2 oms_demo.cpp
// oms_bench.cpp — OMS SHM 性能测试
```

所以：

- **真正的功能演示/测试根本不存在**。`oms_shm.sh demo`（`cmd_demo`）跑的是 bench；
  `cmd_build` 也把 demo 当 bench 编。
- 这就是"tb/tools 下是都该实现的测试"指的那个缺口。

另外，现有的 bench **也没覆盖两个关键路径**：

- `make_rcmd:93` **没有设置 `strategyId`** → 复合 client key 退化成纯 cid 数字，
  `lookup_by_client(strategyId, cid)` 这条**文档里的主入口完全没被测到**。
- `orderId` 是 `1000000+seq`，对同一 `orderSysId` 恒定 → `update_slot` 里
  `orderId_changed` 分支永远不触发（而那个分支有索引泄漏问题，见 P2-3）。

---

## P2-1 · `tb/tools` 没进构建

`tb/CMakeLists.txt` 只有两个 target：`TB`、`InputOrder`。
`oms_bench.cpp` / `oms_demo.cpp` / `oms_query.cpp` **都不在里面**，
`oms_shm.sh build` 是唯一会编它们的地方。

→ 所以上面两个 P0 编译错误永远不会被 CMake/CI 发现。建议把三个工具加成
`add_executable(...)`（或至少 `add_custom_target` + 一条 build-only 的 CI 检查）。

## P2-2 · `oms_shm.sh doctor` 解析错 `total_stale_live_reclaims`

`cmd_doctor`：

```bash
stale_reclaims=$(echo "$out" | grep total_stale_live_reclaims | awk '{print $NF}' | head -1)
```

而 `oms_query --stats` 打的那一行是：

```
  total_stale_live_reclaims  : 7  ← 非零说明有卡单被强制回收, 排查!
```

`$NF` 取到的是行尾的中文提示 `排查!`，不是数字。实测：

```
$ if [ "$sr" -gt 0 ]; then ...   # sr="排查!"
(eval):[:13: integer expression expected: ...
→ 走 else 分支, 打印 "ok stale_live_reclaims = 0"      ← 真值是 7, 报 0
```

**后果**：doctor 里专门用来发现"卡单被强制回收"的那条检查**永远报健康**，
而 `alloc_failures` 那条是好的（那一行末尾就是数字，所以 `$NF` 恰好对）。

**修法**：`awk -F: '{print $2}' | awk '{print $1}'`，或 `grep -oE ':\s*[0-9]+' | tr -dc 0-9`。

## P2-3 · 其它代码问题（按严重度）

| # | 位置 | 问题 |
|---|---|---|
| 1 | `update_slot:848-850` | `orderId` 变化时 `insert_index` 插入新条目，但**不 tombstone 旧条目** → 别名索引泄漏（旧条目永久占 bucket，直到 slot 被回收）。查不到是对的（`lookup_impl:474` 会校验 slot 里的 key），但浪费索引空间，会**加速 P1-1 的索引耗尽** |
| 2 | `insert_index:910-916` | 命中 `hash + key_prefix` 就当作"同一个 key"并**覆盖 `slot_idx`**。两个不同 key 若 hash 与前 16 字节都相同 → 后插入的把前者覆盖掉，前者从此查不到（`lookup_impl` 校验会返回 false）。概率低，但属正确性漏洞 |
| 3 | `key_prefix[16]` 的区分度 | 对复合 client key（`strategyId + cid`）几乎无效：`strategyId` ≥ 16 字符时，同一策略**所有**订单的前 16 字节完全相同，只剩 hash 在区分 → 放大 #2 的概率。建议 prefix 取 key 的**尾部**（cid 在那里），或存完整的 32 位指纹 |
| 4 | `open():275-277` | "read-only open on empty file" 分支 `throw` 前**没关 `fd_`** → fd 泄漏（其它错误分支都关了） |
| 5 | `reset_all():660-663` | 重置了 5 个统计，**漏了 `total_stale_live_reclaims`** → `--reset` 后那个计数还留着上一轮的值，doctor 会误报 |
| 6 | `OmsShmLayout` 注释 `:203` | 写 "Slot 数组: capacity * 512 B"，实际 `kSlotSize = 1024` |
| 7 | `OmsSlot` 注释 `:161` | 写 "RCommand 本身 ~656B"，实际 **536 B** |
| 8 | `oms_query.cpp:216-220` | `100.0 * s.x / s.capacity`，`capacity == 0` 时除零（正常路径 `open()` 已抛，但 `header()` 为空时会走到） |
| 9 | `oms_shm.sh:58-59` | `stat -f -c %T` / `stat -c` 是 GNU-only —— 目标机 Ubuntu 没问题，但 macOS 上 `check` 会报错 |

---

## 我做的验证（可复现）

本机是 macOS，没有 `rapidjson` / `fmt`，所以我在 `/tmp/omsshm_check/` 里
用**桩头 + 打过补丁的 header 副本**验证，**没有改动仓库里任何文件**：

```bash
# 1. 直接编原始 header → 2 个 P0 错误
clang++ -std=c++17 -I include -I stub -o probe probe.cpp

# 2. 只改两处（pad 96→112、加前置声明）后 → 通过
sizeof(OmsShmHeader) = 4096      ← 修好后正确
sizeof(IndexEntry)    = 32
sizeof(OmsSlot)       = 1024
sizeof(RCommand)      = 536
offsetof(order)       = 232

# 3. 三个工具在修好的 header 上全部编译通过
# 4. 运行对照实验（见 P1-1 的表格）
```

## 建议的修复顺序

1. **修两个编译错误**（P0-1 / P0-2）—— 否则后面所有测试都跑不起来。
2. **修索引 2 的幂问题**（P1-1）—— 唯一会导致实盘丢单的那个。
   最快的止血：默认容量改 `131072`，并在 `open()` 里加校验。
3. **把 `tb/tools` 加进 CMake**（P2-1），并写**真正的 `oms_demo`**（P1-2）：
   至少覆盖 ① 复合 client key 的 upsert/lookup 往返、② LIVE→FINISHED→reclaim 的状态迁移、
   ③ `orderId` 变更、④ 非 2 的幂容量的拒绝（或纠正）。
4. **修 `doctor` 的解析**（P2-2），然后处理 P2-3 的 6 个小问题。

# 项目开发历史

按日期与任务号追加。交付时登记实际实现者（含开发助手）与复核者、代码及版本、复跑命令和数据范围、结果位置与限制。多轮实验记参数变化与选用理由。[验收清单](plan.md)

## 2026-11-20 · P00-T01 · 课件与分组核对

- 开发助手依据腾讯文档第 6 组截图核对六名成员及五台机器的分配。主机配置和连通性待 P02 实测。[成员与分配](README.md#系统设计与开发顺序)
- 核对 `大数据分析技术项目-2026版.pptx` 第 4–6 页的选题、集群、汇报和提交要求。教师另明确按个人分工及完成情况评分。[课程交付](README.md#课程交付)

## 2026-11-20 · P00-T02 · 运行表、论文和日志核数

开发助手临时解包两份 RAR，以 PyArrow 逐片核对主键、工作流路径、日期、事件和结论，检查 TAR 中 ZIP 的成员数，再按 run ID 回查仓库键。P02 将独立复现下表基线。

| 检查 | 结果与口径 |
|---|---|
| 运行表 | 165 个 Parquet，19 列；4,112,526 行，run ID 去重亦为 4,112,526 |
| 工作流 | 2,969 个仓库；以仓库和精确路径计 11,396 个工作流；其中 1,186 个工作流在观测窗口只有一次运行 |
| 时间与结论 | UTC 创建时间从 2025-08-14 10:53:05 到 2026-09-21 20:38:02；`skipped` 有 2,410,478 行（58.6%），`success` 有 797,274 行，`failure` 有 526,740 行，另有其他结论 |
| 日志结构 | 92 个按仓库 ID 命名的 TAR；内部为 `<run_id>/attempt-<n>.zip`。30,427 个 ZIP 均非空，`run_id + attempt` 全部唯一 |
| 跨表关联 | 30,393 个不同 run，对应 475 个工作流；全部在运行表中找到，归档仓库与运行表一致 |
| 尝试编号 | 编号 1 有 30,290 个 ZIP，编号 2 有 128 个，编号 3 有 7 个，编号 4 有 2 个。每次尝试单独计数，保留的归档允许缺少较早尝试 |
| 文本抽查 | 在 run `28217390395` 的 attempt 1 中读到 `3_agent.txt`，共 3,603 行，含时间戳和步骤分组标记。可用归档、成员、行号回查信号；此项仅为格式抽查 |

论文全库的 562,877 个非空归档属于 561,367 个 run，受采集时点与日志留存限制，课程子集的分母见上表。教师脚本默认读 17 列，可加 `head_sha` 和 `github_metadata_json`。本地示例导入后覆盖其 Windows 路径。[课程材料与命令](README.md)

## 2026-11-20 · P00-T03 · 文档与六人开发分工

建立六份文档与个人交付约定，确定先接口、再并行产表及建模、最后集成。Git 授权写入 [AGENTS.md](AGENTS.md)，第三方清单先登记候选直接依赖，沿用指定示例的版本、用途、许可及依赖分类。

开发助手在 CPython 3.12.14、PyArrow 25.0.1、pandas 2.2.3 下用教师 `iter_shards` 读取前两片，共 50,000 行。P08 负责全量集群复现。

## 2026-11-20 · P05 / P07 · 异常检测分工与验收口径

确定刘陈昱负责运行级 Spark 检测与同批日志评分，龙启波负责工作流生命周期区间检测与时间回测。验收分别按 run 核对分数、按工作流区间核对告警、按共同样本比较排名并回查证据。开发待 P05 和 P07。

## 2026-11-21 · P00-T04 · 协作规则与课程资料索引

建立 A00 永久编号与影响项关联，固定每个 P 阶段和 A00 小项的分支。按原名和页码核对 HDFS、MapReduce、Spark、K-means、聚类和 TF-IDF。P02 增加 Stage/Shuffle 观测指标，TF-IDF 用作日志备选实验。[课程索引](technology_stack.md#课程资料与查阅路径)

初始化独立仓库，`main` 首次提交 `3b961e2` 包含六份文档和 `.gitignore`。课件与教师原件按 README 获取。

<a name="p01-acceptance"></a>

## 2026-11-21 · P01 · 数据接口、共用样本与批次入口验收

阶段主责刘陈昱，开发助手实现并执行本地验收。使用 CPython 3.12.14、PyArrow 25.0.1、165 个运行分片和 92 个仓库 TAR。按 [README](README.md#开发者第一次运行) 解包和复跑，样本保存在项目的 `.local/p01/`，入口为 [ghawtrace.pipeline](ghawtrace/pipeline.py)，自动化验收为 [test_p01.py](tests/test_p01.py)。

| 项目 | 核验步骤与实测结果 |
|---|---|
| P01-T01 · 连接约定 | 按 [数据接口](technology_stack.md#表的粒度与分析口径) 复核真实样本。747 个 run 归到 17 个工作流、202 个工作流周，按对应键连接均保留 747 行。直接左连 attempt 会扩到 750 行，按 run 聚合后左连保留 747 个唯一 run。合成用例另验证同一 run 两个 attempt、三个证据行与没有归档的 run。 |
| P01-T02 · 可复现样本 | 核验单次工作流、跳过、失败、有归档、无归档、多 attempt 六类锚点。跳过与无归档共用一条 run。样本共 747 个 run 和 294 个唯一 attempt。将其中 12 个完整工作流与全量表逐一对账。换输出目录重新生成，ID 清单和两张 Parquet 文件逐字节一致。 |
| P01-T03 · 输入准备与恢复 | 全量模式读写 4,112,526 个 run，索引 30,427 个 attempt，manifest 记录相对路径、内容 SHA-256、模式、参数和分步行数。同目录重跑样本和全量均返回 `reused: true`。测试在两张表写入后、完成清单写入前强制退出，只有隐藏临时目录。重试成功。两个进程同时写入时只发布一个批次。 |

`python -m unittest discover -s tests -v` 共 5 项通过。全量实测范围为本地输入准备，Spark 多节点作业和数据质量审计由 P02 验收。

## 2026-11-21 · P01 · 文档复核

逐一核对六份文档与代码、依赖、测试及版本管理范围。计划的 34 条验收保持原文，课程本地原件用路径标识。重新生成样本和全量批次，校验表结构、行数与复用结果。新旧样本清单和两张表逐字节一致。教师脚本双分片复读 50,000 行，5 项测试通过。

## 2026-11-22｜P02-T01 五节点环境与共享存储摸底

- **主责与分支**：万明浩，工作分支 `codex/p02`。
- **执行方法**：对 `10.176.62.226` ~ `10.176.62.230`（`node1` ~ `node5`）逐台执行系统、CPU/内存/磁盘、Java/Python/Spark/Hadoop、挂载与进程巡检，并在 `node5` 核查跨节点 SSH 连通性、`hdfs dfsadmin -report` 及 Hadoop/Spark 配置目录。
- **关键结果与发现**：
  - 五台机器均为 CentOS 7 x86_64、JDK `1.8.0_471`、默认 Python `2.7.5`（无 `python3`/`conda`），`node3` 资源最充裕（8 核 / 15G，可用 ~14G），`node5` 可用内存约 5.7G 且有一套单机 `localhost` HDFS
  - 各节点 `/opt/spark` 默认软链接不一致（`node1`~`node4` 为 `2.0.0`，`node5` 为 `2.4.8`，且各机均备有 `3.5.7`）；历史上存在多组守护进程残留；五台节点默认均无网络共享挂载，`node3`/`node4` 的 `/home` 剩余约 6.1G。

## 2026-11-22｜P02-T01 存储路径验证与部署定版

- **主责与分支**：万明浩，工作分支 `codex/p02`。
- **执行方法与证据**：
  1. 在 `node5` 核查 `nfs-server`、`rpcbind`、`exportfs`、`showmount` 与 `mount.nfs`，确认五台节点均未安装 NFS 服务及客户端工具。
  2. 在 `node5` 执行 HDFS 读写删全链路实测（`/opt/hadoop/bin/hdfs dfs -mkdir -p /tmp/p02_test` → 写入 `hello_p02_1795359151` → `-cat` 读回校验一致 → `-rm -r -f /tmp/p02_test` 清理），验证 `hdfs://` 路径可正常读写。

## 2026-11-22 · P03 · 工作流画像、连续周面板与按历史截点特征

阶段主责黄志成，开发助手实现并执行本地验收，待主责与组内复核。输入为 P01 全量批次（165 个分片，4,112,526 个 run）和 747 个 run 的样本批次，另用原始分片目录复跑对照。

复跑入口：`python -m ghawtrace.feature_pipeline --runs-input <P01 批次> --output-root .local/p03`。产物在 `.local/p03/`（全量 `p03-885865…`、样本 `p03-607d48…`）和 `.local/p03-py25/p03-87c047…`（原始分片对照），按 `.gitignore` 不入库。

| 项目 | 实现与实测结果 |
|---|---|
| 入口 | [ghawtrace/features.py](ghawtrace/features.py) 实现画像、事件构成、周面板、截点特征和对账；[ghawtrace/feature_pipeline.py](ghawtrace/feature_pipeline.py) 负责批次、`manifest.json`、原子发布与复用。[README](README.md#p03-工作流画像与周特征) 增加命令。 |
| 指标口径 | 细化见[技术栈](technology_stack.md#表的粒度与分析口径)。事件与小时比例以全部 run 为分母；失败率为 `failure/(success+failure)`；重试率为已知尝试次数中 `run_attempts>1` 的比例；耗时为 `updated_at-created_at`，缺失、倒序或零分母记 null；间隔为同一工作流相邻创建时间差；分位数为精确线性插值；周键为 UTC 周一，采集首末周 `partial`、首个观测周前 `before_first_observation`、其余 `complete`；截点活动特征只看 `created_at<cutoff`，结果特征再看 `updated_at<cutoff`。 |
| P03-T01 · 手算核验 | [test_p03.py](tests/test_p03.py) 用手算样例覆盖单 run、零分母、缺失更新时间、负耗时、跨年周界与事件/小时熵。4 个 run 的样例工作流：事件比例 0.75/0.25、小时占比 0.5/0.25/0.25、活跃天数 3/8、失败率 1/3、重试率 0.25、耗时中位 150.5 s、p90 510 s、间隔中位 147,600 s，均与手算一致。 |
| P03-T02 · 画像与对账 | `workflow_profile.parquet` 11,396 行，`run_count` 合计 4,112,526，等于输入行数；`workflow_event_mix.parquet` 15,332 行覆盖 18 个原始事件。1,186 个单 run 工作流，最大 335,985 个 run，中位 26；`failure_rate` 有 1,007 个 null（无 success/failure 结论），1,186 个间隔不可估计，均带标记。全表 `workflow_id` 无同键多值。 |
| P03-T03 · 周面板 | `workflow_week.parquet` 672,364 行 = 11,396 × 59 周（2025-08-11 至 2026-09-21 的 UTC 周一）。`before_first_observation` 454,827 行计数为 null，`partial` 11,398 行（采集首末周），`complete` 206,139 行，其中 103,433 行为零运行。逐工作流周计数与画像 `run_count` 全部一致。抽查：56,172 次运行的稳定工作流 51 个活跃周；2 次运行的稀疏工作流含 11 个沉寂周；65 次运行的间歇工作流 18 个活跃周、13 段间隔；三者周计数分别合计 56,172、2、65。 |
| P03-T04 · 截点特征 | `workflow_asof.parquet` 672,364 行（59 个周一截点），`--cutoffs` 支持自定义截点及单工作流范围。测试在截点后追加 run 后重建，旧截点各行逐字段不变；结果可见性用 `updated_at` 约束，截止时点前未完成或更新时间缺失的 run 不计入结果类分母。 |
| 全量性能 | P01 批次 83.6 MB 输入：读取与规范化 12.3 s、画像 3.8 s、周面板 7.6 s、截点表 20.1 s、对账 2.3 s，CLI 总计 44.8 s；四张产物 2.1/0.2/1.2/6.4 MB。样本批次 17 个工作流、747 个 run，约 3 s。 |
| 测试与环境 | CPython 3.12.7、PyArrow 16.1.0 与 25.0.1、NumPy 1.26.3、Windows 11 本地；目标仍为集群 Linux，代码无平台相关路径或 shell 调用。`python -m unittest discover -s tests -v` 共 30 项通过（P01 5 项 + P03 25 项）。用 P01 批次和 165 个原始分片分别全量运行，四张表逐表相同。 |
| 方案变更 | 计划原写 PySpark SQL。P03 依赖的 P02-T03 清洗表尚未落地，而 4.1M 行在单机列式下约 45 秒完成；为使本阶段能在本地按手算样例验收，改用 PyArrow + NumPy，输出仍为 Parquet，P04/P05 的 Spark 作业可直接读取。技术栈已同步，若后续要求集群重算只需替换读取层。 |
| 修正 | 验收中发现结果类比例在“有可见结果但无 success/failure 结论”时应记 null，最初被写成 NaN；已改为 null，并在批次对账中新增 `no_unmasked_nan` 检查。 |
| 交付与限制 | P02-T03 清洗表尚未产出，T02 的“对清洗表”对账以 P01 全量 `runs_input` 为基线，P02 完成后按行数复核。小时与深夜结论仅适用于 UTC；耗时是从创建到最后更新的代理量。 |

（本节为 2026-11-22 修复前基线；2026-11-23 审核修复后的口径与数字见下节。）

## 2026-11-23｜P02-T02 多节点 `spark-submit` 验证闭环与节点角色安排
- **节点角色调整原因**：
    1. 原 T1 草案计划由 `node5` 兼任 Master 与存储、`node3` 作 Driver、`node1/2/4` 作 Worker；实测 `node3`（Spark `2.0.0`）连接 `node5`（Spark `2.4.8`）Master 时因跨版本通信协议不兼容反复报 `java.io.EOFException`。
    2. 尝试在 `node3` 启动 Spark `2.0.0` Master 时，因 `/opt/spark/conf/spark-env.sh` 预设 `SPARK_MASTER_HOST='10.176.62.226'` 触发 `java.net.BindException: Cannot assign requested address`。
    3. 最终改由 `node1`（`10.176.62.226`）承担 Spark `2.0.0` Master 与提交端（Driver），`node5` 专注提供 HDFS 存储服务，同版本节点作为 Worker 接入。
- **P02-T02 分布式工作测试实测证据**：
    - 提交命令：`spark-submit --master spark://10.176.62.226:7077 --deploy-mode client --conf spark.hadoop.fs.defaultFS=hdfs://10.176.62.230:9000 /tmp/p02_t02_smoke.py ...`
  - **Application ID**：`app-20261122202423-0003`
  - **多节点执行器分布**：实测调度至 `10.176.62.228`（`node3`）与 `10.176.62.229`（`node4`）两个不同工作节点。
  - **读取 → 分组 → 写回结果**：读取 `hdfs://10.176.62.230:9000/tmp/p02_t02_in/sample.tsv`，按 `grp_key` 聚合得到 `alpha (row_cnt=2, val_sum=3)`、`beta (row_cnt=2, val_sum=8)`、`gamma (row_cnt=1, val_sum=4)`；写回 `hdfs://10.176.62.230:9000/tmp/p02_t02_out`，生成 `_SUCCESS` 及 2 个 `.snappy.parquet` 分片文件。
  - **存储排障记录**：初期使用简写 `hdfs:///...` 时因 `node1` 默认 `fs.defaultFS=hdfs://node1:9000` 触发 `Connection refused` 及 Worker 侧 `UnknownHostException: node1`，通过显式指定完整 `hdfs://10.176.62.230:9000` 路径及 `--conf spark.hadoop.fs.defaultFS=hdfs://10.176.62.230:9000` 解决。
  - **算力池扩容验证**：在 T2 验收通过后，进一步将同版本 `node2`（`10.176.62.227`，Spark `2.0.0`）作为 Worker 成功注册至 `spark://10.176.62.226:7077`，集群计算池扩展为 `node2`/`node3`/`node4` 共 3 个工作节点，为后续分布式清洗与覆盖率审计预留算力。

## 2026-11-23｜P02-T03 全量 `runs` 分布式清洗、对账与可复现验证闭环**
  - **兼容性排障与前置转码**：单片试跑暴露原始 Parquet 使用 `ZSTD` 压缩，Spark `2.0.0` 底层 Thrift 解析报 `Required field 'codec' was not present`。通过 `ghawtrace/convert_for_spark2.py` 严格按 P01 `RUN_INPUT_SCHEMA`（13 列，裁除体积巨大的 `github_metadata_json`）无损转为 `Parquet 1.0 + Snappy`（165 片总计 83.7 MB），单片试跑（25,000 行）随即一次通过。
  - **全量双跑实测证据**（3 Worker：`node2`/`node3`/`node4`，`--executor-cores 2 --executor-memory 1536m`，`spark.sql.shuffle.partitions=36`，JVM 时区 `UTC`）：
    - 第 1 遍：`app-20261123191035-0006` → `hdfs://10.176.62.230:9000/data/ghawtrace/p02_t03_run1`（报告：[run1_report](reports/p02/p02_t03_run1_report.json)）
    - 第 2 遍：`app-20261123192550-0007` → `hdfs://10.176.62.230:9000/data/ghawtrace/p02_t03_run2`（报告：[run2_report](reports/p02/p02_t03_run2_report.json)）
  - **清洗与对账结果**：
    - 全量读入核对：165 片共读入 `4,112,526` 行，`distinct_run_ids = 4,112,526`（与数据集论文总数一致）。
    - 保留与隔离明细：保留 `4,112,269` 行（`runs_clean`，99.994%），隔离 `257` 行（`runs_quarantine`，0.006%）。逐项校验结果：主键非法 `0`、主键重复 `0`、UTC 时间非法 `0`、负耗时 `0`、`status` 为空 `0`、`conclusion` 为空 `257`。
    - 隔离业务归因：257 条 `NULL_CONCLUSION` 全部来自数据抓取截止时尚未结束的运行（`queued`: 176、`pending`: 73、`in_progress`: 8），已完成（`completed`）记录零缺失。
    - 严格对账与重复运行一致：`4,112,269 + 257 == 4,112,526`（`reconciled_equals_input: true`）；两次全量运行规范化报告 `diff` 零差异，落盘回读指纹一致（`written_clean_rows=4,112,269`、`min_run_id=16963156550`、`max_run_id=35652296872`、`sum_duration_sec=71,829,545,485`）

## 2026-11-23｜P02-T05 全量聚合作业分区策略对比与最终配置固化**
- **实验设计**：基于 P02-T03 产出的全量干净表 `runs_clean`（`4,112,269` 行），执行相同的 `(repo_id, event, conclusion)` 全量分组聚合与 Parquet 落盘，固定资源配置（3 台 Worker，实测 8 个 Executor × 2 核 / 1536m），对比 `spark.sql.shuffle.partitions=200`（默认基线）与 `24`（调优配置），通过 Spark Web UI REST API 自动采集 Stage 与 Shuffle 指标。
  - **实测结果对比**：
    - **配置 A（`p=200`，`app-20261123195405-0008`）**：输入 `18` / 输出 `200` 分片（HDFS 200 个 Parquet 文件），耗时 `33.029 s`（Executor 累计 `107,660 ms`），`3` Stage / `219` Task，Shuffle 读写各 `3,894,637 B`（`89,631` 条），Spill `0 B`。
    - **配置 B（`p=24`，`app-20261123195451-0009`）**：输入 `18` / 输出 `24` 分片（HDFS 24 个 Parquet 文件），耗时 `23.906 s`（Executor 累计 `68,385 ms`），`3` Stage / `43` Task，Shuffle 读写各 `3,132,793 B`（`89,631` 条），Spill `0 B`。
  - **一致性与选型结论**：两组配置输出均为 `10,246` 行（`sum_run_cnt=4,112,269`、`sum_duration_sec=71,829,545,485`、`sum_nonempty_log_runs=561,366`），逐行内容 SHA-256 完全一致（`b9f0191f90f927ed87145432569e258c60678f62939069c74e5b55c9161ead3f`）。最终选定 `spark.sql.shuffle.partitions=24`：端到端提速 27.6%、CPU 时间降 36.5%、Task 数降 80.4%、Shuffle 字节降 19.6%、输出小文件降 88% 且无溢写（报告：[p200_report](reports/p02/p02_t05_p200_report.json)、[p24_report](reports/p02/p02_t05_p24_report.json)）。

## 2026-11-23 · P03 · 审核意见修复与回归

阶段主责黄志成，开发助手按组内 2026-11-23 审核意见修复并执行本地回归，待主责与组内复核。审核确认 T01 手算指标可验收；T02 已在 P01 全量上验证，正式验收仍需 P02-T03 的 `runs_clean`；T03 的样本观测覆盖和 T04 的截点可见性与范围按下列六项修复。集群全量聚合方案仍待团队确认，本轮交付定位为“P03 本地实现”。

| 审核问题 | 修复与实测结果 |
|---|---|
| 1. 样本周面板制造虚假沉寂周 | 读取 `sample_manifest.json` 的 `cohorts`（或 `--sample-manifest` 显式传入），只有完整 cohort 工作流按全量观测处理；其余锚点工作流有 run 的周记 `partial`、无观测的周记 `unknown` 且计数为 null。P01 样本中 5 个非 cohort 工作流原先有 **63** 个错误零运行 `complete` 周，修复后为 **0**；12 个 cohort 的周计数与全量逐周一致，样本状态为 `before_first_observation` 529、`complete` 264、`unknown` 193、`partial` 17。 |
| 2. 全局截点列出尚未出现的工作流 | 全局截点只为 `created_at < cutoff` 的工作流出评分行；显式 `repo_id`/`workflow_path` 限定的截点保留冷启动行（`has_prior_runs=false`）。全量 `workflow_asof` 从 672,364 行变为 **206,141** 行（59 个周一截点中首个无可见工作流，表中出现 58 个；覆盖 11,377 个工作流），2025-08-11 截点 0 行，`n_prior_runs=0` 的全局行 0；追加一个全新工作流后旧截点键集与数值不变。 |
| 3. `updated_at` 已到不等于结果已完成 | 已知结果要求 `status=completed`、`conclusion` 非空且 `updated_at < cutoff`；有效耗时与尝试次数也只取 completed 的可见 run；进行中/排队 run 只进入活动类计数与 `open_run_count`，其耗时计入缺失。全量画像 `attempts_known_count` 合计 **4,112,269**、`total_attempts` **4,207,325**、`retried_run_count` **93,891**、`elapsed_known_count` **4,112,269**、`elapsed_missing_count` **257**；最后一个截点 `n_prior_outcomes` 合计 **4,069,017**，与原始数据独立复算的 completed+结论非空+更新时间早于截点一致。新增 completed/skipped + in_progress + queued 的回归用例：活动 3 条、已知结果 1 条、跳过率 1.0、耗时只算 completed。 |
| 4. 截点范围部分为空被静默扩大 | `load_cutoffs` 与 `build_asof` 逐行校验 `repo_id`/`workflow_path` 同时为空或同时非空；只给路径或只给仓库都抛 `ValueError`，两种部分为空均有回归用例。 |
| 5. 只发现输入根目录的 `*.parquet` | `resolve_input` 递归发现嵌套 Parquet；读取层用 Hive 分区并保留所需分区列，`runs_clean/year=2026/month=01/part-00000.parquet` 布局已验证；P01 批次仍优先读取 `runs_input.parquet`。 |
| 6. 批次标识与实际读取的输入可能不一致 | 入口冻结初始文件清单并传给读取层，实际读取只使用冻结清单；发布前重新枚举目录并核对成员、输入文件快照、样本清单哈希和截点文件哈希。新增“执行中新增分片”和“执行中改写截点文件”两个回归用例，均拒绝发布且不留下 `p03-*` 目录。 |

| 项目 | 修复后实测 |
|---|---|
| 测试 | `python -m unittest discover -s tests -v` 共 **38** 项通过（P01 5 项 + P03 33 项），覆盖上述六项和原有手算样例。 |
| 全量运行 | P01 批次 4,112,526 个 run：画像 11,396 行、事件构成 15,332 行、周面板 672,364 行（状态与修复前相同：before 454,827、partial 11,398、complete 206,139，其中零运行 103,433；全量输入无 `unknown`）、截点表 206,141 行；CLI 约 56.6 秒。11 项批次对账全部通过。 |
| 样本运行 | 747 个 run、17 个工作流：画像 17 行、事件构成 25 行、周面板 1,003 行、截点表按可见工作流缩行；5 个非 cohort 工作流的未观测周为 `unknown`，不再出现虚假零运行。 |
| 兼容性 | 批次参数 `interface_version` 升为 2，已有 v1 批次不会被误复用；`requirements.txt` 仍为 PyArrow 25.0.1 + NumPy 1.26.3。 |
| 限制 | 集群全量聚合尚未实现，plan 中保留集群验收项；T02 的 `runs_clean` 对账待 P02-T03；样本数据只用于管线冒烟，不能替代全量验收。小时与深夜结论仅适用于 UTC；耗时仍是创建到最后更新的代理量。 |

（第一次修复后的样本状态 before 529 / unknown 193 是复审前数字；复审后更新为 before 432 / unknown 290，且锚点前未观测周改为 `unknown`，见下节。）

## 2026-11-23 · P03 · 复审三项边界修复

阶段主责黄志成，开发助手按第二轮复审意见修复并回归，待主责与组内复核。上轮三项（全局截点未来工作流、两种不完整截点键、嵌套 Hive 目录含主键仅存分区路径）复审确认可关闭；本轮处理剩余三项。

| 复审问题 | 修复与实测结果 |
|---|---|
| 1. 锚点之前的未观测周仍被标为出现前 | `build_week_panel` 对非完整观测工作流不再写 `before_first_observation`：有采样 run 的周记 `partial`，其余所有未观测周（含锚点之前）记 `unknown` 且计数 null。真实 P01 样本与全量逐键逐周比较，原先 11 个“样本写 before、全量实际活跃”的周（含 108/123 个 run 的周）降为 **0**；样本状态更新为 `before_first_observation` 432、`complete` 264、`unknown` 290、`partial` 17；虚假零运行周仍为 0，12 个 cohort 周计数与全量一致。新增“全量在锚点前有活动、样本只含中段锚点”的回归用例。 |
| 2. 缺少 `status` 整列时默认全部 completed | `read_runs` 与 `normalize_runs` 均要求输入含 `status` 列，缺列直接抛 `ValueError`，不再回退为 `is_completed=True`；`status` 单元格为空仍按 unknown 处理，不产生已知结果。新增回归：同一带结论、尝试数和更新时间的记录，缺整列被拒绝；status 为空时已知结果 0、`n_prior_known_attempts=0`、耗时为 null，而不再输出 300 秒耗时。 |
| 3. 运行中出现的样本清单可让同一批次 ID 对应两种产物 | 入口在开始时只发现一次样本清单，把“有清单/无清单”和清单哈希写入参数；读取层以 `auto_detect_manifest=False` 调用，不再自行发现。发布前若原先无清单而运行中出现 `sample_manifest.json`（或清单消失/改写）则拒绝发布。批次参数升为 `interface_version=3` 并始终含 `coverage`（无清单为 null，有清单为摘要），有/无清单的批次 ID 不再相同。新增“执行中创建清单”回归，拒绝发布且不留 `p03-*`。 |

| 项目 | 复审后实测 |
|---|---|
| 测试 | `python -m unittest discover -s tests -v` 共 **41** 项通过（P01 5 项 + P03 36 项），覆盖上述三项边界和原有手算样例。 |
| 全量输出 | 与 291aa19 相比，`workflow_profile`、`workflow_event_mix`、`workflow_week`、`workflow_asof` 四张表逐表相同：画像 11,396 行、周面板 672,364 行、截点表 206,141 行；11 项对账全部通过。 |
| 样本输出 | 画像 17 行、事件构成 25 行、周面板 1,003 行、可见截点表 457 行；非 cohort 工作流所有未观测周为 `unknown`，不再出现“锚点前活跃却写 before”的周。 |
| 批次 | `interface_version=3`；无清单时参数 `coverage=null`，有清单时记录路径、字节数和 SHA-256；样本批次清单覆盖已写入。 |
| 耗时 | 本轮全量 CLI 约 113.7 秒（前一轮约 56.6 秒，磁盘/负载不同；输出未变），样本约 1.4 秒。 |
| 限制 | T02 仍待 P02-T03 的 `runs_clean` 正式对账；集群全量聚合另行登记。样本只用于管线冒烟；小时与深夜结论仅适用于 UTC。 |

## 2026-11-25 p02修改以及补充测试
1. **本地回归测试（`tests/test_p02_regression.py`）**
   - 覆盖 `convert_for_spark2.py` 路径隔离/同径拒绝/`limit=1` 样本隔离与源文件 SHA-256 保持、`clean_runs.py` 跨秒亚秒精度与反转时间处理、`partition_bench.py` 遥测失败空值标记等场景；本地无 PySpark 桩环境下通过（`skipped=4` 为真实 PySpark 端到端用例，已由集群实测覆盖）。

2. **集群双份全量清洗（`t03`，Spark 2.0.0，8 Executors × 2 Cores / 1536m）**
   - 输入：`hdfs://10.176.62.230:9000/data/ghawtrace/runs_snappy`（`4,112,526` 行）。
   - `run1`（`0016`）与 `run2`（`0017`）均满足 `reconciled_equals_input: true`、`retained_rows: 4,112,526`、`total_quarantined_rows: 0`、`retained_unfinished_runs: 257`（`queued: 176, pending: 73, in_progress: 8`）、`sum_duration_sec: 71835442134`、`sum_elapsed_seconds: 71835442134.0`，两次结果完全一致。

3. **同一清洗底表分区压测（`t05`，输入 `p02_t03_run1/runs_clean`）**
   - `cfgA_p200`（`0018`，`shuffle.partitions=200`）：耗时 `26.678s`，总任务 `219`，累计 Executor 运行时间 `106,392 ms`，Shuffle 读写 `3,751,208` bytes。
   - `cfgB_p24`（`0019`，`shuffle.partitions=24`）：耗时 `21.387s`，总任务 `43`，累计 Executor 运行时间 `66,126 ms`，Shuffle 读写 `2,948,664` bytes。
   - 两档配置产出 `10,302` 行且聚合内容哈希一致（`c9dfdb710b81664a2c3176aa118eb5d4db4035c3de6763df75813f3754acf2d8`）；`p=24` 端到端耗时减少约 `19.8%`，累计 Executor 运行时间减少约 `37.8%`。
4. 在[README](README.md)中添加了转换Parquet格式、集群提交、烟雾测试的指令入口

### 2026-11-26 p02修改2
1. **修改**：clean_runs.py 时间列从 utc_str 字符串改为 _micros_to_timestamp_col 持久化 TimestampType（微秒精度），并移除未定义的 _workflow_key_invalid 引用。
**验收**：run3 批次 18 个 parquet 原样输入 P03 CLI，manifest.status=success、steps[0].output_rows=4112526、11/11 checks 全过（reconcile 步 failed=0），但当前为 timestamp[ns] 无 tz 元数据、样本亚秒为 0
**待确认**:tz 与亚秒两点待管理者确认 P03 是否接受 naive UTC 
2. **修改**：确认 quarantine_reason 只由 _pk_invalid/_pk_dup/_repo_invalid/_workflow_path_invalid/_created_utc_invalid 触发，workflow_id 与 status 不参与隔离，业务代码无需改动。
**验收**：节点 verify_nullable_fields.py 用 4 行合成数据跑 clean_runs.py 端到端，10 项断言 ALL PASS（retained_rows=3、quarantined_rows=1、INVALID_WORKFLOW_PATH=1、unfinished_by_status={<NULL>:1}）；本地 test_p02_regression.py 新增 3 条对应测试，discover 得到 OK (skipped=4)。
3. **修改**：改为不可变批次目录 batches/<token>/ → 回读核验行数 → 写 _SUCCESS → staging 核验 → rename 提升 current，并在 except 分支加“current 丢失且 backup 存在则 rename 回滚”。
**验收**：inject_failure.py 在 4 个发布关键点注入 I/O 错误，4 个故障点全部 PASS（current 均恢复为上一成功批次且完整可读）
4. **修改**：test_p02_regression.py 的 P02_DIR 指向修正为 ghawtrace/p02，pyspark 桩补全 LongType/TimestampType/StructField/StructType，并把 find_spec 改为真实 import 试探以识别空壳 pyspark。
**验收**：干净检出后 python -m unittest discover -s tests -v 输出 Ran 58 tests ... OK (skipped=4)，P01 五项 + P02 非 Spark 测试全过，TestPySparkEndToEndIfAvailable 按设计在无 pyspark 环境时 skip
5. **修改**：partition_bench.py 抽出 evaluate_t05_evidence(stages_raw, executors_raw)，可验收条件收紧为“两接口正常返回 + 至少 1 个 COMPLETE stage + 累计任务数 > 0 + 至少 1 个非 driver executor 的 completedTasks > 0”，report 新增 evidence_breakdown 字段。
**验收**：本地新增 5 条纯函数测试（空列表 / HTTP 错误 / 无任务 / 无 worker / 正向）全部 ok；重跑两份 partition_bench 报告 valid_for_t05_acceptance 仍为 true，output_content_sha256 与改动前一致（c9dfdb...f2d8），证明只收紧判定边界、不影响实测内容。

### 2026-11-26 p02修改3
- 测试改为从报告读取 `clean_path` / `quarantine_path`，不再拼接 `out_dir/runs_clean`，与生产代码的 `batches/<token>/` 布局对齐。
- 调用 `clean_mod.main()` 期间临时禁用 `spark.stop`、退出后恢复，解决 `main()` 停止 session 后测试再读 parquet 报 stopped SparkContext 的问题。
- 为整列全空的 `workflow_id` fixture 显式指定 `StructType`，避免 Spark 2.0 无法推断类型。
- `_write_success_marker` 去掉 Py2 专有的 `unicode` 检查、改用 `isinstance(payload, bytes)`，Python 3 直接调用不再抛 `NameError`。
- 新增 `ghawtrace/p02/verify_nullable_fields.py` 与 `ghawtrace/p02/inject_failure.py` 并随仓库提交，作为可从仓库复跑的目标集群测试入口。
- 记录实际执行的 nullable、时间倒序、跨秒持久化用例结果：本地纯函数用例全通过；节点 `verify_nullable_fields.py` 10/10 PASS 覆盖 nullable 与隔离表。
- 修正 history 中同一轮的旧计数，从“9 项 / 跳过 1 项”更新为“58 收集 / 54 通过 / 4 项 PySpark 端到端按设计跳过”。
- 节点 nullable 验收脚本默认只加载同目录模块
- 故障注入只接受指定故障点，无关异常判 FAIL
- 时间列微秒持久化写后读回证据

### 2026-11-27 p02review fix04
1. 旧批次两张表与第一份报告对账
**代码修改**
- `ghawtrace/p02/inject_failure.py` 的 `verify_rep1_paths` 改为**始终尝试读取** `rep1["clean_path"]` 与 `rep1["quarantine_path"]`：
  - 读表失败（目录缺失、`part-*` 不存在）直接 FAIL，不再用 `if os.path.isdir(...) else 0` 把缺失记成 0 行；
  - 实际行数与报告 `retained_rows`、`total_quarantined_rows` 逐一比对，不匹配即 FAIL；
  - `_SUCCESS` 中的 `batch_token` 与报告 `batch_token` 不一致也 FAIL。
- 新增 `run_row_mismatch_regressions()`，用独立临时目录覆盖三个场景：
  - `quarantine_missing`：删除隔离表目录 → 预期 FAIL；
  - `clean_shrunk`：清洗表缩到 1 行（写到临时目录后替换，保留 `_SUCCESS`）→ 预期 FAIL；
  - `quarantine_zero_ok`：隔离表存在且实读 0 行、报告 `total_quarantined_rows=0` → 预期 PASS。
- `_run_case` 区分 `InjectedFault`（`stage_name == fp` 才算命中）与其他异常，`other_exc` 非空即 FAIL；`all_pass` 加 `bool(results)` 防空列表误判。
**验收**
```bash
  ssh root@10.176.62.226
  cd /root/ghawtrace_check
  /opt/spark/bin/spark-submit \
    --master local[1] \
    --py-files ghawtrace/p02/clean_runs.py \
    --conf spark.pyspark.python=python \
    --conf spark.pyspark.driver.python=python \
    ghawtrace/p02/inject_failure.py \
    2>&1 | tee reports/p02/inject_failure.log
```
期望输出：
```
[setup] loading clean_runs from: /root/ghawtrace_check/ghawtrace/p02/clean_runs.py
...
FAULT INJECTION RESULTS
PASS - after_batch_clean_write - stage=after_batch_clean_write; batch_token=local-1795735334048_c2d7ecfd; clean_rows=3; quarantine_rows=1
PASS - after_batch_success - stage=after_batch_success; batch_token=local-1795735334048_9c860a36; clean_rows=3; quarantine_rows=1
PASS - after_stage_success - stage=after_stage_success; batch_token=local-1795735334048_10f4a10c; clean_rows=3; quarantine_rows=1
PASS - after_rename_current_to_backup - stage=after_rename_current_to_backup; batch_token=local-1795735334048_ced84533; clean_rows=3; quarantine_rows=1
PASS - input_corrupted - input_corrupted: InjectedFault=False, other_exc=yes, current_unchanged=True
PASS - quarantine_missing - rejected as expected: cannot read rep1 quarantine_path file:///tmp/p02_row_quarantine_missing_E6fqUr/out/batches/local-1795735334048_32114186/runs_quarantine: u'Path does not exist: file:/tmp/p02_row_quarantine_missing_E6fqUr/out/batches/local-1795735334048_32114186/runs_quarantine;'
PASS - clean_shrunk - rejected as expected: rep1 clean rows mismatch: actual=1 expected=3
PASS - quarantine_zero_ok - PASS as expected: ok
============================================================
RESULT: ALL PASS
```
2. Spark 亚秒读回结果入仓
**代码修改**
- 新增 ghawtrace/p02/verify_time_precision.py，从报告 rep["clean_path"] 读 Spark 实际写出的 Parquet，断言 P03 真正读取的 created_at_utc / updated_at_utc 两列微秒值： 
- 跨秒小数：00:00:00.900001Z → 00:00:01.100002Z，created_at_utc.us=900001、updated_at_utc.us=100002、elapsed_seconds≈0.200001；
- 同秒倒序：00:00:05.500000Z → 00:00:05.200000Z，created_at_utc.us=500000、updated_at_utc.us=200000、elapsed_seconds=None。
- 脚本默认从同目录加载 clean_runs.py（或 CLEAN_RUNS_PATH 覆盖），不依赖 /tmp。
**验收**
```bash
ssh root@10.176.62.226
cd /root/ghawtrace_check
/opt/spark/bin/spark-submit \
  --master local[1] \
  --py-files ghawtrace/p02/clean_runs.py \
  --conf spark.pyspark.python=python \
  --conf spark.pyspark.driver.python=python \
  ghawtrace/p02/verify_time_precision.py \
  2>&1 | tee reports/p02/time_precision_readback.log

期望输出：
```
[time] row1 created_at_utc.us = 900001  (expected 900001)
[time] row1 updated_at_utc.us = 100002  (expected 100002)
[time] row1 elapsed_seconds   = 0.200001  (expected ~0.200001)
[time] row2 created_at_utc.us = 500000  (expected 500000)
[time] row2 updated_at_utc.us = 200000  (expected 200000)
[time] row2 elapsed_seconds   = None  (expected None)
PASS - row1 created_at_utc.us == 900001
PASS - row1 updated_at_utc.us == 100002
PASS - row1 elapsed_seconds ~= 0.200001
PASS - row2 created_at_utc.us == 500000
PASS - row2 updated_at_utc.us == 200000
PASS - row2 elapsed_seconds is None
RESULT: ALL PASS
```
验收只需两行：900001 / 100002 + 0.200001（跨秒），500000 / 200000 + None（同秒倒序），证明 Spark 2.0 写出的 Parquet 保留微秒，P03 读取的两列可靠。
3. README中的两项指令已做修改，不再依赖临时目录，改用受控的项目检出目录
检验完这三项之后，即可合并分支



## 2026-11-27 · P02-T04 全量日志覆盖验收

P02-T01/T02/T03/T05 由万明浩交付。P02-T04 主责调整为刘陈昱，交付 `ghawtrace/p02/coverage.py`、故障样例和[完整聚合报告](reports/p02/p02_t04_coverage.json)。P03-T02 仍由黄志成主责，刘陈昱协作集群实现与清洗表对账。张锦稷承接 P06 日志解析与指纹。

审计从成功 P01 批次流式读取 run，并核对原始 TAR 的 SHA-256。读取成员头，不执行或解压日志内容。将 ZIP 键、所属仓库、成员大小与出现次数同 attempt 索引逐一比对。用 SQLite 左连接去重后的覆盖 run，按创建时间 UTC 月份和原始 event 汇总，保留零覆盖组。复跑命令见 [README](README.md#p02-日志覆盖审计)。

| 模式 | TAR | ZIP | 带日志 run / 全部 run | 覆盖仓库 / 总仓库 | 覆盖工作流 / 总工作流 | 月 × 事件组 |
|---|---:|---:|---:|---:|---:|---:|
| full | 92 | 30,427 | 30,393 / 4,112,526（0.7390%） | 92 / 2,969 | 475 / 11,396 | 188 |
| sample | 92 | 294 | 291 / 747（38.9558%） | 11 / 14 | 14 / 17 | 27 |

两种模式的重复键、孤立 ZIP、仓库不符、空成员、非法成员、索引多出与缺失均为 0。样本有意包含归档锚点，其覆盖率仅描述该样本。合成测试另验同一 run 多 attempt、无日志组、重复键、孤立和错仓时的计数及失败状态。

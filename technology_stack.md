# 技术栈与数据接口

P01 已在本地实测 CPython 3.12.14 与 PyArrow 25.0.1，后者固定在 [依赖清单](requirements.txt)。P03 以 NumPy 1.26.3 做分组统计，本地实测 CPython 3.12.7、PyArrow 16.1.0 与 25.0.1 两版结果逐表一致。P02 起的选型待实施，五节点配置、版本和资源上限由 P02 实测。各发行版及许可证见 [第三方清单](THIRD_PARTY_NOTICES.md)。

## 按流程选型

| 流程与任务 | 工具 | 输入、处理与输出 | 运行位置和验证 |
|---|---|---|---|
| 输入准备（P01） | unar、Python、PyArrow、`tarfile` | 解包 RAR。按列分批读取 Parquet，按仓库 TAR 的成员清单建立 attempt 索引 | 本地实测两种模式，按参数读取源目录。可用教师脚本的 `iter_shards`、`load_runs`（pandas）或 `query`（DuckDB）独立抽查 |
| 批处理环境（P02） | 集群 Linux、JVM、Apache Spark、PySpark、`spark-submit` | 提交驱动程序并将清洗、聚合、分区写回分发给执行器 | 先盘点五台机器。已有可用 YARN 就沿用；空环境拟用 Spark Standalone，一台协调、四台工作。以实际执行器记录认定多机运行。[集群说明](https://spark.apache.org/docs/latest/cluster-overview.html) |
| 数据存取与清洗（P02） | 共享存储、Parquet、Spark SQL / DataFrame | 分片投影、类型和 UTC 转换、主键与状态审计、分区落盘。按读取模式比较分区方案，避免按 run 建目录 | 优先用已有 HDFS 或所有节点同路径可访问的共享挂载；没有共享存储时，先在 P02 明确部署可用方案并验证跨节点读写。[Spark Parquet 文档](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) |
| 特征工程（P03） | PyArrow（`pyarrow.dataset`、`pyarrow.compute`）、NumPy 分组扫描、`ghawtrace/features.py` | 读 P01 批次、P02 清洗表或嵌套分区 Parquet 目录；输入必须含 `status` 列；按 `(repo_id, workflow_path)` 排序分组；P01 样本按 `sample_manifest.json` 的 cohort 区分完整观测与锚点；产出工作流画像、事件构成、连续周面板和按历史截点特征；全部写 Parquet | 本地实测 165 个分片、4,112,526 个 run、11,396 个工作流与 59 个 UTC 周，单进程约 1–2 分钟；手算样例与 11 项对账检查随批次记录。P04–P07 直接读取 Parquet，无需重算 |
| 聚类（P04） | `pyspark.ml` 的 VectorAssembler、StandardScaler、KMeans、ClusteringEvaluator；scikit-learn 的 adjusted_rand_score | 数值向量 → 缩放 → 拟合多个 K 和种子 → 对照轮廓系数、簇规模、ARI；用原始单位解释中心与代表样本 | Spark 运行聚类；ARI 比较同一组工作流的两套标签。记录距离度量和实际缩放方式。[聚类与评估](https://spark.apache.org/docs/latest/ml-clustering.html) |
| 运行级异常（P05-T01） | PySpark SQL 窗口与分位数聚合，Python 样本复算 | 一个 run 一行。用同一工作流在评分前已观测到的运行估计耗时中位数、MAD 与重试分布，输出分项、综合分数及原因 | 对零 MAD 约定回退或缺失规则，对跳过及历史不足给出状态。按特征声明精确值或近似分位数的复核容差 |
| 生命周期与时间回测（P05-T02、P05-T03） | Spark SQL 周面板、持续性规则、时间顺序回放 | 一个工作流可有多个沉寂、恢复或突发区间，保存开始周、结束周和历史节律 | 在已知变化点上核验告警延迟，用稳定序列估计误报，标出末端截尾区间 |
| 日志指纹（P06） | Python 标准库 `tarfile`、`zipfile`、`io`、`re`；PyArrow/Parquet | 以仓库 TAR 为分工单元，依次读单个 ZIP 和文本成员；记录规则命中、有效行数、成员定位、解析状态，先聚合 attempt 再形成 run 特征 | 批处理按 TAR 分配工作；保留证据行号，避免在 driver 解压整批数据或把全文收集至内存 |
| 联合无监督分析（P07） | scikit-learn IsolationForest、pandas、DuckDB | 在同一有日志 run 集合上比较结构化与联合模型；对排名变化案例移除对应的日志特征组后重训，关联原始证据。将 `score_samples` 转为高分表示异常的统一方向 | 约三万条 run 的特征矩阵可在协调节点上建模。记录时间切分、种子、缺失率、排名变化及 Top-K 重合。[模型接口](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html) |
| 查询、演示与协作（P08/P09） | DuckDB、Streamlit、Plotly.py、Git | 从已产出结果按工作流、run 和时间范围查询，绘制原型分布、时间线、异常排名与证据摘要；Git 记录源码与文档版本 | 演示端读取汇总表和索引，支持无日志状态。Git 权限见 [项目规则](AGENTS.md) |

## 课程资料与查阅路径

按原文件名和幻灯片页码查找课程群或组内共享课件，开发时对照实际发行版。

| 对应任务 | 课程资料 | 项目中的用法与对照入口 |
|---|---|---|
| P02 集群与存储 | `week1.2.Hadoop概述.pptx` 第 17 页，`week2.1.HDFS.pptx` 第 19–25、44–50、51–54 页，`week6.1.spark-general.pptx` 第 27–30 页 | 根据资源管理器、存储和执行器的职责划分节点。若采用 HDFS，核验输入与产物对各节点可见。按仓库保留日志 TAR，避免上传大量单条日志文件。[Spark 集群部署](https://spark.apache.org/docs/latest/cluster-overview.html) |
| P02-T05、P03、P05-T01 分区与窗口 | `week2.2.Mapreduce.pptx` 第 16、20–29 页，`week6.2.spark原理.pptx` 第 14–21、26–27 页 | 用按键重分区、宽依赖、Shuffle 和 Stage 理解聚合与窗口的成本。在同一全量作业比较两种分区设置，记录执行器、Shuffle 和 Stage 耗时。历史窗口依据实际可见时间构造。[Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html) · [Spark SQL 窗口函数](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html) |
| P04 画像聚类 | `week3.2.Kmeans.pptx` 第 3–8 页，`week4.1.机器学习.pptx` 第 50–56 页，`week4.2.聚类.pptx` 第 4–10 页 | 参照“分配到最近中心、重算质心”的迭代关系解释 Spark MLlib 结果。课件对距离和初始中心的讨论用于设计量纲、K 和随机种子的对照实验。[Spark MLlib 聚类](https://spark.apache.org/docs/latest/ml-clustering.html) |
| P06–P07 日志文本特征备选 | `week3.1.tfidf.pptx` 第 2–7 页 | 课件区分单篇词频与全语料文档频率。若规则指纹遗漏较多可重复的模式，可把归一化日志模板作为词、run 作为文档，试验只在训练集合拟合的 TF-IDF；现有规则指纹继续提供可回查证据。[Spark MLlib TF-IDF](https://spark.apache.org/docs/latest/ml-features.html#tf-idf) |

## 表的粒度与分析口径

P01 的 `runs_input.parquet` 从 19 个源字段中读取 13 列，保留时间字符串、状态和空值，供 P02 清洗。`log_archive_count`、`log_file_count` 和 `empty_archive_count` 属于源数据统计。`attempt_index.parquet` 记录课程 TAR 中的 ZIP 位置，ZIP 内容及目标步骤由后续阶段核验。[输出模式与命令](README.md#开发者第一次运行)

下表为现有产物与 P02–P07 的下游约定。主键非空，ID 使用 int64，attempt 为从 1 起的 int32，路径保留原文及大小写。下游用 Parquet 微秒 UTC 时间戳，Spark 会话设为 UTC，周键取周一日期。

| 粒度与产物 | 唯一键 | 最小字段、单位与空值 |
|---|---|---|
| run · `runs_clean` | `run_id` | `repo_id`、`workflow_path`、可空的 `workflow_id`，`created_at_utc` 作为归周时间。缺失或无法解析的创建时间交 P02 隔离。保留可空的 `updated_at_utc`、`event`、`status`、`conclusion`、`run_attempts`。从创建到更新的 `elapsed_seconds` 为浮点秒，更新时间缺失或倒序时记空并纳入质量统计 |
| 工作流 · `workflow_profile` | `repo_id, workflow_path` | 一行一个精确工作流。身份含可空的 `workflow_id`（取最小观测值）、`workflow_id_variants` 与 `workflow_id_conflict`。计数与窗口：`run_count`、`first_seen_utc`、`last_seen_utc`、`first_week_utc`、`last_week_utc`、`observation_span_days`、`active_day_count`、`active_day_rate`、`mean_runs_per_active_day`、`max_daily_runs`。结论：六类结论计数加 `other`/`unknown`、`skipped_rate`、`failure_rate`、`open_run_count`。重试：`attempts_known_count`、`total_attempts`、`mean_attempts`、`retried_run_count`、`retry_rate`、`max_attempts`，只统计 `status=completed` 的 run。耗时：已知、缺失、负值与零值计数及 `mean`/`median`/`p90`/`p99`/`max` 秒，只统计 completed 的有效耗时；进行中/排队的 run 计入 `elapsed_missing_count`、`open_run_count` 和活动类计数。间隔：`interval_count` 及 `mean`/`min`/`median`/`p90`/`max` 秒。时相：24 个 `hour_XX_utc_share`、`peak_hour_utc`、`hour_entropy_bits`、`night_share_utc`（UTC 0–5 时）。事件：18 个 `event_<name>_share`、`event_other_share`、`event_unknown_share`、`schedule_share`、`dispatch_share`、`webhook_share`、`top_event`、`event_type_count`、`event_entropy_bits`。比例取 0 到 1；零分母、缺失更新时间或单 run 的间隔记 null，并由 `failure_rate_estimable`、`retry_rate_estimable`、`elapsed_estimable`、`interval_estimable` 标记 |
| 工作流事件构成 · `workflow_event_mix` | `repo_id, workflow_path, event` | 精确 `run_count` 与 `run_share`。事件名保留源表原始值（空值记 `unknown`），用于回查画像比例和按事件复算 |
| 工作流周 · `workflow_week` | `repo_id, workflow_path, week_start_utc` | 周为 UTC 周一开始的半开七天区间，面板覆盖输入首个到最后一个 UTC 周的完整网格（11,396 × 59）。`observation_state` 四种值：首周与末周为 `partial`（采集窗口截断），完整观测工作流首个观测周之前的周为 `before_first_observation`（计数 null），完整观测的其余周为 `complete`（无运行时 `run_count=0`）；P01 样本中不属于完整 cohort 的锚点工作流，其有观测的周记 `partial`，其余所有未观测周（包括锚点之前）记 `unknown`（计数 null），避免把样本缺口写成零运行或“出现前”。字段：`run_count`、`active_day_count`、八类结论计数、`retried_run_count`、`elapsed_median_seconds`、`elapsed_p90_seconds`；周耗时与重试只统计 completed 的 run |
| 历史截点特征 · `workflow_asof` | `repo_id, workflow_path, cutoff_id` | 每个工作流在评分时点的既往特征。`window_start_utc` 为当时已观测到的最早 run，`window_end_utc = cutoff_utc`，`prior_window_days` 为窗口长度。`n_prior_runs` 为 cutoff 前创建的 run 数；`n_prior_outcomes` 为 completed、结论非空且 `updated_at` 早于 cutoff 的 run 数。`prior_skip_rate = 可见 skipped / n_prior_outcomes`；`prior_failure_rate = 可见 failure / (可见 success + failure)`；`prior_retry_rate = completed 且可见的 known attempts 中重试比例`；`prior_elapsed_*` 用 completed 且耗时有效的可见 run；`n_prior_known_attempts` 为 completed、可见且有尝试次数的 run 数。`prior_active_day_count`、`prior_runs_per_day`、`prior_*_gap_seconds` 只用创建时间。全局截点只为 cutoff 前已出现的工作流出行（表中没有该工作流即表示当时不可见）；`repo_id`/`workflow_path` 显式限定的截点允许冷启动行，此时 `has_prior_runs = false`。无结果记 null，用 `has_prior_runs`、`has_prior_outcomes` 标记 |
| attempt · `attempt_index`、`attempt_fingerprint` | `run_id, attempt` | 索引含 `repo_id`、输入根目录相对 `tar_path`、TAR 内 `tar_member` 与 ZIP 字节数 `archive_bytes`。指纹保留 `parse_status`（`parsed`、`no_target_member` 或 `failed`）、有效行数 `eligible_lines` 和信号计数。已解析零命中为 0，未能解析的特征为 null |
| 证据行 · `log_evidence` | `run_id, attempt, member, line_no` | `member` 是 ZIP 内文件名，`line_no` 是解码后原成员从 1 起算的行号，成员和行号不因过滤重排。同一行的多种命中放在其标签列表，展示字段为脱敏摘录 |

按工作流键和 UTC 周键将 run 分别连到一条画像与一条周记录。P03 的四张表由同一批次写出，`workflow_asof` 再按 `repo_id, workflow_path, cutoff_id` 关联。证据按 `run_id, attempt` 连接并保留归档与成员定位。run 级输出先聚合证据到 attempt，再聚合到 `run_fingerprint`，保留归档数、解析成功数和特征分母。与 `runs_clean` 左连，无归档行的日志特征为空。核对总行数与唯一 run 数均等于主表。模型分数、参数和证据键随批次保存。

P03 批次目录为 `output_root/p03-<sha256>`，含四张 Parquet 与 `manifest.json`：输入相对路径、字节数和 SHA-256，接口版本（当前 3）、截点参数与样本覆盖信息，分步输入输出行数，11 项对账结果，以及各产物行数和字节数。`status=success` 且产物字节数一致时才复用。`--cutoffs` 可传入自定义截点表（`cutoff_id`、`cutoff_utc`，可选 `repo_id`/`workflow_path` 限定单个工作流，两个范围列必须同时为空或同时非空），用于 P05 的回测周或 P07 的目标 run 时点。`--sample-manifest` 或输入目录内的 `sample_manifest.json` 用于标记样本中的完整 cohort；入口只发现一次并把“有清单/无清单”和文件哈希写进参数，读取层不再自行发现，发布前核验样本清单未出现、消失或改写。输入必须包含 `status` 列，缺失直接报错；支持嵌套的 Hive 分区目录（如 `year=2026/month=01/part-00000.parquet`）。入口冻结文件清单并传给读取层，发布前核验分片成员、输入快照、样本清单和截点文件哈希未变。

样本取各场景最小的 run ID。单次工作流按全表计数，多 attempt 以至少两个已索引 ZIP 为准。另按键选最多 12 个有归档且有 20–150 次运行的工作流，保留完整记录。场景可共享 run，输出校验 run ID 唯一。批次 ID 由输入相对路径及内容 SHA-256、模式、接口版本和样本参数决定。写完隐藏目录后一次性重命名发布。复用时检查完成状态、批次 ID、产物列表和文件大小。

- **时间。** `updated_at - created_at` 表示从创建到最后更新的观测耗时，可能涵盖等待或重跑。报告标清代理量；精确到某执行阶段的结论须用日志时间戳核验。UTC 小时仅支持 UTC 口径的结论。周键为 `created_at_utc` 所属 UTC 周的周一 00:00；完整观测时周面板的 `partial` 只由采集窗口的首末周决定，工作流自身的首末 run 不截断周；样本锚点工作流没有可证明的“出现前”，所有未观测周（含锚点之前）均为 `unknown`。截点特征取 `created_at < cutoff`；已知结果还要求 `status=completed`、结论非空且 `updated_at < cutoff`，耗时和尝试次数也只取 completed 的可见 run，因此追加未来记录不改变旧截点。`status` 是必需列，缺失不能默认 completed。
- **结果。** 跳过率取 `skipped / 全部 run`。建议失败率定义为 `failure / (success + failure)`，命名为“成功/失败结论中的失败率”；其余结论另列。分母为零记缺失。重试率取 completed 且已知尝试次数的 run 中 `run_attempts > 1` 的比例，重跑次数为尝试次数减一；进行中/排队的 run 不计入这个分母，日志里的进程内重试另算。
- **P03 特征。** 事件比例按事件计 `event_run_count / run_count`，18 个已知事件加 `other`/`unknown` 两类；`schedule_share` 取 `schedule`，`dispatch_share` 取 `workflow_dispatch + repository_dispatch`，`webhook_share` 为其余事件（含 `workflow_run`、`push`、PR 与 issue 类）。UTC 小时分布为 24 个 `hour_XX_utc_share`，`night_share_utc` 取 0–5 时。活跃天数按 UTC 日期去重；`observation_span_days = last_day - first_day + 1`，`active_day_rate = active_day_count / observation_span_days`，`mean_runs_per_active_day = run_count / active_day_count`，`max_daily_runs` 为同一 UTC 日的最大 run 数。耗时和尝试次数只统计 `status=completed` 的 run；进行中/排队的 run 只计入活动类计数和 `open_run_count`，其耗时计入缺失。间隔为同一工作流按创建时间排序后的相邻差，单 run 无间隔。耗时与间隔分位数用精确线性插值（`numpy.percentile`，method `linear`），非近似分位。小时与深夜结论仅对 UTC 口径成立。
- **覆盖。** 运行画像以 11,396 个有 run 的工作流为总体。教师提供的日志子集覆盖 475 个工作流；子集未提供归档、归档缺少目标步骤、已解析且零命中分别编码。对比实验的两组均取同一批可用 run。
- **时间切分。** 全期画像用于解释整体原型。回测时用 P03 的 `workflow_asof`：全局截点只为当时已出现的工作流出评分行，结果类特征要求 `status=completed`、结论非空且 `updated_at` 早于截点；耗时和尝试次数只取 completed 的可见 run。显式限定 `repo_id`/`workflow_path` 的截点允许冷启动行（`has_prior_runs=false`）。对只保留最新快照的结论、尝试数和耗时，用最后更新时间约束可见性。不能复原的中间 attempt 状态记作未知。

## 环境定版与复现

P02 由万明浩登记节点 OS/架构、JDK 发行版及主版本、Spark/PySpark 精确版本、Python 版本、共享路径、执行器数量与内存。按实装 Spark 选择兼容的 Java/Python，保持驱动与执行器 Python 小版本一致。把实测入口写入 README，冻结运行、实验和演示依赖。P08 全量复跑保存输入清单、代码版本、参数、时间范围和 Spark 应用 ID。

### 五节点巡检与环境现状

- **统一基线**：五台节点（`10.176.62.226` ~ `10.176.62.230`，登录用户 `root`）均为 CentOS Linux 7 (Core) x86_64（内核 `3.10.0-327.el7.x86_64`），均安装 Oracle JDK `1.8.0_471`，系统默认 Python 为 `2.7.5`（暂未发现 `python3` 或 `conda`）；当前各节点均无现成网络共享挂载（`mount` 无 NFS/HDFS 共享挂载）。
- **节点明细**：
  - `node1`（`10.176.62.226`）：4 核（Xeon E5649 @ 2.53GHz）/ 7.6G 内存（可用 ~2.0G）；`/` 50G（可用 39G）、`/home` 48G（可用 48G）；`/opt/spark` 指向 `spark-2.0.0-bin-hadoop2.7`，另有 `spark-3.5.7-bin-hadoop3` 与 `/opt/hadoop`；当前无 Hadoop/Spark 守护进程运行。
  - `node2`（`10.176.62.227`）：4 核 / 7.6G 内存（可用 ~1.1G）；`/` 50G（可用 16G）、`/home` 48G（可用 34G）；`/opt/spark` 指向 `spark-2.0.0-bin-hadoop2.7`，另有 `spark-3.5.7-bin-hadoop3`；运行着 `NameNode`、`SecondaryNameNode`、`ResourceManager`。
  - `node3`（`10.176.62.228`）：8 核 / 15G 内存（可用 ~14G）；`/` 50G（可用 30G）、`/home` 48G（可用 6.1G，已用 88%）；`/opt/spark` 指向 `spark-2.0.0-bin-hadoop2.7`，另有 `spark-3.5.7-bin-hadoop3`；运行着 `DataNode`、`NodeManager`；已验证可 SSH 连接。
  - `node4`（`10.176.62.229`）：4 核 / 7.6G 内存（可用 ~1.3G）；`/` 50G（可用 29G）、`/home` 48G（可用 6.1G，已用 88%）；`/opt/spark` 指向 `spark-2.0.0-bin-hadoop2.7`，另有 `spark-3.5.7-bin-hadoop3`；运行着 `DataNode`、`NodeManager`。
  - `node5`（`10.176.62.230`）：4 核 / 7.6G 内存（可用 ~5.7G）；`/` 50G（可用 41G）、`/home` 48G（可用 48G）；`/opt/spark` 指向 `spark-2.4.8-bin-hadoop2.7`，另有 `spark-2.0.0-bin-hadoop2.7`、`spark-3.5.7-bin-hadoop3`；运行着 Spark Standalone `Master`/`Worker`（`spark://10.176.62.230:7077`）及 `NameNode`/`SecondaryNameNode`/`DataNode`（单机 `localhost` HDFS，容量约 49.98GB，可用约 40.89GB）。
- **存储与跨节点可读写路径**：五台机器未安装 NFS 工具且无网络挂载，但 `node5`（`10.176.62.230`）运行着可用 HDFS（`Live datanodes (1): node5`，总容量 49.98 GB，可用 40.89 GB）。已在 `node5` 实测 `hdfs dfs -mkdir`、`-put` 写入、`-cat` 读出与 `-rm` 清理闭环，统一采用 **`hdfs://`** 路径（如 `hdfs:///ghawtrace/...`）作为跨节点作业的可读写数据路径（同时避开 `node3`、`node4` 已用 88% 的本地 `/home` 分区）。

### 集群节点角色分工

| 节点 | IP 地址 | 承担角色 | Spark / 存储说明 |
| :--- | :--- | :--- | :--- |
| `node1` | `10.176.62.226` | **Spark Master + Driver（提交端）** | Spark `2.0.0` Standalone Master（RPC `7077` / Web UI `8080`），`spark-submit --deploy-mode client` 入口 |
| `node2` | `10.176.62.227` | **Spark Worker** | Spark `2.0.0`，已成功注册至 `spark://10.176.62.226:7077` |
| `node3` | `10.176.62.228` | **Spark Worker** | Spark `2.0.0`，已在 P02-T02 烟雾测试中实测承担执行器计算 |
| `node4` | `10.176.62.229` | **Spark Worker** | Spark `2.0.0`，已在 P02-T02 烟雾测试中实测承担执行器计算 |
| `node5` | `10.176.62.230` | **HDFS 存储服务节点** | 提供统一存储入口 `hdfs://10.176.62.230:9000` |

### 运行环境与统一存储约定
- **基础环境**：CentOS 7，Oracle JDK `1.8.0_471`，系统 Python `2.7.5`（`PYSPARK_PYTHON=/usr/bin/python`，PySpark 脚本保持 Python 2.7 兼容语法）。
- **Spark 路径**：`node1`–`node4` 统一使用 `/opt/spark -> /opt/spark-2.0.0-bin-hadoop2.7`。
- **统一存储路径**：集群无 NFS 共享挂载，所有分布式读写统一走 `hdfs://10.176.62.230:9000`。因各节点本地 `fs.defaultFS` 配置不一致（如 `node1` 默认指向 `hdfs://node1:9000`），提交作业时**必须显式传入完整 `hdfs://10.176.62.230:9000/...` 路径并附加 `--conf spark.hadoop.fs.defaultFS=hdfs://10.176.62.230:9000`**。

### 常用启停与提交命令
- **启动 / 停止 Master（`node1`）**：
  - `/opt/spark/sbin/start-master.sh`
  - `/opt/spark/sbin/stop-master.sh`
- **启动 / 停止 Worker（`node2` / `node3` / `node4`）**：
  - `/opt/spark/sbin/start-slave.sh spark://10.176.62.226:7077`
  - `/opt/spark/sbin/stop-slave.sh`
- **标准提交模板（在 `node1` 执行）**：
  ```bash
  HDFS_URI="hdfs://10.176.62.230:9000"
  /opt/spark/bin/spark-submit \
    --master spark://10.176.62.226:7077 \
    --deploy-mode client \
    --conf spark.hadoop.fs.defaultFS="${HDFS_URI}" \
    <script.py> \
    "${HDFS_URI}/<input_path>" \
    "${HDFS_URI}/<output_path>"

- **Parquet 兼容性与时区约定**：Spark `2.0.0` 不支持新版 `pyarrow` 默认的 `ZSTD` 压缩编解码；原始 Parquet 分片需先按 P01 `RUN_INPUT_SCHEMA`（13 列，裁除 `github_metadata_json`）无损转为 `Parquet 1.0 + Snappy` 格式再上传 HDFS（`hdfs://10.176.62.230:9000/data/ghawtrace/runs_snappy`）。涉及 UTC 时间解析的作业，提交时必须附加 `--conf "spark.driver.extraJavaOptions=-Duser.timezone=UTC"` 与 `--conf "spark.executor.extraJavaOptions=-Duser.timezone=UTC"`

- **推荐分区与资源配置**：在 3 台 Worker 节点（实测分配 8 个 Executor × 2 核 / 1536m，总计 16 核）基线下，常规全量聚合分析统一使用 `--conf spark.sql.shuffle.partitions=24`（较默认 `200` 提速 27.6%、HDFS 输出小文件减少 88% 且零溢写）


P03 已在 CPython 3.12.7、PyArrow 16.1.0 与 25.0.1、NumPy 1.26.3 下用 P01 全量批次和 165 个原始分片分别复跑，四张输出逐表一致；`python -m unittest discover -s tests` 共 41 项通过（含 P01 的 5 项）。审核修复后全量单进程约 1–2 分钟，`workflow_asof` 为 206,141 行（59 个周一截点中首个无可见工作流，表中出现 58 个截点；覆盖 11,377 个工作流）；样本批次（747 个 run、17 个工作流）约 1–3 秒，5 个非 cohort 工作流的未观测周全部为 `unknown`（状态计数 before 432、complete 264、unknown 290、partial 17）。代码不含平台相关路径或 shell 调用，目标仍为集群 Linux。

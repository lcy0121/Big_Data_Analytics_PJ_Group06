# 技术栈与数据接口

开发机 CPython 3.12，固定 PyArrow 25.0.1 与 NumPy 1.26.3。P03 也在 CPython 3.12.7 / PyArrow 16.1.0 复核。课程集群为 Spark 2.0.0 Standalone、Hadoop 2.7.7 HDFS、Oracle JDK 8、Python 2.7.5。P01–P03 已验收，P05-T01 在本机 PySpark 4.0.1 / JDK 21 完成全量验收。P04、P05 其余任务和 P06–P09 待验收。[锁定版本](requirements.txt) · [第三方声明](THIRD_PARTY_NOTICES.md)

## 按流程选型

| 流程与任务 | 工具 | 输入、处理与输出 | 运行位置和验证 |
|---|---|---|---|
| 输入准备（P01） | unar、Python、PyArrow、`tarfile` | 解包 RAR。按列分批读取 Parquet，按仓库 TAR 的成员清单建立 attempt 索引 | 本地实测两种模式，按参数读取源目录。可用教师脚本的 `iter_shards`、`load_runs`（pandas）或 `query`（DuckDB）独立抽查 |
| 批处理环境（P02） | 集群 Linux、JVM、Apache Spark、PySpark、`spark-submit` | 提交驱动程序并将清洗、聚合、分区写回分发给执行器 | node1 为提交机和 Master，node2–4 为 Worker，node5 提供 HDFS。以实际执行器记录认定多机运行。[集群说明](https://spark.apache.org/docs/latest/cluster-overview.html) |
| 数据存取与清洗（P02） | 共享存储、Parquet、Spark SQL / DataFrame | 分片投影、类型和 UTC 转换、主键与状态审计、分区落盘。按读取模式比较分区方案，避免按 run 建目录 | Spark 全量清洗使用 HDFS 不可变批次、Snappy Parquet 和 UTC 时间列。[Spark Parquet 文档](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) |
| 日志覆盖（P02-T04） | PyArrow 批读、Python tarfile、SQLite 3.53.1（sqlite3） | 独立扫描 TAR 成员与 P01 attempt 索引逐键、大小及重数比对。在临时磁盘库以唯一 run 左连同仓库非空 ZIP，按 UTC 创建月 × 原始 event 汇总 | 同一入口支持样本和全量；完整 TAR 清单、异常项与分母见[覆盖报告](reports/p02/p02_t04_coverage.json) |
| 特征工程（P03） | PyArrow（`pyarrow.dataset`、`pyarrow.compute`）、NumPy 分组扫描、`ghawtrace/features.py` | 读 P01 批次、P02 清洗表或嵌套分区 Parquet 目录；输入必须含 `status` 列；按 `(repo_id, workflow_path)` 排序分组；P01 样本按 `sample_manifest.json` 的 cohort 区分完整观测与锚点；产出工作流画像、事件构成、连续周面板和按历史截点特征；全部写 Parquet | 本地实测 165 个分片、4,112,526 个 run、11,396 个工作流与 59 个 UTC 周，单进程约 1–2 分钟；手算样例与 11 项对账检查随批次记录。P04–P07 取已发布的 Parquet；Spark 2 通过 read_features 恢复微秒 UTC 类型 |
| 集群画像复算（P03-T02） | PySpark 2.0.0 RDD、Python 2.7 标准库、HDFS | 从固定成功批次的类型化 UTC 列提取整数微秒，按仓库和精确路径分组，计算画像全部 106 列。每次作业保存 24 个 gzip 分区及实际处理工作流的节点记录 | 三台 Worker 两轮通过。PyArrow 对照每个键、类型、空值，浮点绝对和相对容差均为 1e-12。Spark 2 读取四表使用 `ghawtrace/p03/read_features.py`。[验收报告](reports/p03/p03_t02_repeat.json) |
| 聚类（P04） | `pyspark.ml` 的 VectorAssembler、StandardScaler、KMeans；Python 3 端用 scikit-learn 的 silhouette_score、adjusted_rand_score 评估 | 数值向量 → 缩放 → 拟合多个 K 和种子 → 对照轮廓系数、簇规模、ARI；用原始单位解释中心与代表样本 | Spark 2.0 拟合；在固定样本上记录距离、缩放、轮廓系数及跨种子 ARI。Spark ClusteringEvaluator 自 2.3.0 引入。[API 版本](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.evaluation.ClusteringEvaluator.html) [聚类与评估](https://spark.apache.org/docs/latest/ml-clustering.html) |
| 运行级异常（P05-T01） | PySpark DataFrame 按工作流重分区、分区内按时间与 run ID 排序；Python 有界窗口精确求中位数、MAD 和重试尾部频率 | 一个 run 一行，保留历史支持度、两个分项、综合分数和原因 | 本机 Spark 4.0.1 双跑，独立 Python 复算。见[评分口径](#p05-scoring) |
| 生命周期与时间回测（P05-T02、P05-T03） | Spark SQL 周面板、持续性规则、时间顺序回放 | 一个工作流可有多个沉寂、恢复或突发区间，保存开始周、结束周和历史节律 | 在已知变化点上核验告警延迟，用稳定序列估计误报，标出末端截尾区间 |
| 日志指纹（P06） | Python 标准库 `tarfile`、`zipfile`、`io`、`re`；PyArrow/Parquet | 以仓库 TAR 为分工单元，依次读单个 ZIP 和文本成员；记录规则命中、有效行数、成员定位、解析状态，先聚合 attempt 再形成 run 特征 | 批处理按 TAR 分配工作；保留证据行号，避免在 driver 解压整批数据或把全文收集至内存 |
| 联合无监督分析（P07） | scikit-learn IsolationForest、pandas、DuckDB | 在同一有日志 run 集合上比较结构化与联合模型；对排名变化案例移除对应的日志特征组后重训，关联原始证据。将 `score_samples` 转为高分表示异常的统一方向 | 约三万条 run 的特征矩阵可在协调节点上建模。记录时间切分、种子、缺失率、排名变化及 Top-K 重合。[模型接口](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html) |
| 查询、演示与协作（P08/P09） | DuckDB、Streamlit、Plotly.py、Git | 从已产出结果按工作流、run 和时间范围查询，绘制原型分布、时间线、异常排名与证据摘要；Git 记录源码与文档版本 | 演示端读取汇总表和索引，支持无日志状态。Git 权限见 [项目规则](AGENTS.md) |

## 课程资料与查阅路径

按原文件名和幻灯片页码查找课程群或组内共享课件，开发时对照实际发行版。

| 对应任务 | 课程资料 | 项目中的用法与对照入口 |
|---|---|---|
| P02 集群与存储 | `week1.2.Hadoop概述.pptx` 第 17 页，`week2.1.HDFS.pptx` 第 19–25、44–50、51–54 页，`week6.1.spark-general.pptx` 第 27–30 页 | 根据资源管理器、存储和执行器的职责划分节点。使用 HDFS，核验输入与产物对各节点可见。按仓库保留日志 TAR，避免上传大量单条日志文件。[Spark 集群部署](https://spark.apache.org/docs/latest/cluster-overview.html) |
| P02-T05、P03、P05-T01 分区与窗口 | `week2.2.Mapreduce.pptx` 第 16、20–29 页，`week6.2.spark原理.pptx` 第 14–21、26–27 页 | 用按键重分区、宽依赖、Shuffle 和 Stage 理解聚合与窗口的成本。在同一全量作业比较两种分区设置，记录执行器、Shuffle 和 Stage 耗时。历史窗口依据实际可见时间构造。[Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html) · [Spark SQL 窗口函数](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html) |
| P04 画像聚类 | `week3.2.Kmeans.pptx` 第 3–8 页，`week4.1.机器学习.pptx` 第 50–56 页，`week4.2.聚类.pptx` 第 4–10 页 | 参照“分配到最近中心、重算质心”的迭代关系解释 Spark MLlib 结果。课件对距离和初始中心的讨论用于设计量纲、K 和随机种子的对照实验。[Spark MLlib 聚类](https://spark.apache.org/docs/latest/ml-clustering.html) |
| P06–P07 日志文本特征备选 | `week3.1.tfidf.pptx` 第 2–7 页 | 课件区分单篇词频与全语料文档频率。若规则指纹遗漏较多可重复的模式，可把归一化日志模板作为词、run 作为文档，试验只在训练集合拟合的 TF-IDF；现有规则指纹继续提供可回查证据。[Spark MLlib TF-IDF](https://spark.apache.org/docs/latest/ml-features.html#tf-idf) |

## 表的粒度与分析口径

P01 从 19 个源字段投影 13 列，保留原始 UTC 字符串、状态和空值。P02 将相同 13 列从原始分片转码，11 月 27 日在 P03 用 P01 与 P02 的全量输入交叉复核。`log_archive_count`、`log_file_count` 和 `empty_archive_count` 属于源数据统计。`attempt_index.parquet` 记录课程 TAR 中的 ZIP 位置，ZIP 内容及目标步骤由后续阶段核验。[输出模式与命令](README.md#开发者第一次运行)

下表为现有产物与 P02–P07 的下游约定。主键非空，ID 使用 int64，attempt 为从 1 起的 int32，路径保留原文及大小写。下游用 Parquet 微秒 UTC 时间戳，Spark 会话设为 UTC，周键取周一日期。

| 粒度与产物 | 唯一键 | 最小字段、单位与空值 |
|---|---|---|
| run · `runs_clean` | `run_id` | `repo_id`、`workflow_path`、可空的 `workflow_id`，`created_at_utc` 作为归周时间。缺失或无法解析的创建时间交 P02 隔离。保留可空的 `updated_at_utc`、`event`、`status`、`conclusion`、`run_attempts`。从创建到更新的 `elapsed_seconds` 为浮点秒，更新时间缺失或倒序时记空并纳入质量统计 |
| 工作流 · `workflow_profile` | `repo_id, workflow_path` | 一行一个精确工作流。身份含可空的 `workflow_id`（取最小观测值）、`workflow_id_variants` 与 `workflow_id_conflict`。计数与窗口：`run_count`、`first_seen_utc`、`last_seen_utc`、`first_week_utc`、`last_week_utc`、`observation_span_days`、`active_day_count`、`active_day_rate`、`mean_runs_per_active_day`、`max_daily_runs`。结论：六类结论计数加 `other`/`unknown`、`skipped_rate`、`failure_rate`、`open_run_count`。重试：`attempts_known_count`、`total_attempts`、`mean_attempts`、`retried_run_count`、`retry_rate`、`max_attempts`，只统计 `status=completed` 的 run。耗时：已知、缺失、负值与零值计数及 `mean`/`median`/`p90`/`p99`/`max` 秒，只统计 completed 的有效耗时；进行中/排队的 run 计入 `elapsed_missing_count`、`open_run_count` 和活动类计数。间隔：`interval_count` 及 `mean`/`min`/`median`/`p90`/`max` 秒。时相：24 个 `hour_XX_utc_share`、`peak_hour_utc`、`hour_entropy_bits`、`night_share_utc`（UTC 0–5 时）。事件：18 个 `event_<name>_share`、`event_other_share`、`event_unknown_share`、`schedule_share`、`dispatch_share`、`webhook_share`、`top_event`、`event_type_count`、`event_entropy_bits`。比例取 0 到 1；零分母、缺失更新时间或单 run 的间隔记 null，并由 `failure_rate_estimable`、`retry_rate_estimable`、`elapsed_estimable`、`interval_estimable` 标记 |
| 工作流事件构成 · `workflow_event_mix` | `repo_id, workflow_path, event` | 精确 `run_count` 与 `run_share`。事件名保留源表原始值（空值记 `unknown`），用于回查画像比例和按事件复算 |
| 工作流周 · `workflow_week` | `repo_id, workflow_path, week_start_utc` | 周为 UTC 周一开始的半开七天区间，面板覆盖输入首个到最后一个 UTC 周的完整网格（11,396 × 59）。`observation_state` 四种值：完整观测工作流先将首次出现前的周记为 `before_first_observation`（计数 null），此后处在采集窗口首、末周的记为 `partial`，其余为 `complete`（无运行时 `run_count=0`）；P01 样本中不属于完整 cohort 的锚点工作流，其有观测的周记 `partial`，其余所有未观测周（包括锚点之前）记 `unknown`（计数 null），避免把样本缺口写成零运行或“出现前”。字段：`run_count`、`active_day_count`、八类结论计数、`retried_run_count`、`elapsed_median_seconds`、`elapsed_p90_seconds`；周耗时与重试只统计 completed 的 run |
| 历史截点特征 · `workflow_asof` | `repo_id, workflow_path, cutoff_id` | 每个工作流在评分时点的既往特征。`window_start_utc` 为当时已观测到的最早 run，`window_end_utc = cutoff_utc`，`prior_window_days` 为窗口长度。`n_prior_runs` 为 cutoff 前创建的 run 数；`n_prior_outcomes` 为 completed、结论非空且 `updated_at` 早于 cutoff 的 run 数。`prior_skip_rate = 可见 skipped / n_prior_outcomes`；`prior_failure_rate = 可见 failure / (可见 success + failure)`；`prior_retry_rate = completed 且可见的 known attempts 中重试比例`；`prior_elapsed_*` 用 completed 且耗时有效的可见 run；`n_prior_known_attempts` 为 completed、可见且有尝试次数的 run 数。`prior_active_day_count`、`prior_runs_per_day`、`prior_*_gap_seconds` 只用创建时间。全局截点只为 cutoff 前已出现的工作流出行（表中没有该工作流即表示当时不可见）；`repo_id`/`workflow_path` 显式限定的截点允许已知键在首次 run 前产生冷启动行，此时 `has_prior_runs = false`。无结果记 null，用 `has_prior_runs`、`has_prior_outcomes` 标记 |
| attempt · `attempt_index`、`attempt_fingerprint` | `run_id, attempt` | 索引含 `repo_id`、输入根目录相对 `tar_path`、TAR 内 `tar_member` 与 ZIP 字节数 `archive_bytes`。指纹保留 `parse_status`（`parsed`、`no_target_member` 或 `failed`）、有效行数 `eligible_lines` 和信号计数。已解析零命中为 0，未能解析的特征为 null |
| 证据行 · `log_evidence` | `run_id, attempt, member, line_no` | `member` 是 ZIP 内文件名，`line_no` 是解码后原成员从 1 起算的行号，成员和行号不因过滤重排。同一行的多种命中放在其标签列表，展示字段为脱敏摘录 |

按工作流键和 UTC 周键将 run 分别连到一条画像与一条周记录。P03 的四张表由同一批次写出，`workflow_asof` 再按 `repo_id, workflow_path, cutoff_id` 关联。证据按 `run_id, attempt` 连接并保留归档与成员定位。run 级输出先聚合证据到 attempt，再聚合到 `run_fingerprint`，保留归档数、解析成功数和特征分母。与 `runs_clean` 左连，无归档行的日志特征为空。核对总行数与唯一 run 数均等于主表。模型分数、参数和证据键随批次保存。

P03 批次目录为 `output_root/p03-<sha256>`，含四张 Parquet 与 `manifest.json`：输入相对路径、字节数和 SHA-256，接口版本（当前 3）、截点参数与样本覆盖信息，分步输入输出行数，11 项对账结果，以及各产物行数和字节数。集群正式交付路径和 Spark 2 读取命令见 [README](README.md#组员取用已验收的全量表)。`status=success` 且产物字节数一致时才复用。`--cutoffs` 可传入自定义截点表（`cutoff_id`、`cutoff_utc`，可选 `repo_id`/`workflow_path` 限定单个工作流，两个范围列必须同时为空或同时非空），用于 P05 的回测周或 P07 的目标 run 时点。`--sample-manifest` 或输入目录内的 `sample_manifest.json` 用于标记样本中的完整 cohort；入口只发现一次并把“有清单/无清单”和文件哈希写进参数，读取层不再自行发现，发布前核验样本清单未出现、消失或改写。输入必须包含 `status` 列，缺失直接报错；支持嵌套的 Hive 分区目录（如 `year=2026/month=01/part-00000.parquet`）。入口冻结文件清单并传给读取层，发布前核验分片成员、输入快照、样本清单和截点文件哈希未变。

样本取各场景最小的 run ID。单次工作流按全表计数，多 attempt 以至少两个已索引 ZIP 为准。另按键选最多 12 个有归档且有 20–150 次运行的工作流，保留完整记录。场景可共享 run，输出校验 run ID 唯一。批次 ID 由输入相对路径及内容 SHA-256、模式、接口版本和样本参数决定。写完隐藏目录后一次性重命名发布。复用时检查完成状态、批次 ID、产物列表和文件大小。

- **时间与可见性。** 观测耗时是从创建到最后更新的秒数，可含等待和平台重跑；具体执行阶段须用日志核验。周键为创建时间所在 UTC 周的周一，小时结论限于 UTC。回测依照上表的截点和最终更新时间限制可见性，中间 attempt 状态无法还原时记未知。
- **分母与分位数。** 跳过率 = skipped / 全部 run，失败率 = failure / (success + failure)，重试率 = 已知尝试次数 ≥ 1 的 completed run 中尝试次数 > 1 的比例；零分母记 null。平台重跑次数为尝试次数减一，日志内部重试另计。相邻创建时间差为间隔；单 run 没有间隔。耗时与间隔使用精确线性插值分位数。
- **活动与事件。** 事件及 UTC 小时比例均除以该工作流全部 run。`schedule_share` 取 schedule，`dispatch_share` 取 workflow_dispatch 和 repository_dispatch，其余有效事件计入 webhook，unknown 单列。UTC 活跃日去重，跨日跨度含首尾；活跃日率、日均运行数分别为活跃日数 / 跨度、run 数 / 活跃日数。峰值日取单日最大 run 数。
- **覆盖。** 全量画像含 11,396 个工作流，课程日志覆盖其中 475 个。无归档、无目标步骤、已解析零命中分别编码。联合实验统一取两种模型均可用的 run。

<a name="p05-scoring"></a>

### P05-T01 评分与输出

- **评分时点。** `run_scores` 按 `run_id` 唯一，以 UTC 最终更新时间评分。目标和参照均要求 completed、结论有值且非 skipped、更新时间有效且不早于创建时间。失败 run 可以参与。覆盖范围为整个清洗批次，每个跳过、未完成或时间无效的目标仍占一行。
- **窗口。** 在同一 `(repo_id, workflow_path)` 取严格早于评分时点的最近 64 个合格 run。按更新时间、run_id 升序取末尾；同一时刻完成的目标共享先前窗口。记录 `history_start_utc`、`history_end_utc`、`n_history`，每个分项至少需要 5 个有效历史观测。这里按完成时点取历史，可纳入目标运行期间已完成的其他 run。
- **耗时分项。** 观测秒数沿用 P02 的创建至更新时间差。历史有效耗时的精确中位数为 m，中位绝对偏差为 MAD。令 d = |目标耗时 − m|，则 `elapsed_score = d / (d + max(1 秒, 1.4826 × MAD))`。一秒下限保留零 MAD 情况下的可复算分数。记录 `n_elapsed`、`median_seconds`、`mad_seconds`。
- **重试分项。** 按平台 `run_attempts`（至少 1 次）计算。n 为历史已知次数的 run 数，t 为历史次数大于等于本次的 run 数。`retry_score = 1 − (1+t)/(1+n)`；1 次时得 0。记录 `n_attempts` 和 `tail_count`。该分项衡量平台重跑的罕见程度。
- **合成与复用。** `score` 取可用分项最大值，越大表示偏离越大。`score_status` 为 scored、partial 或 unscored。两项均不可算时分数为空；分母、目标观测值和 `reason` 可定位原因。正常基线为 within_baseline，非零分写出占主导的耗时方向、重试或并列原因。单项历史不足时仅用另一项。这里的分数用于排序，异常阈值及误报评估属于 P05-T03。
- **发布。** 每次运行写新批次，读回 Parquet 比较输入输出键集、总行数、唯一键数及时间类型后发布成功 manifest。manifest 保存 P02 token、分片摘要、代码 SHA-256、窗口配置、样本清单摘要、Spark 版本、应用 ID 和各执行节点实际输出数量。本机样本与全量均从同一 P02 来源取历史。[运行命令](README.md#p05-run-scores)

## 环境定版与复现

万明浩完成五节点巡检。下表容量为 11 月 22 日的快照；角色为 T02 定版后实际参与作业的分工。统一使用 CentOS 7 x86_64、内核 `3.10.0-327.el7.x86_64`、Oracle JDK `1.8.0_471`、系统 Python `2.7.5`。当时未发现 Python 3、conda、NFS 工具或网络挂载。五机均备有 Spark 3.5.7；计算统一用 node1–4 的 `/opt/spark`（2.0.0），驱动与执行器使用 `/usr/bin/python`。

| 节点 / IP | 验收角色 | 核数；内存总/可用（G） | 根分区 / home 可用（G） | 巡检时的进程或差异 |
|---|---|---|---|---|
| node1 / 10.176.62.226 | Spark Master、client Driver；RPC 7077、UI 8080 | 4（Xeon E5649 2.53 GHz）；7.6/2.0 | 39 / 48 | 初查无守护进程，后启动 Master；有 /opt/hadoop |
| node2 / 10.176.62.227 | Spark Worker | 4；7.6/1.1 | 16 / 34 | 原有 NameNode、SecondaryNameNode、ResourceManager |
| node3 / 10.176.62.228 | Spark Worker | 8；15/14 | 30 / 6.1 | 原有 DataNode、NodeManager；已验证 SSH |
| node4 / 10.176.62.229 | Spark Worker | 4；7.6/1.3 | 29 / 6.1 | 原有 DataNode、NodeManager |
| node5 / 10.176.62.230 | Hadoop 2.7.7 HDFS | 4；7.6/5.7 | 41 / 48 | 原有本机 Spark 2.4.8 Master/Worker 和 NameNode、SecondaryNameNode、DataNode；另装 Spark 2.0.0 |

五机根分区总量 50G、home 48G。node3/4 的 home 当时已用 88%。组内共享 URI 为 `hdfs://10.176.62.230:9000`；node5 单 DataNode 实测容量 49.98 GB、可用 40.89 GB，已验证 mkdir、put、cat、rm。各机默认文件系统不同（node1 曾指向 node1:9000）。显式使用完整 URI；作业涉及默认路径时另设 `spark.hadoop.fs.defaultFS`。清洗和聚合均以 JVM UTC 运行。

| 操作 | 所在节点与命令 |
|---|---|
| Master 启动 / 停止 | node1：`/opt/spark/sbin/start-master.sh` / `/opt/spark/sbin/stop-master.sh` |
| Worker 启动 / 停止 | 各 Worker：`/opt/spark/sbin/start-slave.sh spark://10.176.62.226:7077` / `/opt/spark/sbin/stop-slave.sh` |
| 清洗、压测、画像提交 | node1；实参、HDFS 路径、驱动与执行器 UTC 配置见 [README](README.md#p02-集群清洗与分区实验) |

原始分片使用 ZSTD，转码器投影合同中的 13 列为 Parquet 1.0 + Snappy，记录各原片与输出 SHA-256；`github_metadata_json` 等六列仅留原件。清洗全量重跑按固定批次发布，257 个未完成 run 计入活动。P02 分区验收基于三台 Worker、8 个执行器 × 2 核 / 1536m。固定聚合从 200 调到 24 分区，输出均为 10,302 行且摘要相同：耗时 30.410→21.607 秒（缩短 28.95%），输出分区减少 88%，两组零溢写。P03 复跑使用 24 分区、核数上限 12。[200 分区](reports/p02/p02_t05_p200_report.json) · [24 分区](reports/p02/p02_t05_p24_report.json)

P03 全量截点共 59 个周一，第一个没有可见工作流，实际输出 58 个截点、11,377 个工作流、206,141 行。747 个 run 的样本通常需 1–3 秒，17 个工作流含 5 个非 cohort；周状态 before/complete/unknown/partial 依次为 432/264/290/17。P08 复跑记录输入、代码版本、参数、时间范围和 Spark 应用 ID。[版本与实验差异](history.md#p03-cluster)

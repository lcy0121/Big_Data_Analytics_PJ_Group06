# 技术栈与数据接口

本机 CPython 3.12、PyArrow 25.0.1、NumPy 1.26.3；P03 另验 CPython 3.12.7 / PyArrow 16.1.0。集群 Spark 2.0.0 Standalone、Hadoop 2.7.7、Oracle JDK 8、Python 2.7.5。P00–P03、P05-T01 已验收，P05-T01 另用本机 PySpark 4.0.1 / JDK 21 全量对账。[计划](plan.md) · [依赖锁定](requirements.txt) · [第三方声明](THIRD_PARTY_NOTICES.md)

## 按流程选型

| 任务 | 工具、处理与交付 |
|---|---|
| P01 输入 | unar 解 RAR，PyArrow 投影，`tarfile` 列成员建 attempt 索引。样本、全量均已验证。教师脚本提供 `iter_shards`、`load_runs`（pandas）、`query`（DuckDB）。 |
| P02 清洗 | Linux/JVM 上用 `spark-submit`、PySpark SQL/DataFrame 分布式清洗、聚合、写 Snappy Parquet。按执行器核实多机，审计主键、状态、UTC 后发布不可变批次。按读取负载选分区，避免每个 run 单建目录。 [部署](https://spark.apache.org/docs/latest/cluster-overview.html) · [Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) |
| P02-T04 覆盖 | PyArrow 批读，`tarfile` 枚举，与 P01 索引逐键比大小、重数。SQLite 3.53.1 将唯一 run 左连同仓库非空 ZIP，按 UTC 创建月 × 原始 event 汇总。样本/全量共用入口。 [清单、异常与分母](reports/p02/p02_t04_coverage.json) |
| P03 四表 | PyArrow dataset/compute、NumPy 按仓库和精确路径排序扫描。输入可取 165 个原片或固定批次 36 个清洗分片。全量 4,112,526 个 run、11,396 个工作流、59 个 UTC 周，单进程约 1–2 分钟。手算例与 11 项对账见下文。 [实现](ghawtrace/features.py) |
| P03-T02 集群画像 | PySpark 2 RDD 与 Python 2.7 标准库读固定清洗表，以 UTC 整数微秒在三台 Worker 双跑画像。每轮 24 个 gzip 分区含实际节点。PyArrow 按键比较 106 列的类型、空值、数值，浮点绝对/相对容差均 1e-12。 [复跑报告](reports/p03/p03_t02_repeat.json) |
| P04 聚类（计划） | Spark 2 的 VectorAssembler、StandardScaler、KMeans 拟合多组 K 和种子，按原始单位解释质心与代表样本。Python 3 用 scikit-learn 在固定样本记录距离、缩放、轮廓系数、簇规模与跨种子 ARI。Spark ClusteringEvaluator 始于 2.3.0。[API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.evaluation.ClusteringEvaluator.html) · [聚类](https://spark.apache.org/docs/latest/ml-clustering.html) |
| P05-T01 运行评分 | 按工作流重分区排序，用有界窗口精确算中位数、MAD、重试尾频。集群 Spark 2、本机 Spark 4 各双跑，跨版本逐键一致，独立复算 751 个目标。 [接口](#p05-scoring) |
| P05-T02/T03 生命周期与回测（计划） | Spark SQL 周面板、持续性规则、时间回放。按工作流保留多段沉寂/恢复/突发的起止周与既往节律。已知变化点测延迟，稳定序列测误报，标出末端截尾。 |
| P06 日志指纹（计划） | 按仓库 TAR 分工，Python `tarfile/zipfile/io/re` 逐 ZIP 和成员读取，避免 driver 整批解压或收集全文。Parquet 记命中、有效行数、定位和解析状态，attempt 汇总到 run。 |
| P07 联合评分（计划） | 在协调节点用 scikit-learn IsolationForest、pandas、DuckDB 处理约三万条有日志 run。同样本对比结构化与联合模型，将 `score_samples` 转为高分表异常。记录切分、种子、缺失率、排名和 Top-K 重合；移除相关日志特征组重训，回查原始证据。[接口](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html) |
| P08/P09 演示与协作（计划） | DuckDB 查汇总表和索引，Streamlit/Plotly.py 按工作流、run、时间展示原型、时间线、排名、证据和无日志状态。Git 管理源码、文档。 [权限](AGENTS.md) |

## 课程资料与查阅路径

页码从 1 起。《大数据分析技术项目-2026版》第 4–6 页规定选题、集群与交付。按 `week1.1.大数据分析技术.pptx` 第 32 页分类，清洗、画像、评分属离线分析，P08 计划交互查询已发布结果。

| 对应任务 | 课程资料 | 项目中的用法与对照入口 |
|---|---|---|
| P01/P02 列式存储 | `week7.3.parquet.pptx` 第 2–8、12–15 页 | HDFS 提供共享文件，Parquet 组织列，Spark 或 PyArrow 执行计算。行组内按列分块、按页编码和压缩。实测原片为 Parquet 2.6/ZSTD；转码器只读取 19 列中的 13 列，以 Parquet 1.0/Snappy 写给课程 Spark 2。[转码实现](ghawtrace/p02/convert_for_spark2.py) |
| P02 集群与存储 | `week1.2.Hadoop概述.pptx` 第 11–17 页，`week2.1.HDFS.pptx` 第 19–25、43、45–50、52–54 页，`week6.1.spark-general.pptx` 第 27–31 页 | 本组用 Spark Standalone 的 Master/Worker 调度，node5 提供 HDFS。验证跨节点读写；按仓库保存日志 TAR，减少 HDFS 小文件与元数据开销。[Spark 集群部署](https://spark.apache.org/docs/latest/cluster-overview.html) |
| P02-T05、P03、P05-T01 分区 | `week2.2.Mapreduce.pptx` 第 16、20–29、44 页，`week6.2.spark原理.pptx` 第 14–21、26–27 页 | 同一键的数据归入同一计算分区。用宽依赖、Shuffle 和 Stage 解释 P02 同一全量聚合在 200/24 分区下的耗时，并用执行器记录核对实际节点。[实验报告](reports/p02/p02_t05_p24_report.json) · [Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html) |
| P04 画像聚类（计划） | `week3.2.Kmeans.pptx` 第 3–8 页，`week4.1.机器学习.pptx` 第 51–57 页，`week4.2.聚类.pptx` 第 4–10 页 | 样本分配到最近中心，再重算质心。距离受量纲影响，初始中心、离群点及非凸簇会影响解释；用计划中的缩放、多 K、多种子、代表样本和边界样本核验。[Spark MLlib 聚类](https://spark.apache.org/docs/latest/ml-clustering.html) |
| P05-T01 历史窗口 | `week5.1_stream.pptx` 第 16、18–21 页 | Spark 批作业按工作流、最终更新时间排序，用最近 64 个合格 run 的计数滑动窗口精确算中位数和 MAD。同刻完成者共用此前窗口。[窗口实现](ghawtrace/p05/score_runs.py) |
| P06–P07 日志文本特征备选 | `week3.1.tfidf.pptx` 第 2–7 页 | 课件区分单篇词频与全语料文档频率。若规则指纹遗漏较多可重复的模式，可把归一化日志模板作为词、run 作为文档，试验只在训练集合拟合的 TF-IDF；现有规则指纹继续提供可回查证据。[Spark MLlib TF-IDF](https://spark.apache.org/docs/latest/ml-features.html#tf-idf) |
| P05-T01 已验收；P08 查询待做 | `week7.1.交互式查询.pptx` 第 2–6、9–10 页，`week7.2.Dremel.pptx` 第 23、25、27 页 | P05 只投影 Worker、状态、原因，分批分组相加，同 Spark 清单对账。P08 可按工作流和时间筛高分 run，再按证据键定位 attempt 与日志行。[验收实现](ghawtrace/p05/verify_scores.py) |

## 表的粒度与分析口径

P01 投影、P02 转码均取源表 19 列中的 13 列，保留 UTC 字符串、状态和空值，两路全量经 P03 对账。源字段 `log_archive_count/log_file_count/empty_archive_count` 与课程包实物分计。P01 定位 ZIP，P02-T04 核 TAR 成员，P06 解析 ZIP 和目标步骤。[输出与命令](README.md#开发者第一次运行)

表键非空，路径保留原文与大小写。run、仓库数值 ID 为 int64，P01 的 `attempt` 为从 1 起的 int32，P05 的 `run_attempts` 为可空 long，`cutoff_id` 为字符串。统一 UTC、周一日期。P02/P05 在 Spark 中读为 timestamp；P03 的五个 Parquet UTC 微秒时间字段在 Spark 2 默认读为 bigint，用 [read_features](ghawtrace/p03/read_features.py) 恢复 timestamp。

| 粒度与产物 | 唯一键 | 最小字段、单位与空值 |
|---|---|---|
| run · `runs_clean` | `run_id` | 仓库、精确路径、可空 workflow ID。按 `created_at_utc` 归周，缺失或不可解析的创建时间隔离。保留可空的 `updated_at_utc/event/status/conclusion/run_attempts`，`elapsed_seconds` 为创建到更新的浮点秒，更新缺失或倒序记空并计质量指标。 |
| 工作流 · `workflow_profile` | `repo_id, workflow_path` | [106 列及类型](ghawtrace/features.py)。取最小非负 workflow ID，记变体数/冲突。活动含 run 数、首末时间/周、跨度、活跃日数/率、活跃日日均及日峰值。六类结论及 other/unknown，跳过率、失败率、开放数。completed 尝试含已知数、总数、均值、最大值、重试数/率。耗时含已知/缺失/负/零计数，及有效秒数的均值、中位、P90、P99、最大。进行中、排队计活动、开放与耗时缺失。间隔含个数、均值、最小、中位、P90、最大秒数。24 小时占比、峰时及占比、熵、UTC 0–5 时占比；18 类事件及 other/unknown 占比、schedule/dispatch/webhook 占比、首位事件及占比、种类数、熵。比例 0–1，失败/重试/耗时/间隔各有 estimable 标志。 |
| 事件 · `workflow_event_mix` | `repo_id, workflow_path, event` | 原始事件（缺失记 unknown）的 `run_count` 与 `run_share`，可回查画像比例。 |
| 周 · `workflow_week` | `repo_id, workflow_path, week_start_utc` | 全输入首末 UTC 周连续网格（11,396 × 59），周一开始，七天半开。完整工作流出现前 `before_first_observation`（计数 null），此后采集首末周 `partial`，其余 `complete`（无 run 为 0）。非 cohort 锚点有观测周 `partial`，其余含出现前均 `unknown`（计数 null）。含 run 数、活跃日数、八类结论数、completed 重试数和有效耗时中位数、P90。 |
| 截点 · `workflow_asof` | `repo_id, workflow_path, cutoff_id` | 窗口从当时最早可见 run 到 cutoff，天数为 `prior_window_days`。`n_prior_runs` 计截点前创建的 run；活跃日、日频率、间隔仅依创建时间。结论须 completed、非 null、最终更新严格早于 cutoff，所得 `n_prior_outcomes` 作跳过率分母，失败率用可见 success + failure。`n_prior_known_attempts` 为截点前已更新的 completed 中有效次数的 run 数，作重试率分母；耗时同样筛状态、更新时间，再取有效值。全局截点只出已出现工作流；显式指定已知键可在首个 run 前出冷启动行。不可估计记 null，以 `has_prior_runs/has_prior_outcomes` 标记。 |
| attempt · `attempt_index`（现有）、`attempt_fingerprint`（P06 约定） | `run_id, attempt` | 索引含 `repo_id`、相对输入根的 `tar_path`、`tar_member`、`archive_bytes`。指纹将记 `parse_status`（parsed/no_target_member/failed）、`eligible_lines`、信号计数。已解析零命中记 0，未解析特征为 null。 |
| 证据行 · `log_evidence`（P06 约定） | `run_id, attempt, member, line_no` | ZIP 成员原名，解码后从 1 起的原始行号，过滤保留原定位。一行多命中共用标签列表，展示脱敏摘录。 |

run 按工作流键连画像、加 UTC 周键连周表，截点另加 `cutoff_id`。P06/P07 约定按 run/attempt 连证据、保留归档及成员定位，先汇总 attempt 再出 `run_fingerprint`（归档数、解析成功数、特征分母）。左连清洗主表，无归档特征为空，总行数与唯一 run 数应守恒。按批次保留模型分数、参数、证据键。

P03 输入须含 `run_id`（唯一）、`repo_id`、`workflow_path`、`status`、`created_at_utc` 或 `created_at`。键与创建时间禁 null。支持单文件和 Hive 嵌套分片（如 `year=2026/month=01/part-00000.parquet`）。`--cutoffs` 替换默认网格，用于回测周或指定 run 时点。须有唯一非 null 的 `cutoff_id` 和非 null 时间列（`cutoff_utc/cutoff_at/cutoff` 任选一）；仓库与路径须成对给列，每行同空或同非空。

`output_root/p03-<sha256>` 含四表与 manifest，记接口 v3、输入相对路径/字节/哈希、样本与截点参数、分步行数、11 项对账、产物行数/字节。冻结输入，样本清单显式指定或自动发现一次。发布前核分片成员与快照、样本清单有无及哈希、截点哈希；复用核状态、批次 ID、产物集合及大小。[交付路径与 Spark 2 入口](README.md#组员取用已验收的全量表)

P01 六类锚点各取最小 run ID，可重叠。单次工作流按全表计，多 attempt 需至少两个已索引 ZIP。另按键选最多 12 个有归档、含 20–150 次运行的完整工作流，去重输出 run。批次 ID 纳入输入相对路径、内容 SHA-256、模式、接口版本、样本参数。隐藏目录写完再重命名，复用核验成功状态、ID、产物列表与大小。

- **时间。** 观测耗时从创建算到最终更新，可含等待与平台重跑，执行阶段须查日志。按 UTC 创建日归周一，小时结论限 UTC。截点可见性见上表，中间 attempt 状态无法还原则记未知。
- **分母。** 跳过率 = skipped / 全部 run，失败率 = failure / (success + failure)。重试率取 completed 中尝试次数 ≥1 者，计其中 >1 的比例。零分母记 null。平台重跑次数为尝试次数减一，日志内部重试另计。间隔为相邻创建时间差，单 run 无间隔。耗时、间隔用精确线性插值分位数。
- **活动与事件。** 事件、UTC 小时占比均除以工作流全部 run。`schedule_share` 取 schedule，`dispatch_share` 取 workflow_dispatch、repository_dispatch，其余有效事件归 webhook，unknown 单列。UTC 活跃日去重，跨度含首尾；活跃日率 = 活跃日数 / 跨度，活跃日日均 = run 数 / 活跃日数，日峰值取单日最大 run 数。
- **覆盖。** 全量画像含 11,396 个工作流，课程日志覆盖其中 475 个。无归档、无目标步骤、已解析零命中分别编码。联合实验统一取两种模型均可用的 run。

<a name="p05-scoring"></a>

### P05-T01 评分与输出

- **目标与窗口。** `run_scores` 每个目标一行，全量取全部清洗 run，样本取清单目标，两者均读同批工作流历史。目标与参照须 completed、结论有值且非 skipped，最终更新有效且不早于创建；failure 可参与。按更新时间、run ID 排序，取评分时点前最近 64 个合格 run，含目标运行期间已完成者。同时完成者共用此前窗口。记 `history_start_utc/history_end_utc/n_history`；各分项至少 5 个有效历史值。
- **耗时。** d 为目标秒数与历史精确中位数 m 的绝对差，`elapsed_score = d / (d + max(1 秒, 1.4826 × MAD))`。一秒下限处理零 MAD，保留 `n_elapsed/median_seconds/mad_seconds`。
- **重试。** 平台次数须 ≥1，n 为历史有效次数个数，t 为其中次数 ≥ 目标的个数，`retry_score = (n−t)/(n+1)`，目标仅 1 次时得 0。保留 `n_attempts/tail_count`。
- **输出。** `score` 取可用分项最大值。2/1/0 项可用对应 `score_status=scored/partial/unscored`，0 项得分 null。`reason` 记排除、支持不足、缺指标、零分 within_baseline，或非零主导项（耗时方向、重试、并列）。保留目标观测值与分母。unscored 也可有有效最终更新时间，查分须看状态。阈值、误报归 P05-T03。
- **发布。** 新批次回读双向键集、行数、唯一数、时间类型，再发成功 manifest。记 P02 token、分片与样本清单摘要、脚本 SHA-256、窗口、Spark 版本、应用 ID、各节点实产行数。[复跑](README.md#p05-run-scores)

## 环境定版与复现

教师在大群公布 `root` 密码登录方式，地址见腾讯文档本组行。第 6 组已从 node1 读回正式 HDFS 批次。[登录](README.md#登录课程集群)

万明浩 11 月 22 日巡检容量，节点角色取 T02 验收配置。五机为 CentOS 7 x86_64、内核 `3.10.0-327.el7.x86_64`、Oracle JDK `1.8.0_471`、Python `2.7.5`，当时未见 Python 3、conda、NFS 工具或网络挂载。五机均有 Spark 3.5.7，计算用 node1–4 的 `/opt/spark` 2.0.0 与系统 `/usr/bin/python`。

| 节点 / IP | 验收角色 | 核数；内存总/可用（G） | 根分区 / home 可用（G） | 巡检时的进程或差异 |
|---|---|---|---|---|
| node1 / 10.176.62.226 | Spark Master、client Driver；RPC 7077、UI 8080 | 4（Xeon E5649 2.53 GHz）；7.6/2.0 | 39 / 48 | 初查无守护进程，后启动 Master；有 /opt/hadoop |
| node2 / 10.176.62.227 | Spark Worker | 4；7.6/1.1 | 16 / 34 | 原有 NameNode、SecondaryNameNode、ResourceManager |
| node3 / 10.176.62.228 | Spark Worker | 8；15/14 | 30 / 6.1 | 原有 DataNode、NodeManager；已验证 SSH |
| node4 / 10.176.62.229 | Spark Worker | 4；7.6/1.3 | 29 / 6.1 | 原有 DataNode、NodeManager |
| node5 / 10.176.62.230 | Hadoop 2.7.7 HDFS | 4；7.6/5.7 | 41 / 48 | 原有本机 Spark 2.4.8 Master/Worker 和 NameNode、SecondaryNameNode、DataNode；另装 Spark 2.0.0 |

五机根分区 50G、home 48G，node3/4 的 home 当时已用 88%。共享 `hdfs://10.176.62.230:9000`（node5 单 DataNode，容量 49.98 GB，可用 40.89 GB），实测 mkdir、put、cat、rm。默认文件系统因节点而异（node1 曾指向 node1:9000），用完整 URI，涉及默认路径时显式设 `spark.hadoop.fs.defaultFS`。清洗、聚合的 JVM 用 UTC。

| 操作 | 所在节点与命令 |
|---|---|
| Master 启动 / 停止 | node1：`/opt/spark/sbin/start-master.sh` / `/opt/spark/sbin/stop-master.sh` |
| Worker 启动 / 停止 | 各 Worker：`/opt/spark/sbin/start-slave.sh spark://10.176.62.226:7077` / `/opt/spark/sbin/stop-slave.sh` |
| 清洗、压测、画像提交 | node1；实参、HDFS 路径、驱动与执行器 UTC 配置见 [README](README.md#p02-集群清洗与分区实验) |

原片转码记输入、输出 SHA-256，含 `github_metadata_json` 的六列留在原件。P02 对保留行以结论为 null 或 trim 后为空统计 unfinished；P05 的 `not_completed` 按状态非 completed 判定。固定批次两条件命中同一批 257 行，均计入活动。

分区实验用三台 Worker、8 个执行器（各 2 核 / 1536m）。`workers_with_completed_tasks` 计执行器，物理节点按 host 去重。200→24 分区均产 10,302 行、摘要相同，30.410→21.607 秒（−28.95%），输出分区 −88%，零溢写。P03 用 24 分区、最多 12 核。[200 分区](reports/p02/p02_t05_p200_report.json) · [24 分区](reports/p02/p02_t05_p24_report.json)

P03 全量截点共 59 个周一，第一个没有可见工作流，实际输出 58 个截点、11,377 个工作流、206,141 行。747 个 run 的样本通常需 1–3 秒，17 个工作流含 5 个非 cohort；周状态 before/complete/unknown/partial 依次为 432/264/290/17。P08 复跑记录输入、代码版本、参数、时间范围和 Spark 应用 ID。[版本与实验差异](history.md#p03-cluster)

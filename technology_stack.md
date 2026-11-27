# 技术栈与数据接口

开发机用 CPython 3.12、PyArrow 25.0.1、NumPy 1.26.3，P03 另以 CPython 3.12.7 / PyArrow 16.1.0 复核。课程集群为 Spark 2.0.0 Standalone、Hadoop 2.7.7、Oracle JDK 8、Python 2.7.5。P00–P03 与 P05-T01 已验收，T01 另在本机 PySpark 4.0.1 / JDK 21 全量对账。其余见[计划](plan.md)。[依赖锁定](requirements.txt) · [第三方声明](THIRD_PARTY_NOTICES.md)

## 按流程选型

| 任务 | 工具、处理与交付 |
|---|---|
| P01 输入 | 本机 unar 解 RAR，PyArrow 分批投影，`tarfile` 按仓库列成员建 attempt 索引。样本、全量均已实测。教师脚本可用 `iter_shards`、`load_runs`（pandas）、`query`（DuckDB）抽查。 |
| P02 集群与清洗 | Linux/JVM 上用 `spark-submit`、PySpark SQL / DataFrame 分发清洗、聚合与分区写回。node1 提交兼 Master，node2–4 为 Worker，node5 提供 HDFS。按实际执行器核验多机；审计主键、状态、UTC 类型后写不可变 Snappy Parquet，按读取负载比分区，避免为每个 run 建目录。[部署](https://spark.apache.org/docs/latest/cluster-overview.html) · [Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) |
| P02-T04 覆盖 | PyArrow 批读，`tarfile` 独立枚举 TAR，与 P01 索引逐键比大小及重数。用本地 SQLite 3.53.1 将唯一 run 左连同仓库非空 ZIP，按 UTC 创建月 × 原始 event 汇总，样本/全量共用入口。[清单、异常与分母](reports/p02/p02_t04_coverage.json) |
| P03 特征 | PyArrow dataset/compute、NumPy 按仓库和精确路径排序扫描。165 片、4,112,526 个 run、11,396 个工作流、59 个 UTC 周，单进程全量约 1–2 分钟。四表、手算例与 11 项对账见下节及[实现](ghawtrace/features.py)。 |
| P03-T02 集群对账 | PySpark 2 RDD + Python 2.7 标准库从固定清洗表取 UTC 整数微秒，在三台 Worker 双跑 106 列画像，各输出 24 个 gzip 分区及节点记录。PyArrow 按键核对类型、空值、数值，绝对与相对容差均 1e-12。[复跑报告](reports/p03/p03_t02_repeat.json) |
| P04 聚类（计划） | Spark 2 的 VectorAssembler、StandardScaler、KMeans 拟合多组 K 和种子，按原始单位解释质心与代表样本。Python 3 用 scikit-learn 在固定样本记录距离、缩放、轮廓系数、簇规模与跨种子 ARI。Spark ClusteringEvaluator 始于 2.3.0。[API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.evaluation.ClusteringEvaluator.html) · [聚类](https://spark.apache.org/docs/latest/ml-clustering.html) |
| P05-T01 运行评分 | PySpark 按工作流重分区并排序，Python 有界窗口计算精确中位数、MAD 和重试尾部。课程 Spark 2 与本机 Spark 4 各双跑，跨版本逐键一致，Python 独立复算 751 个目标。[接口](#p05-scoring) |
| P05-T02/T03 生命周期与回测（计划） | Spark SQL 周面板、持续性规则和时间回放。每个工作流可有多个沉寂、恢复或突发区间，保留起止周与既往节律。用已知变化点测延迟，稳定序列估误报，标出末端截尾。 |
| P06 日志指纹（计划） | 以仓库 TAR 分工，Python `tarfile/zipfile/io/re` 逐 ZIP、逐成员读，避免 driver 整批解压或收集全文。Parquet 记录规则命中、有效行数、定位与解析状态，从 attempt 聚合到 run。 |
| P07 联合评分（计划） | 在协调节点用 scikit-learn IsolationForest、pandas、DuckDB 处理约三万条有日志 run。同样本对比结构化与联合模型，将 `score_samples` 转为高分表异常。记录切分、种子、缺失率、排名和 Top-K 重合；移除相关日志特征组重训，回查原始证据。[接口](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html) |
| P08/P09 查询演示与协作（计划） | DuckDB 查汇总表与索引，Streamlit/Plotly.py 按工作流、run、时间展示原型、时间线、排名、证据及无日志状态。Git 管理源码和文档。[权限](AGENTS.md) |

## 课程资料与查阅路径

按原文件名和幻灯片页码查找课程群或组内共享课件，开发时对照实际发行版。

| 对应任务 | 课程资料 | 项目中的用法与对照入口 |
|---|---|---|
| P02 集群与存储 | `week1.2.Hadoop概述.pptx` 第 17 页，`week2.1.HDFS.pptx` 第 19–25、44–50、51–54 页，`week6.1.spark-general.pptx` 第 27–30 页 | 根据资源管理器、存储和执行器的职责划分节点。使用 HDFS，核验输入与产物对各节点可见。按仓库保留日志 TAR，避免上传大量单条日志文件。[Spark 集群部署](https://spark.apache.org/docs/latest/cluster-overview.html) |
| P02-T05、P03、P05-T01 分区与窗口 | `week2.2.Mapreduce.pptx` 第 16、20–29 页，`week6.2.spark原理.pptx` 第 14–21、26–27 页 | 用按键重分区、宽依赖、Shuffle 和 Stage 理解聚合与窗口的成本。在同一全量作业比较两种分区设置，记录执行器、Shuffle 和 Stage 耗时。历史窗口依据实际可见时间构造。[Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html) · [Spark SQL 窗口函数](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html) |
| P04 画像聚类 | `week3.2.Kmeans.pptx` 第 3–8 页，`week4.1.机器学习.pptx` 第 50–56 页，`week4.2.聚类.pptx` 第 4–10 页 | 参照“分配到最近中心、重算质心”的迭代关系解释 Spark MLlib 结果。课件对距离和初始中心的讨论用于设计量纲、K 和随机种子的对照实验。[Spark MLlib 聚类](https://spark.apache.org/docs/latest/ml-clustering.html) |
| P06–P07 日志文本特征备选 | `week3.1.tfidf.pptx` 第 2–7 页 | 课件区分单篇词频与全语料文档频率。若规则指纹遗漏较多可重复的模式，可把归一化日志模板作为词、run 作为文档，试验只在训练集合拟合的 TF-IDF；现有规则指纹继续提供可回查证据。[Spark MLlib TF-IDF](https://spark.apache.org/docs/latest/ml-features.html#tf-idf) |

## 表的粒度与分析口径

P01 从 19 个源字段投影 13 列，保留 UTC 字符串、状态与空值。P02 从原片转码同 13 列，两路全量输入已在 P03 交叉核对。源字段 `log_archive_count`、`log_file_count`、`empty_archive_count` 与课程包实物分开统计。P01 索引定位 ZIP，P02-T04 查 TAR 成员，P06 才解析 ZIP 内容和目标步骤。[输出与命令](README.md#开发者第一次运行)

表键非空，路径保留原文与大小写。run、仓库数值 ID 为 int64，P01 的 `attempt` 为从 1 起的 int32，P05 的 `run_attempts` 为可空 long，`cutoff_id` 为字符串。统一 UTC、周一日期。P02/P05 在 Spark 中读为 timestamp；P03 的五个 Parquet UTC 微秒时间字段在 Spark 2 默认读为 bigint，用 [read_features](ghawtrace/p03/read_features.py) 恢复 timestamp。

| 粒度与产物 | 唯一键 | 最小字段、单位与空值 |
|---|---|---|
| run · `runs_clean` | `run_id` | 仓库、精确路径、可空 workflow ID。按 `created_at_utc` 归周，缺失或不可解析的创建时间隔离。保留可空的 `updated_at_utc/event/status/conclusion/run_attempts`，`elapsed_seconds` 为创建到更新的浮点秒，更新缺失或倒序记空并计质量指标。 |
| 工作流 · `workflow_profile` | `repo_id, workflow_path` | 106 列的[字段名与类型](ghawtrace/features.py)。身份取最小非负 workflow ID，记变体数及冲突。活动含 run 数、首末时间/周、跨度、活跃日数/率、活跃日日均与日峰值。结论有六类及 other/unknown、跳过率、失败率、开放 run 数。completed 的重试含已知数、总数、均值、最大值、重试数/率；耗时含已知/缺失/负/零计数，及有效值均值、中位数、P90、P99、最大秒数。进行中、排队计活动、开放与耗时缺失。间隔含个数、均值、最小、中位、P90、最大秒数。时相含 24 小时占比、峰时及占比、熵、UTC 0–5 时占比；事件含 18 类及 other/unknown 占比、schedule/dispatch/webhook 占比、首位事件及占比、种类数和熵。比例 0–1，失败、重试、耗时、间隔各有 estimable 标志。 |
| 事件 · `workflow_event_mix` | `repo_id, workflow_path, event` | 原始事件（缺失记 unknown）的 `run_count` 与 `run_share`，可回查画像比例。 |
| 周 · `workflow_week` | `repo_id, workflow_path, week_start_utc` | 全输入首末 UTC 周的连续网格（11,396 × 59），每周为周一起半开七天。完整工作流首次出现前为 `before_first_observation`（计数 null），此后采集首末周为 `partial`，其余为 `complete`（无 run 记 0）。非 cohort 锚点有观测周为 `partial`，包括首次出现前在内的未观测周为 `unknown`（计数 null）。字段含 run 数、活跃日数、八类结论计数、completed 重试数及有效耗时中位数、P90。 |
| 截点 · `workflow_asof` | `repo_id, workflow_path, cutoff_id` | 窗口从当时最早可见 run 到 cutoff，天数为 `prior_window_days`。`n_prior_runs` 计截点前创建的 run；活跃日、日频率、间隔仅依创建时间。结论须 completed、非空、最终更新严格早于 cutoff，所得 `n_prior_outcomes` 作跳过率分母，失败率用可见 success + failure。重试率分母 `n_prior_known_attempts` 为可见 completed 中已知有效次数，耗时仅用可见 completed 的有效值。全局截点只出已出现工作流；显式指定已知键可在首个 run 前出冷启动行。不可估计记 null，以 `has_prior_runs/has_prior_outcomes` 标记。 |
| attempt · `attempt_index`（现有）、`attempt_fingerprint`（P06 约定） | `run_id, attempt` | 索引含 `repo_id`、相对输入根的 `tar_path`、`tar_member`、`archive_bytes`。指纹将记 `parse_status`（parsed/no_target_member/failed）、`eligible_lines`、信号计数。已解析零命中记 0，未解析特征为 null。 |
| 证据行 · `log_evidence`（P06 约定） | `run_id, attempt, member, line_no` | ZIP 成员原名，解码后从 1 起的原始行号，过滤保留原定位。一行多命中共用标签列表，展示脱敏摘录。 |

run 按工作流键连画像、加 UTC 周键连周表，截点另加 `cutoff_id`。P06/P07 约定按 run/attempt 连证据、保留归档及成员定位，先汇总 attempt 再出 `run_fingerprint`（归档数、解析成功数、特征分母）。左连清洗主表，无归档特征为空，总行数与唯一 run 数应守恒。按批次保留模型分数、参数、证据键。

P03 在 `output_root/p03-<sha256>` 发布四表与 manifest，记输入相对路径、字节、哈希，接口版本 3、样本和截点参数、分步行数、11 项对账及产物行数/字节数。入口冻结分片，可读单文件或嵌套 Hive 分区（如 `year=2026/month=01/part-00000.parquet`），缺 `status` 即报错。`--cutoffs` 表覆盖默认网格，必含 `cutoff_id/cutoff_utc`；时间列也接受 `cutoff_at` 或 `cutoff`，截点 ID 必须唯一；可附仓库与路径，每行两者同空或同非空，用于回测周或目标 run 时点。样本清单可显式传入或在输入目录发现一次，记录有无及哈希；发布前核对分片成员与文件快照、样本清单有无及哈希、截点文件哈希。复用校验状态、批次 ID、manifest 产物集合及各文件大小。[交付路径与 Spark 2 入口](README.md#组员取用已验收的全量表)

P01 六类锚点各取最小 run ID，可重叠。单次工作流按全表计，多 attempt 需至少两个已索引 ZIP。另按键选最多 12 个有归档、含 20–150 次运行的完整工作流，去重输出 run。批次 ID 纳入输入相对路径、内容 SHA-256、模式、接口版本、样本参数。隐藏目录写完再重命名，复用核验成功状态、ID、产物列表与大小。

- **时间与可见性。** 观测耗时是从创建到最后更新的秒数，可含等待和平台重跑；具体执行阶段须用日志核验。周键为创建时间所在 UTC 周的周一，小时结论限于 UTC。回测依照上表的截点和最终更新时间限制可见性，中间 attempt 状态无法还原时记未知。
- **分母与分位数。** 跳过率 = skipped / 全部 run，失败率 = failure / (success + failure)，重试率 = 已知尝试次数 ≥ 1 的 completed run 中尝试次数 > 1 的比例；零分母记 null。平台重跑次数为尝试次数减一，日志内部重试另计。相邻创建时间差为间隔；单 run 没有间隔。耗时与间隔使用精确线性插值分位数。
- **活动与事件。** 事件及 UTC 小时比例均除以该工作流全部 run。`schedule_share` 取 schedule，`dispatch_share` 取 workflow_dispatch 和 repository_dispatch，其余有效事件计入 webhook，unknown 单列。UTC 活跃日去重，跨日跨度含首尾；活跃日率、日均运行数分别为活跃日数 / 跨度、run 数 / 活跃日数。峰值日取单日最大 run 数。
- **覆盖。** 全量画像含 11,396 个工作流，课程日志覆盖其中 475 个。无归档、无目标步骤、已解析零命中分别编码。联合实验统一取两种模型均可用的 run。

<a name="p05-scoring"></a>

### P05-T01 评分与输出

- **目标与窗口。** `run_scores` 对清洗表每个 run 留一行。评分及参照须 completed、结论有值且非 skipped、最终更新有效并不早于创建，failure 可参与。按工作流取严格早于评分时点的最近 64 个合格 run，依更新时间、run ID 排序，含目标运行期间已完成的其他 run。同时完成者共用先前窗口，保留 `history_start_utc/history_end_utc/n_history`。每项至少 5 个有效历史值。
- **耗时。** d 为目标秒数与历史精确中位数 m 的绝对差，`elapsed_score = d / (d + max(1 秒, 1.4826 × MAD))`。一秒下限处理零 MAD，保留 `n_elapsed/median_seconds/mad_seconds`。
- **重试。** 平台次数须 ≥1，n 为历史有效次数个数，t 为其中次数 ≥ 目标的个数，`retry_score = (n−t)/(n+1)`，目标仅 1 次时得 0。保留 `n_attempts/tail_count`。
- **输出。** `score` 取可用分项最大值；两项、一项、零项可用分别记 `score_status=scored/partial/unscored`，零项时得分 null。`reason` 解释排除、支持不足、缺指标、零分 within_baseline，或非零主导项的耗时方向、重试、并列。保留目标观测值、分母。有效最终更新有时也属于 unscored 行，查可用分数须看状态；阈值与误报留 P05-T03。
- **发布。** 每次写新批次，读回核验双向键集、总数、唯一数、时间类型，再发布成功 manifest。记录 P02 token、分片摘要、脚本 SHA-256、窗口、样本清单摘要、Spark 版本、应用 ID、节点实际行数。样本与全量取同一清洗批次历史。[复跑](README.md#p05-run-scores)

## 环境定版与复现

教师在课程大群公布 `root` 密码登录方式，各组地址见腾讯文档本组行。第 6 组已实登 node1，读回正式 HDFS 批次。[登录](README.md#登录课程集群)

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

原片 ZSTD 转 13 列 Parquet 1.0/Snappy，记录原片及输出 SHA-256，含 `github_metadata_json` 的六列留在原件。按固定批次发布全量清洗，257 个未完成 run 计入活动。固定聚合用三台 Worker、8 个执行器（各 2 核 / 1536m）。报告的 `workers_with_completed_tasks` 计执行器，物理节点按 host 去重。200→24 分区，均输出 10,302 行且摘要相同，30.410→21.607 秒（−28.95%），输出分区 −88%，均零溢写。P03 取 24 分区、最多 12 核。[200 分区](reports/p02/p02_t05_p200_report.json) · [24 分区](reports/p02/p02_t05_p24_report.json)

P03 全量截点共 59 个周一，第一个没有可见工作流，实际输出 58 个截点、11,377 个工作流、206,141 行。747 个 run 的样本通常需 1–3 秒，17 个工作流含 5 个非 cohort；周状态 before/complete/unknown/partial 依次为 432/264/290/17。P08 复跑记录输入、代码版本、参数、时间范围和 Spark 应用 ID。[版本与实验差异](history.md#p03-cluster)

# Agent 运行行为画像与异常发现

大数据分析技术期末项目 · 第 6 组 · 选题 5

分析 GitHub Agent 工作流的运行画像、单次异常与长期节律，以执行日志追查运行指标遗漏的问题。最终在课程集群批处理，演示时可回溯工作流、run 与日志行。

## 当前可用内容

P00–P03、P05-T01 已验收。P01 提供共用样本，P02 完成五节点盘点、清洗、分区实验和日志覆盖审计。P03 在三台 Worker 双跑，11,396 个工作流的 106 列画像与独立实现一致，含 59 个 UTC 周的四表已发布至 HDFS。P05-T01 在同一批次给 4,112,526 个 run 各留一行，两次集群全量与本机 Spark 4 逐键一致。[运行评分](#p05-run-scores)

原件从课程群或组内共享目录按文件名获取。Git 保存代码和文档，本地或共享存储保存原件与数据批次。

| 材料 | 内容 |
|---|---|
| `大数据分析技术项目-2026版.pptx` | 第 4 页是选题，第 5–6 页是集群、汇报与提交要求 |
| `GHAWTrace_ref/runs.rar` | 165 个 Parquet 2.6 分片（19 列，ZSTD 压缩），4,112,526 个唯一 run，覆盖 2,969 个仓库和 11,396 个有记录的工作流 |
| `GHAWTrace_ref/logs.rar` | 92 个仓库 TAR，内含 30,427 个非空 attempt ZIP，对应 30,393 个 run 和 475 个工作流，均可关联运行表 |
| `GHAWTrace_ref/2611.06866v1.pdf` | 采集、日志留存与案例。25,014 个核验工作流中有 13,618 个没有观测到运行 |
| `GHAWTrace_ref/read_runs.py` | 教师提供的整表、逐分片及 DuckDB SQL 示例，默认使用 Windows 路径 |

## 开发者第一次运行

可直接取用共享批次。自行生成需 Python 3.12（实测 3.12.14）和 `unar`。已有 `runs/*.parquet`、`logs/*.tar` 时，把 `P01_INPUT` 设为其父目录，跳过解包。

```sh
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt

P01_INPUT="$PWD/.local/ghawtrace"
mkdir -p "$P01_INPUT"
unar -q -D -o "$P01_INPUT" GHAWTrace_ref/runs.rar
unar -q -D -o "$P01_INPUT" GHAWTrace_ref/logs.rar

mkdir -p .local
python -m ghawtrace.pipeline --input-root "$P01_INPUT" --output-root .local/p01 --mode sample > .local/p01-result.json
BATCH=$(python -c 'import json; print(json.load(open(".local/p01-result.json"))["batch_dir"])')
python -m unittest discover -s tests -v
```

- `sample`、`full` 均扫描原始分片和 TAR，返回 `batch_dir`、`reused`。成功目录含 `manifest.json`、原始投影 `runs_input.parquet`、按 `tar_path/tar_member` 定位 ZIP 的 `attempt_index.parquet`，相同完整批次可复用。
- 样本含 747 个 run 和 294 条 attempt 索引，另附 `sample_manifest.json` 列出 run ID、六类锚点与 12 个完整 cohort。周序列或工作流统计用完整 cohort，合成边界例见测试。[批次约定](technology_stack.md#表的粒度与分析口径)
- 上述测试在缺少 PySpark 时跳过 Spark 回归。本机完整回归另用 JDK 21 和 `pip install pyspark==4.0.1`，在该环境中运行下列命令。

```sh
TZ=UTC SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON="$(command -v python)" \
  PYSPARK_SUBMIT_ARGS='--py-files ghawtrace/p02/clean_runs.py --conf spark.ui.enabled=false --conf spark.sql.ansi.enabled=false --conf spark.sql.shuffle.partitions=4 --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC --conf spark.executor.extraJavaOptions=-Duser.timezone=UTC pyspark-shell' \
  python -m unittest discover -s tests -v
```

### 登录课程集群

在课程网络运行 `ssh root@10.176.62.226`，按提示输入教师在课程大群公布的口令。登录后 `hostname` 应为 `node1`。其余地址见腾讯文档第 6 组行，连接失败按教师通知反馈节点和报错。[五台节点分工](technology_stack.md#环境定版与复现)

### P02 集群清洗与分区实验

组内 HDFS 已有全量转码输入。自行转码用 `python ghawtrace/p02/convert_for_spark2.py "$P01_INPUT/runs" .local/runs_snappy`（Python 3，13 列，Parquet 1.0/Snappy）。输入输出目录须互不包含。末尾可加正整数限定前几片，`--clean-output` 先清空输出。

在 node1 仓库根目录用 Bash 运行。每次使用独立目录，按 36 分区清洗，再用同一不可变批次比较 24 和 200 分区。

```bash
NN=hdfs://10.176.62.230:9000
DATA="$NN/data/ghawtrace"
WORK=$(mktemp -d)
OUT="$DATA/p02_$(basename "$WORK")"
P02=ghawtrace/p02
submit() {
  /opt/spark/bin/spark-submit --master spark://10.176.62.226:7077 --deploy-mode client \
    --executor-cores 2 --executor-memory 1536m --conf "spark.hadoop.fs.defaultFS=$NN" \
    --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
    --conf spark.executor.extraJavaOptions=-Duser.timezone=UTC "$@"
}
submit --conf spark.sql.shuffle.partitions=36 "$P02/clean_runs.py" \
  "$DATA/runs_snappy" "$OUT/clean" "$WORK/clean.json"
TOKEN=$(python -c 'import json,sys; print(json.load(open(sys.argv[1]))["batch_token"])' "$WORK/clean.json")
for n in 24 200; do
  submit --conf "spark.sql.shuffle.partitions=$n" "$P02/partition_bench.py" \
    "partition-p$n" "$OUT/clean/batches/$TOKEN/runs_clean" "$OUT/p$n" "$WORK/p$n.json"
done
```

烟雾作业读无表头 TSV（组键、整数），写每组行数与和。以下三个合成边界脚本在 node1 均应输出 `ALL PASS`。

```bash
submit "$P02/p02_t02_smoke.py" "$NN/tmp/p02_t02_in/sample.tsv" "$OUT/smoke"
for test in verify_nullable_fields inject_failure verify_time_precision; do
  /opt/spark/bin/spark-submit --master 'local[1]' --py-files "$P02/clean_runs.py" \
    --conf spark.pyspark.python=python --conf spark.pyspark.driver.python=python \
    --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
    --conf spark.executor.extraJavaOptions=-Duser.timezone=UTC "$P02/$test.py"
done
```

[边界案例与批次差异](history.md#p02-revisions) · [集群启停](technology_stack.md#环境定版与复现)

### P02 日志覆盖审计
Python 3 的 `BATCH` 与 `P01_INPUT` 沿用 P01 成功批次和解包根目录，两种模式通用。

```sh
python -m ghawtrace.p02.coverage --batch "$BATCH" --tar-dir "$P01_INPUT/logs" --output .local/coverage.json
```

全量报告按 TAR 列 ZIP、唯一 run、工作流数，按 UTC 创建月 × 原始 event 列覆盖分子与分母。每个 run 仅计一次覆盖。P06 以此核对解析范围。[覆盖报告](reports/p02/p02_t04_coverage.json)

### P03 工作流画像与周特征

Python 3 入口接受 P01 成功批次（优先读 `runs_input.parquet`）、P02 `runs_clean`、单个 Parquet 或嵌套 Hive 分区目录。输入须含 `status`。

```sh
python -m ghawtrace.feature_pipeline --runs-input "$BATCH" --output-root .local/p03
```

输出画像、事件构成、连续周面板、历史截点四表及含输入摘要与 11 项对账的 manifest，相同输入可复用。样本约 1–3 秒，全量约 1–2 分钟。自动发现样本清单，也可传 `--sample-manifest`；完整 cohort 能识别零运行周，其余工作流未观测周为 `unknown`。`--cutoffs my_cutoffs.parquet` 指定截点，`--cutoff-grid none` 关闭默认网格并保留空表 schema。[字段与发布](technology_stack.md#表的粒度与分析口径) · [手算用例](tests/test_p03.py)

### 组员取用已验收的全量表

在 node1 读取同批四表与成功 manifest。P04 取 `workflow_profile`，P05-T02/T03 可取 `workflow_week`、`workflow_asof`，P07 使用截点特征；`workflow_event_mix` 保留事件明细。P05-T01 直接读 P02 清洗批次。P06 的覆盖报告在同一共享根目录。下例按 Spark 2 的 UTC 时间类型读回并打印行数与范围。

```sh
HDFS=hdfs://10.176.62.230:9000/data/ghawtrace
SHARED="$HDFS/group06_accept_xrxGyscu"
FEATURES="$SHARED/p03-76f186f28bf8fb2208a4b1af2faab4df8011073df778eab7bd3f995dee9063b2"
/opt/spark/bin/spark-submit --master 'local[1]' --py-files ghawtrace/p02/clean_runs.py \
  --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
  --conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
  ghawtrace/p03/read_features.py "$FEATURES"
```

下游作业沿用 UTC 配置，加 `--py-files ghawtrace/p03/read_features.py,ghawtrace/p02/clean_runs.py`，执行 `from read_features import read_features`、`df = read_features(spark, batch_uri, "workflow_profile")`。Python 3 / PyArrow 可直接读下载后的 Parquet。

### 复跑 P03-T02 集群验收

在 node1 检出目录使用新 HDFS 路径。作业按工作流分组，各 Worker 的 gzip JSON 行保留分区、节点与应用 ID。

```sh
HDFS=hdfs://10.176.62.230:9000/data/ghawtrace
CLEAN_BATCH="$HDFS/p02_t03_run1/batches/app-20261125190005-0027_8644c332"
WORK=$(mktemp -d)
SPARK_OUT="$HDFS/p03_$(basename "$WORK")"
/opt/spark/bin/spark-submit --master spark://10.176.62.226:7077 --deploy-mode client \
  --executor-cores 2 --executor-memory 1536m --conf spark.cores.max=12 \
  --conf spark.sql.shuffle.partitions=24 --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
  --conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
  ghawtrace/p03/spark_profile.py "$CLEAN_BATCH" "$SPARK_OUT"
CLEAN="$WORK/${CLEAN_BATCH##*/}"
hdfs dfs -get "$CLEAN_BATCH" "$CLEAN"
hdfs dfs -get "$SPARK_OUT" "$WORK/spark-out"
(cd "$CLEAN/runs_clean" && sha256sum *.parquet) > "$WORK/clean.sha256"
echo "$WORK"
```

复制整个 `WORK` 到 Python 3 开发机，更新变量后在仓库根目录重建四表，核验输入摘要、多机证据及画像逐键数值与空值，预期 `mismatches: 0`。

```sh
CLEAN="$WORK/app-20261125190005-0027_8644c332"
python -m ghawtrace.feature_pipeline --runs-input "$CLEAN/runs_clean" --output-root "$WORK/reference"
python -m ghawtrace.p03.verify_cluster --reference "$WORK"/reference/p03-* \
  --spark-output "$WORK/spark-out" --p02-marker "$CLEAN/_SUCCESS" \
  --clean-sha256 "$WORK/clean.sha256" --report "$WORK/acceptance.json"
```

<a name="p05-run-scores"></a>

### P05-T01 单次运行评分

输入为 P02 不可变批次（JSON `_SUCCESS` 和 `runs_clean/`），程序校验成功标记并逐片计算 SHA-256。样本只筛目标 run，仍取全量同工作流历史。使用 Python 3.12、项目依赖、PySpark 4.0.1、JDK 21，在仓库根目录运行 Bash。`WORK` 指上节取回的固定批次材料，`BATCH` 指 P01 的 **sample** 批次。已有完整输入可直接设置 `CLEAN`、`SAMPLE`，也可只从 HDFS 取回上述 `CLEAN_BATCH` 目录用于评分。

```bash
CLEAN="$WORK/app-20261125190005-0027_8644c332"
SAMPLE="$BATCH/sample_manifest.json"

OUT=$(mktemp -d "$PWD/.local/p05-rerun.XXXXXXXX")
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$(command -v python)"
score_runs() {
  spark-submit --master 'local[4]' --driver-memory 3g \
    --conf spark.sql.shuffle.partitions=24 \
    --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
    --conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
    ghawtrace/p05/score_runs.py "$CLEAN" "$@"
}
score_runs "$OUT/sample" --sample-manifest "$SAMPLE"
score_runs "$OUT/full"
score_runs "$OUT/repeat" --partitions 17
python -m ghawtrace.p05.verify_scores --clean-batch "$CLEAN" --sample-manifest "$SAMPLE" \
  --sample-batch "$OUT"/sample/p05-* --full-batch "$OUT"/full/p05-* \
  --repeat-batch "$OUT"/repeat/p05-* > "$OUT/acceptance.json"
```

作业返回成功批次路径。P07/P08 校验 `manifest.json` 为 `success`，用 `spark.read.parquet(batch + "/run_scores")` 按 `run_id` 连接。全量 4,112,526 个唯一 run，样本 747 个。验收比较两个全量作业的全部业务列，并从清洗表独立复算样本与具名案例。[评分口径](technology_stack.md#p05-scoring) · [本机验收](reports/p05/p05_t01_acceptance.json)

**组内取表。** node1 的 PySpark 可直接读取已发布全量表。

```python
batch = "hdfs://10.176.62.230:9000/data/ghawtrace/group06-p05-FIx6sUOQ/full/p05-app-20261127011053-0038-9a9bf37e4593"
scores = spark.read.parquet(batch + "/run_scores")
print(scores.count(), scores.select("run_id").distinct().count())  # 均为 4112526
```

**课程集群复跑。** 在 node1 沿用 `submit`、`DATA`、`CLEAN_BATCH`，将 `SAMPLE` 设为已复制到本机的 P01 样本清单。三次作业各返回成功批次的 HDFS 地址。

```bash
WORK=$(mktemp -d)
OUT="$DATA/p05_$(basename "$WORK")"
score() {
  submit --conf spark.cores.max=12 --conf spark.sql.shuffle.partitions=24 \
    ghawtrace/p05/score_runs.py "$CLEAN_BATCH" "$@"
}
score "$OUT/sample" --sample-manifest "$SAMPLE"
score "$OUT/full"
score "$OUT/repeat" --partitions 17
```

用 `hdfs dfs -get` 分别取回三个完整批次，在 Python 3 开发机将 `verify_scores` 的三个批次参数指向导出目录，加 `--compare-local-full 本机全量批次目录`。程序对齐跨版本业务字段，并从 Parquet 统计各 Worker 下每种评分状态和原因的行数，与作业清单逐组核对。[集群验收](reports/p05/p05_t01_cluster_acceptance.json) · [HDFS 独立读回](reports/p05/p05_t01_hdfs_readback.json)

### 教师脚本双分片抽查（可选）

macOS/Linux 需 `pyarrow`、`pandas`、`unar`、教师脚本和 `runs.rar`。

```sh
export PJ_SAMPLE_DIR="$(mktemp -d)"
unar -q -D -o "$PJ_SAMPLE_DIR" GHAWTrace_ref/runs.rar \
  runs/part-00000.parquet runs/part-00001.parquet
python3 - <<'PY'
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path("GHAWTrace_ref").resolve()))
import read_runs

read_runs.RUNS_DIR = str(Path(os.environ["PJ_SAMPLE_DIR"]) / "runs")
columns = ["run_id", "event", "conclusion", "created_at", "run_attempts"]
shard_sizes = [len(frame) for frame in read_runs.iter_shards(columns)]
assert shard_sizes == [25_000, 25_000], shard_sizes
print(f"{len(shard_sizes)} 个分片，共 {sum(shard_sizes):,} 行")
PY
```

预期 `2 个分片，共 50,000 行`。全量抽查将 `RUNS_DIR` 指向 165 片目录，安装 `duckdb`，`read_runs.query("SELECT count(*) AS n FROM runs")` 应得 4,112,526。

## 系统设计与开发顺序

万明浩交付集群、清洗与分区实验，黄志成主责画像，刘陈昱完成日志覆盖审计、协作画像集群复算并交付 P05-T01。接下来季雨昊做 P04 聚类，龙启波做 P05 生命周期检测与回测，张锦稷做 P06 日志指纹与定位。P07 比较结构化与联合评分，P08 集成查询演示。run 创建范围为 2025-08-14 至 2026-09-21（UTC），按最终更新计的评分时点延至 2026-10-17。[分工与验收](plan.md) · [课程索引](technology_stack.md#课程资料与查阅路径) · [Git 规则](AGENTS.md)

## 课程交付

总评中平时成绩占 20%，课程 PJ 占 80%（`week1.1.大数据分析技术.pptx` 第 36 页）。每组最多六人，在集群搭建实验环境，报告与系统演示合计 20–30 分钟。PPT 说明完成内容、过程、数据集、经验和分工，每人展示自己的技术实现与完成证据。提交**源代码及使用说明、PPT、项目文档**，截止时间与入口以教师通知为准。

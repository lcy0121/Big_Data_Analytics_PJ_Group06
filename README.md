# Agent 运行行为画像与异常发现

大数据分析技术期末项目 · 第 6 组 · 选题 5

本项目从 GitHub Agent 工作流提取行为画像，识别单次运行与长期节律的异常，并用执行日志寻找仅凭运行指标难以发现的问题。最终在课程集群批处理，通过演示追溯到工作流、run 和日志行。

## 当前可用内容

P01 提供可重跑的输入准备和共用样本。P02 已完成五节点盘点、Spark 分布式清洗、分区实验和全量日志覆盖审计。P03 已用正式清洗表验收 4,112,526 个 run、11,396 个工作流与 59 个 UTC 周。三台 Worker 两次全量聚合均与独立画像的 106 列逐项一致，四张交付表已在组内 HDFS 发布。P05-T01 已在本机 PySpark 对同一清洗批次完成全量运行评分，两次复跑逐键一致。[运行评分入口](#p05-run-scores)

课程资料可从课程群或组内共享目录按以下文件名获取。Git 记录项目代码和文档，原件与生成批次保存在本地或共享存储。

| 材料 | 内容 |
|---|---|
| `大数据分析技术项目-2026版.pptx` | 第 4 页是选题，第 5–6 页是集群、汇报与提交要求 |
| `GHAWTrace_ref/runs.rar` | 165 个 Parquet 分片，4,112,526 个唯一 run，覆盖 2,969 个仓库和 11,396 个有记录的工作流 |
| `GHAWTrace_ref/logs.rar` | 92 个仓库 TAR，内含 30,427 个非空 attempt ZIP，对应 30,393 个 run 和 475 个工作流，均可关联运行表 |
| `GHAWTrace_ref/2611.06866v1.pdf` | 采集、日志留存与案例。25,014 个核验工作流中有 13,618 个没有观测到运行 |
| `GHAWTrace_ref/read_runs.py` | 教师提供的整表、逐分片及 DuckDB SQL 示例，默认使用 Windows 路径 |

## 开发者第一次运行

组员可直接使用共享的已完成批次。自行生成时使用 Python 3.12（实测 3.12.14）、仓库锁定的依赖和 `unar`。若已有 `runs/*.parquet` 与 `logs/*.tar`，将 `P01_INPUT` 指向该目录并跳过解包。

```sh
python3 -m venv .venv
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

- 两种模式均需读取课程原始分片和 TAR。将 `--mode sample` 改为 `--mode full` 可准备全量输入。命令返回 `batch_dir` 与 `reused`。已完成批次的目录为非隐藏目录，包含两张 Parquet 表和状态为 `success` 的 `manifest.json`。
- 课程样本有 **747 个 run 与 294 个 attempt 索引**。`sample_manifest.json` 列出 run ID、六类样例和 `cohorts` 中的 12 个完整工作流。做周序列或工作流统计时用这 12 个工作流。合成边界案例在测试中单独生成。
- `runs_input.parquet` 保留原始列投影，`attempt_index.parquet` 通过 `tar_path` 和 `tar_member` 定位源 TAR 与 ZIP。同一批次满足完成条件时会复用。[数据与批次约定](technology_stack.md#表的粒度与分析口径)

### P02 集群清洗与分区实验

Python 3 开发机可用 `python ghawtrace/p02/convert_for_spark2.py "$P01_INPUT/runs" .local/runs_snappy` 将 13 个项目字段转为 Parquet 1.0 / Snappy。输出目录与输入须互不包含。可在末尾加正整数，仅转换排序靠前的分片；显式加 `--clean-output` 会先清空输出目录。组内 HDFS 已有全量转码输入。

在 node1 的仓库根目录用 Bash 执行。下例给每次复跑分配独立目录，使用与已验收清洗一致的 36 个分区；对比分区实验使用同一份固定清洗表。

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

烟雾验证读取两列无表头 TSV（组键、整数），写回每组的行数与和。沿用上面的变量，在 node1 运行下列命令。边界脚本各自生成合成数据；nullable、故障注入和微秒精度三个脚本均应输出 `ALL PASS`。

```bash
submit "$P02/p02_t02_smoke.py" "$NN/tmp/p02_t02_in/sample.tsv" "$OUT/smoke"
for test in verify_nullable_fields inject_failure verify_time_precision; do
  /opt/spark/bin/spark-submit --master 'local[1]' --py-files "$P02/clean_runs.py" \
    --conf spark.pyspark.python=python --conf spark.pyspark.driver.python=python \
    --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
    --conf spark.executor.extraJavaOptions=-Duser.timezone=UTC "$P02/$test.py"
done
```

脚本位置、合成边界、首轮与正式批次的数据差异见[开发历史](history.md#p02-revisions)。启动和停止服务见[集群环境](technology_stack.md#环境定版与复现)。

### P02 日志覆盖审计
在 Python 3 环境执行。将 `BATCH` 指向上方 P01 命令返回的成功目录，`P01_INPUT` 指向解包目录。sample 与 full 使用同一入口。

```sh
python -m ghawtrace.p02.coverage --batch "$BATCH" --tar-dir "$P01_INPUT/logs" --output .local/coverage.json
```

全量报告逐 TAR 列出 ZIP、唯一 run 和覆盖工作流，并按 UTC 创建月及原始 event 列出分子、分母和覆盖率。多次 attempt 只给一个 run 贡献一次覆盖。P06 可据此核对实际解析的范围。[全量报告](reports/p02/p02_t04_coverage.json)

### P03 工作流画像与周特征

在 Python 3 环境使用上方得到的 `BATCH`。也可输入 P02 的 `runs_clean`、单个 Parquet 或嵌套 Hive 分区目录。P01 批次优先读取 `runs_input.parquet`；输入须含 `status`。

```sh
python -m ghawtrace.feature_pipeline --runs-input "$BATCH" --output-root .local/p03
```

批次含画像、事件构成、周面板、历史截点四张 Parquet 和记录输入摘要及 11 项对账的 manifest；同一输入可复用。样本约 1–3 秒，全量约 1–2 分钟。样本清单自动发现，也可用 `--sample-manifest` 指定。完整 cohort 可以估计零运行周，部分观测工作流的未观测周为 `unknown`。使用 `--cutoffs my_cutoffs.parquet` 指定截点，或用 `--cutoff-grid none` 停用默认周截点（仍产生有 schema 的空截点表）。字段、分母、冻结与发布条件见[数据口径](technology_stack.md#表的粒度与分析口径)，手算例见[特征测试](tests/test_p03.py)。

### 组员取用已验收的全量表

在 node1 的检出目录执行。四张 Parquet 和成功 manifest 共用一个批次，P04 取 `workflow_profile`，P05 取 `workflow_week` 和 `workflow_asof`，事件明细在 `workflow_event_mix`。P06 的 `p02_t04_coverage.json` 位于同一共享根目录。Spark 2 读取时间列时使用下面的入口，程序将打印行数和 UTC 时间范围。

```sh
HDFS=hdfs://10.176.62.230:9000/data/ghawtrace
SHARED="$HDFS/group06_accept_xrxGyscu"
FEATURES="$SHARED/p03-76f186f28bf8fb2208a4b1af2faab4df8011073df778eab7bd3f995dee9063b2"
/opt/spark/bin/spark-submit --master 'local[1]' --py-files ghawtrace/p02/clean_runs.py \
  --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
  --conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
  ghawtrace/p03/read_features.py "$FEATURES"
```

下游 PySpark 作业用 `--py-files ghawtrace/p03/read_features.py,ghawtrace/p02/clean_runs.py` 提交，沿用上面的 UTC 配置，在代码中执行 `from read_features import read_features` 和 `df = read_features(spark, batch_uri, "workflow_profile")`。Python 3 / PyArrow 可直接读取下载后的 Parquet。

### 复跑 P03-T02 集群验收

在 node1 的项目检出目录运行。每次使用新的 HDFS 输出目录。作业按工作流分组，在实际执行的 Worker 上计算画像，输出带分区、节点和应用 ID 的压缩 JSON 行。

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
hdfs dfs -get "$CLEAN_BATCH/runs_clean" "$WORK/runs_clean"
hdfs dfs -get "$CLEAN_BATCH/_SUCCESS" "$WORK/p02-marker.json"
hdfs dfs -get "$SPARK_OUT" "$WORK/spark-out"
(cd "$WORK/runs_clean" && sha256sum *.parquet) > "$WORK/clean.sha256"
echo "$WORK"
```

将该工作目录复制到已安装本仓库依赖的 Python 3 开发机，把 `WORK` 设为复制后的位置，再从仓库根目录运行。命令先重建四张 Parquet，再检查输入摘要、跨节点执行、每个工作流的列值和空值。通过时输出 `mismatches: 0`。

```sh
python -m ghawtrace.feature_pipeline --runs-input "$WORK/runs_clean" --output-root "$WORK/reference"
python -m ghawtrace.p03.verify_cluster --reference "$WORK"/reference/p03-* \
  --spark-output "$WORK/spark-out" --p02-marker "$WORK/p02-marker.json" \
  --clean-sha256 "$WORK/clean.sha256" --report "$WORK/acceptance.json"
```

<a name="p05-run-scores"></a>

### P05-T01 单次运行评分

输入是 P02 的不可变批次目录，其中含 JSON 格式的 `_SUCCESS` 与 `runs_clean/`。程序逐片计算 SHA-256。全量与样本使用同一份清洗表；样本按 P01 清单选择目标 run，每个目标仍取同工作流的完整历史。本机验收环境为 Python 3.12、PySpark 4.0.1、JDK 21 和仓库锁定的 Python 依赖。

以下 Bash 示例使用本机已取回的课程分片。安装好上述环境、激活虚拟环境，在仓库根目录执行。已有其他固定批次时可直接设置 `CLEAN`、`SAMPLE`，跳过本例的输入目录准备。

```bash
CLEAN="$PWD/.local/p05-input/app-20261125190005-0027_8644c332"
mkdir -p "$CLEAN/runs_clean"
cp -n .local/p02-marker.json "$CLEAN/_SUCCESS"
cp -n .local/p02-cluster-clean/*.parquet "$CLEAN/runs_clean/"
SAMPLE=$(find "$PWD/.local/p01" -name sample_manifest.json -print -quit)

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

成功作业打印新批次路径。P07/P08 可用 `spark.read.parquet(batch + "/run_scores")` 读取 `manifest.json` 状态为 `success` 的批次，按 `run_id` 连接。全量预期 4,112,526 个不同 run，样本 747 个。独立复算需要两个不同的全量 Spark 作业；逐键比较全部业务列，另对样本及具名案例从清洗记录重算。组内集群复跑可用上文 node1 的 `submit` 函数提交同一脚本，传入固定 `CLEAN_BATCH` 和独立 HDFS 输出根目录。课程 Spark 2 实机执行与 HDFS 读回待 P08 记录。[评分口径](technology_stack.md#p05-scoring) · [本机验收报告](reports/p05/p05_t01_acceptance.json)

### 教师脚本双分片抽查（可选）

在 macOS/Linux 的项目虚拟环境中安装 `pyarrow` 和 `pandas`，用 `unar` 解出两个分片抽查。仅需教师脚本和 `runs.rar`。

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

预期输出 `2 个分片，共 50,000 行`。核对全表时，将 `RUNS_DIR` 指向已解包的 165 个 Parquet 分片所在目录，安装 `duckdb` 后调用 `read_runs.query("SELECT count(*) AS n FROM runs")`，预期为 4,112,526。

## 系统设计与开发顺序

P01–P03 已验收。万明浩交付集群、清洗与分区实验；刘陈昱主责日志覆盖审计，并协作黄志成完成工作流画像的集群聚合与复核。刘陈昱已完成 P05-T01 单次运行评分的本机全量验收。季雨昊负责 P04 聚类，龙启波负责 P05 生命周期检测与回测，张锦稷负责 P06 日志指纹和证据定位。P07 对比结构化与联合评分，P08 集成查询和演示。观测期为 2025-08-14 至 2026-09-21（UTC）。[六人分工与验收](plan.md) · [课程索引](technology_stack.md#课程资料与查阅路径) · [协作与 Git 规则](AGENTS.md)

## 课程交付

每组最多六人，使用集群搭建实验环境，报告与系统演示合计 20–30 分钟。PPT 讲清项目完成内容、过程、数据集、经验与小组分工。按老师对个人评分的说明，每人都展示自己的课程技术实现和完成证据。最终提交**源代码及使用说明、PPT、项目文档**；具体截止时间与提交入口以教师后续通知为准。

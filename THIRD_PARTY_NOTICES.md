# 第三方软件与材料声明

本地 P01–P03 的第三方 Python 包固定于 [requirements.txt](requirements.txt)。其余版本按实测环境或打包结果登记，完整版权及许可证以实际发行版为准。

## 数据集、论文与课程材料

| 材料 | 作者、版本或来源 | 本项目用途与声明 |
|---|---|---|
| GH-AWTrace | Jasem Khelifi、Hao Li、Ali Ouni、Mohammed Sayagh、Mohamed Aymen Saied；本地快照由教师以 `runs.rar`、`logs.rar` 提供 | 用于运行画像和日志分析；课堂日志包是原数据集的子集。本地包对应的上游修订号尚未取得。作者说明源仓库各有条款，数据集卡未给第三方工作流文件和日志统一授权。[上游数据集及引用说明](https://huggingface.co/datasets/jaykay9999/GHAWTrace) |
| *GH-AWTrace: A Large-Scale Dataset of GitHub Agentic Workflow Execution Logs* | 同上五位作者，2026，arXiv:2611.06866v1 | 数据来源、采集方法与使用限制的引文。项目文档和 PPT 引用论文，复用图表或长段材料时另核具体来源。[论文 v1](https://arxiv.org/abs/2611.06866v1) |
| 运行表读取脚本 | 教师提供的本地 `GHAWTrace_ref/read_runs.py`，未标明独立版本、作者或开源许可证 | 小样本和字段参考。提交或公开衍生代码时标明来源，遵循课堂允许范围 |
| 2026 版项目介绍 PPT | 课程分发的本地 `大数据分析技术项目-2026版.pptx`，未见单独许可证 | 核对选题与考核要求 |

技术报告建议引文：Khelifi, J., Li, H., Ouni, A., Sayagh, M., and Saied, M. (2026). *GH-AWTrace: A Large-Scale Dataset of GitHub Agentic Workflow Execution Logs*. arXiv:2611.06866v1. [作者论文](https://arxiv.org/abs/2611.06866v1)

## 分析、实验与演示的直接依赖

| 上游项目 / 包 | 使用范围 | 当前版本依据 | 上游主许可证与依据 |
|---|---|---|---|
| Apache Spark / PySpark（含 Spark SQL、MLlib） | 分布式清洗、特征、聚类 | P02固定并实测2.0.0 | Apache-2.0，发行版还带有组件声明。[LICENSE](https://github.com/apache/spark/blob/master/LICENSE) · [NOTICE](https://github.com/apache/spark/blob/master/NOTICE) |
| Apache Arrow / pyarrow | 教师脚本、P01 与 P03 的 Parquet 读写 | P01 固定 25.0.1；P03 在 16.1.0 与 25.0.1 上复核，结果一致，集群待锁定 | Apache-2.0，随附组件许可见文件后部。[LICENSE](https://github.com/apache/arrow/blob/main/LICENSE.txt) |
| NumPy / numpy | P03 分位数、分组统计与周面板 | 本地实测 1.26.3，与 PyArrow 25.0.1 联测；集群待锁定 | BSD-3-Clause。[LICENSE](https://github.com/numpy/numpy/blob/main/LICENSE.txt) |
| SQLite（CPython sqlite3） | P02-T04 在本地磁盘核对 run 与 attempt | 本地 SQLite 3.53.1，随 Python 环境调用 | SQLite 核心为公有领域。[上游声明](https://sqlite.org/copyright.html) |
| pandas | 教师读取示例、有限规模的结果表 | 本地实测 2.2.3，集群待锁定 | BSD-3-Clause。[LICENSE](https://github.com/pandas-dev/pandas/blob/main/LICENSE) |
| DuckDB / duckdb | 教师 SQL 接口、独立复算与结果查询 | 教师脚本已引用，项目版本待锁定 | MIT。[LICENSE](https://github.com/duckdb/duckdb/blob/main/LICENSE) |
| scikit-learn | ARI、Isolation Forest、联合实验 | 计划引入，待锁定 | BSD-3-Clause。[COPYING](https://github.com/scikit-learn/scikit-learn/blob/main/COPYING) |
| Streamlit | Demo 查询与页面交互 | 计划引入，待锁定 | Apache-2.0。[LICENSE](https://github.com/streamlit/streamlit/blob/develop/LICENSE) |
| Plotly.py / plotly | 原型分布、运行与生命周期时间线 | 计划引入，待锁定 | MIT。[LICENSE](https://github.com/plotly/plotly.py/blob/main/LICENSE.txt) |

## 运行环境和开发工具

| 项目 | 使用情形及版本状态 | 许可依据 |
|---|---|---|
| CPython 及标准库 | 本地示例实测 3.12.14，P03 另实测 3.12.7，集群系统 Python 2.7.5。`tarfile`、`zipfile`、`io`、`re` 随解释器提供 | PSF License Agreement 及发行版列出的历史和组件许可。[官方许可](https://docs.python.org/3/license.html) |
| The Unarchiver CLI / unar、lsar | 本地解包与检查 RAR；系统工具，开发环境自行提供 | 本机安装包附 LGPL v2.1 文本，Homebrew 配方标注 LGPL-2.1-or-later。[上游命令行工具](https://theunarchiver.com/command-line) · [配方](https://formulae.brew.sh/formula/unar) |
| JVM / JDK | 运行 Spark；课程集群实装 Oracle JDK 1.8.0_471，由集群提供 | 以所选发行版的 LICENSE 和第三方声明为准 |
| Apache Hadoop | HDFS 2.7.7 提供跨节点存储，集群 `hadoop version` 已核验 | Apache-2.0，另含组件许可。[LICENSE](https://github.com/apache/hadoop/blob/trunk/LICENSE.txt) |
| Git | 已用于版本管理，本轮实测 2.56.1 | GPL v2，具体以项目 COPYING 为准。[COPYING](https://github.com/git/git/blob/master/COPYING) |

Parquet 文件格式由实际读写实现（Spark、PyArrow 或 DuckDB）覆盖。团队自产特征、模型和文档的授权范围另定。

## 锁定、更新与提交

1. P02 按分析、演示和开发工具记录包名、精确版本及来源，留存直接与解析出的传递依赖。单机验证标为本地实测。
2. 依赖增删时更新此表。复用上游实现、示例或资产时登记作者、地址、修订、改动与许可文件。
3. P09 对照提交包和环境复核依赖，保留所需版权、LICENSE 和 NOTICE，注明外部工具来源，并在 [history.md](history.md) 记录版本及日期。

登记结构参考刘陈昱另一项目的第三方声明，保留运行、开发、传递依赖与版本依据的区分。[参考文档](https://github.com/lcy0121/dsh-plugin-compat-check/blob/main/THIRD_PARTY_NOTICES.md)

# 第三方软件与材料声明

项目 Python 3 管线锁定 PyArrow、NumPy。下列版本区分实测、可选与计划项，完整许可随发行版。[requirements.txt](requirements.txt)

## 数据集、论文与课程材料

| 材料 | 来源、用途与使用范围 |
|---|---|
| GH-AWTrace | Jasem Khelifi、Hao Li、Ali Ouni、Mohammed Sayagh、Mohamed Aymen Saied。教师提供 `runs.rar`、`logs.rar` 作画像与日志分析，课堂日志为子集，上游修订号待核。各源仓库条款独立，数据集卡未统一授权第三方工作流及日志。[数据集与引用](https://huggingface.co/datasets/jaykay9999/GHAWTrace) |
| *GH-AWTrace: A Large-Scale Dataset of GitHub Agentic Workflow Execution Logs* | 文档、报告、PPT 引用上述五位作者（依序）、全名、2026、arXiv:2611.06866v1，说明采集方法和限制。复用图表或长段材料另核来源。[论文 v1](https://arxiv.org/abs/2611.06866v1) |
| `GHAWTrace_ref/read_runs.py` | 教师提供，用于抽样、字段参考。独立版本、作者及开源许可未标明，提交或公开衍生代码须注明来源并遵循课堂允许范围。 |
| `大数据分析技术项目-2026版.pptx` | 课程分发，用于核对选题与考核，未见单独许可证。 |
| 大数据分析技术周课件（PPT/PPTX） | 教师分发。项目按原文件名及页码引用教学方法，课件原件由课程渠道提供。[课程索引](technology_stack.md#课程资料与查阅路径) |

## 分析、实验与演示的直接依赖

| 项目 | 用途与版本依据 | 主许可及上游依据 |
|---|---|---|
| Apache Spark / PySpark（SQL、MLlib） | 集群清洗、特征、运行评分用 2.0.0，聚类待 P04。本机 P05 用 4.0.1 逐键复核，其 Python/JVM 桥 Py4J 0.10.9.9 为 BSD-3-Clause。[Py4J 许可](https://www.py4j.org/faq.html#what-is-the-license-of-py4j) | Apache-2.0 及发行组件声明。[LICENSE](https://github.com/apache/spark/blob/master/LICENSE) · [NOTICE](https://github.com/apache/spark/blob/master/NOTICE) |
| Apache Arrow / pyarrow | 教师示例、P01–P03 Parquet 与 P05 复算。固定 25.0.1，P03 另验 16.1.0 | Apache-2.0，附带组件见 [LICENSE](https://github.com/apache/arrow/blob/main/LICENSE.txt) |
| NumPy / numpy | P03 本机分组、分位数、周面板，1.26.3 与 Arrow 25.0.1 联测。集群画像用标准库 | BSD-3-Clause。[LICENSE](https://github.com/numpy/numpy/blob/main/LICENSE.txt) |
| SQLite / sqlite3 | P02-T04 磁盘对账 run/attempt，Python 环境实测 3.53.1 | 核心为公有领域。[声明](https://sqlite.org/copyright.html) |
| pandas | 教师示例实测 2.2.3，有限规模结果分析版本待锁定 | BSD-3-Clause。[LICENSE](https://github.com/pandas-dev/pandas/blob/main/LICENSE) |
| DuckDB / duckdb | 教师 SQL 接口、计划中的独立复算与查询，待锁定 | MIT。[LICENSE](https://github.com/duckdb/duckdb/blob/main/LICENSE) |
| scikit-learn（计划） | P04 轮廓系数、ARI，P07 Isolation Forest，待锁定 | BSD-3-Clause。[COPYING](https://github.com/scikit-learn/scikit-learn/blob/main/COPYING) |
| Streamlit（计划） | Demo 查询交互，待锁定 | Apache-2.0。[LICENSE](https://github.com/streamlit/streamlit/blob/develop/LICENSE) |
| Plotly.py / plotly（计划） | 原型分布、运行与生命周期时间线，待锁定 | MIT。[LICENSE](https://github.com/plotly/plotly.py/blob/main/LICENSE.txt) |

## 运行环境和开发工具

| 项目 | 使用情形及版本状态 | 许可依据 |
|---|---|---|
| CPython、标准库 | 本机 3.12.14，P03 另验 3.12.7，集群 2.7.5。含 tarfile、zipfile、io、re | PSF License Agreement 及历史/组件许可。[官方许可](https://docs.python.org/3/license.html) |
| The Unarchiver CLI（unar、lsar） | 本地自行提供，解包/检查 RAR | 本机包附 LGPL v2.1，Homebrew 标 LGPL-2.1-or-later。[上游](https://theunarchiver.com/command-line) · [配方](https://formulae.brew.sh/formula/unar) |
| JVM / JDK | Spark 运行时，集群提供 Oracle JDK 1.8.0_471，本机 P05 用 OpenJDK 21 | 所选发行版 LICENSE 及第三方声明 |
| Apache Hadoop | 集群 `hadoop version` 核验 2.7.7，HDFS 跨节点存储 | Apache-2.0 及组件许可。[LICENSE](https://github.com/apache/hadoop/blob/trunk/LICENSE.txt) |
| Git | 版本管理，实测 2.56.1 | GPL v2，详见 [COPYING](https://github.com/git/git/blob/master/COPYING) |

Parquet 按读写实现（Spark、PyArrow、DuckDB）登记。团队特征、模型、文档的授权另定。

## 锁定、更新与提交

依分析、演示、开发用途登记包名、版本、来源、实测环境及直接/传递依赖，随增删更新。复用代码、示例、资产须记作者、地址、修订、改动及许可。P09 对照提交包与环境，保留所需版权、LICENSE、NOTICE 和外部工具来源，在[历史](history.md)记版本、日期。

分类与版本依据参考刘陈昱另一项目的[第三方声明](https://github.com/lcy0121/dsh-plugin-compat-check/blob/main/THIRD_PARTY_NOTICES.md)。

# Data Engineer 面试题 & 扩展项目

> 基于 `dataengineer-transformations-python` 项目，涵盖代码审查、测试设计、性能优化与扩展编程。

---

## 🔍 一、代码审查类问题（Code Review）

### `ingest.py`

1. `sanitize_columns` 目前只处理了空格替换为下划线，但如果列名包含特殊字符（如 `-`、`.`、`(`）会怎样？请扩展这个函数使其更健壮。

2. `ref_df.write.parquet(transformation_path)` 没有设置 `mode`，如果目标路径已存在数据会发生什么？如何修复？

3. 代码中有 `print` 语句（`printSchema`, `show`）混入了生产逻辑，这在生产环境中是好的实践吗？如何改进？

### `word_count_transformer.py`

4. 代码中 import 了 `countDistinct` 但从未使用，这说明什么？如何在 CI/CD 中自动捕获这类问题？
   > 提示：项目已配置 `ruff`，对应 `F401` 规则。

5. `coalesce(1)` 会把所有数据写入单个文件，在什么场景下这是合理的？在什么场景下会成为性能瓶颈？

6. 测试中 `@pytest.mark.skip` 是什么意思？为什么它被跳过了？如果你接手这个项目，你会怎么处理？

### `distance_transformer.py`

7. 代码导入了 `math` 模块但从未使用，为什么？如何避免此类问题？

8. `mode="append"` 写 Parquet：如果同一批数据被运行两次，数据会重复吗？如何设计幂等性（Idempotency）？

9. Haversine 公式假设地球是完美球体，实际误差约 0.5%。在 Citibike 这个场景（城市内短途骑行）中可以接受吗？有没有更精确的替代方案？

---

## 🧪 二、测试设计类问题（Testing）

10. 目前单元测试只覆盖了 `sanitize_columns`，`word_count_transformer` 和 `distance_transformer` 的核心逻辑完全没有单元测试。请为 `compute_distance` 函数编写单元测试，不依赖真实 SparkSession。
    > 提示：考虑使用 mock 或重构函数使其可以单元测试。

11. 集成测试和单元测试的边界应该如何划分？这个项目的划分合理吗？

12. 如何为 `word_count_transformer` 补充边界测试？例如：
    - 空文件
    - 只有标点符号的文件
    - 超大文件（性能边界）

---

## ⚡ 三、性能与扩展性类问题（Performance & Scalability）

13. 如果 `words.txt` 从几 MB 增长到几百 GB，现有代码需要做哪些调整？

14. `groupBy("word").count()` 在极端情况下（词汇量极大或数据高度倾斜）可能有什么问题？

15. Citibike 数据从 CSV 转换为 Parquet 的意义是什么？Parquet 相比 CSV 有哪些具体优势？

---

## 🛠️ 四、扩展编程项目

### ⭐⭐ 难度一：代码质量改进

**目标：** 修复项目中所有已知的代码质量问题

- [ ] 移除未使用的 import（`countDistinct`、`math`）
- [ ] 扩展 `sanitize_columns`，支持更多特殊字符（如 `-`、`.`、`(`、`)`）
- [ ] 为 `ingest.py` 的 write 操作添加适当的 `mode` 参数
- [ ] 取消 word count 集成测试的 `@pytest.mark.skip` 并让其通过

---

### ⭐⭐⭐ 难度二：新增 Transformer

**目标：** 实现 `trip_duration_transformer.py`

- [ ] 读取 Citibike 数据，计算每次骑行的时长（分钟）
- [ ] 按时长分桶统计分布：
  - `0–10 min`
  - `10–30 min`
  - `30 min+`
- [ ] 输出结果为 Parquet 格式
- [ ] 编写完整的单元测试和集成测试

---

### ⭐⭐⭐⭐ 难度三：数据质量 Pipeline

**目标：** 为 `ingest.py` 增加数据质量校验层

- [ ] 校验经纬度范围是否在合理范围内（纽约市坐标范围）
- [ ] 校验行程时长是否为正数
- [ ] 将不合格的数据写入「隔离区」（quarantine path），而非直接丢弃
- [ ] 输出一份数据质量报告，包含：
  - 总行数
  - 合格行数
  - 不合格原因分布

---

### ⭐⭐⭐⭐⭐ 难度四：增量处理 & 幂等性

**目标：** 将现有的全量处理改造为支持增量处理

- [ ] 基于文件名或日期分区，只处理新增数据
- [ ] 保证重复运行不产生重复数据（幂等性）
- [ ] 用 `pytest` 编写验证幂等性的集成测试

---

## 💡 直接加分题

> `word_count_transformer.py` 中，`sailors'` 和 `gatsby's` 这类带撇号的词被当作一个词处理了。
>
> **问题：** 你觉得这个行为对吗？如果要正确处理以下两种情况：
> - **缩写词**（如 `haven't` → 保留为一个词）
> - **所有格**（如 `gatsby's` → 拆分为 `gatsby`）
>
> 你会如何修改正则表达式？请写出修改后的代码。

---

*建议优先完成 **难度一（代码质量改进）** 和 **难度二（trip_duration_transformer）**，这两个最能体现 Data Engineering 的日常工作能力。*

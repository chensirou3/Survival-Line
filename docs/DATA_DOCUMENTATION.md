# Survival Line 数据技术文档

## 一、项目概述

本项目构建了一个覆盖美国 **50 州 + DC**、**2008–2023 年** 的州-年面板数据集（state-year panel），用于计算每个州每年的 **Survival Line（生存线）**，并将其与最低工资进行比较分析。

### 核心公式（主版本）

```
SurvivalLine_{s,t} = 12 × ContractRent_{s,t}
                   + TFP_national_{t} × (RPP_goods_{s,t} / 100)
                   + 12 × ElectricBill_{s,t}
                   + 12 × GasBill_{s,t}
```

### 面板规模

| 维度 | 值 |
|------|------|
| 横截面单位 | 51（50 州 + DC） |
| 时间范围 | 2008–2023（16 年） |
| 理论总观测数 | 816 |
| 主版本有效观测 | 765（93.8%） |
| 唯一缺口 | ACS 2020 年（51 行，按设计保留缺失） |

---

## 二、数据源详细说明

### 2.1 最低工资（Minimum Wage）

| 属性 | 内容 |
|------|------|
| **主数据源** | Vaghul & Zipperer Historical Minimum Wage Dataset |
| **URL** | https://github.com/benzipperer/historicalminwage |
| **版本** | v1.4.0（GitHub Releases，Excel zip 格式） |
| **原始文件** | `mw_state_annual.xlsx`（从 zip 内提取） |
| **覆盖范围** | 2008–2022（主数据集），2023 由补丁补充 |
| **获取方式** | Python `requests` 自动下载 zip → 解压 → 读取 Excel |
| **脚本** | `scripts/02_min_wage/download_min_wage.py` |

**关键字段处理逻辑：**
- `min_wage_nominal`：使用 `Annual State Maximum`（该年州内最高生效费率）
- `federal_min_wage`：硬编码联邦最低工资历史表（1997–2023，最新为 $7.25）
- `binding_min_wage = max(state_mw, federal_mw)`
- `annualized_mw_income = binding_min_wage × 2080`（40小时/周 × 52周）

**2023 年数据补丁：**
- 脚本：`scripts/patches/patch_02_update_minwage_2023.py`
- 策略：先尝试 V&Z v1.5.0，若不可用则使用 DOL/EPI 手动编制的 51 州费率
- 来源：美国劳工部工时司（DOL Wage and Hour Division）、经济政策研究所（EPI）公开表格

### 2.2 住房（Housing）

| 属性 | 内容 |
|------|------|
| **数据源** | U.S. Census Bureau, American Community Survey (ACS) 1-Year Estimates |
| **API** | `https://api.census.gov/data/{year}/acs/acs1` |
| **主变量** | B25058_001E — Median Contract Rent（州级中位合同租金，月度） |
| **辅助变量** | B25064_001E — Median Gross Rent（含部分公用事业，仅用于稳健性检验） |
| **地理粒度** | `for=state:*`（所有州） |
| **覆盖范围** | 2008–2019, 2021–2023（**2020 年 ACS 1-Year 因 COVID 未发布**） |
| **获取方式** | 逐年调用 Census API，每次请求间隔 0.5 秒限流 |
| **脚本** | `scripts/03_housing/download_housing.py` |

**注意事项：**
- Contract Rent **不含** 公用事业费用，因此主公式中可单独加入电力和天然气
- Gross Rent **包含**部分公用事业，仅用于稳健性版本，**不可**与独立公用事业账单相加
- ACS 2020 年 1-Year Estimates 因 COVID 数据质量不达标而未被 Census Bureau 发布
- 本项目**不对 2020 年进行插值**，保留为缺失值

### 2.3 公用事业（Utilities）

#### 2.3.1 电力（Electricity）

| 属性 | 内容 |
|------|------|
| **数据源** | U.S. Energy Information Administration (EIA) |
| **主文件（2008–2020）** | `revenue_annual.xlsx` + `customers_annual.xlsx` |
| **主文件URL** | `https://www.eia.gov/electricity/data/state/{filename}` |
| **补丁文件（2021–2023）** | EIA Form HS861 历史汇总（`HS861 2010-.xlsx`） |
| **补丁URL** | `https://www.eia.gov/electricity/data/state/xls/861/HS861 2010-.xlsx` |
| **计算公式** | `月均账单 = (Revenue × 1000) / (Customers × 12)` |
| **筛选条件** | `Industry Sector Category = "Total Electric Industry"`，仅取 `Residential` 列 |
| **覆盖范围** | 2008–2023（补丁后 100%） |

**获取与处理脚本：**
- 原始管线：`scripts/04_utilities/download_utilities.py`
- 补丁脚本：`scripts/patches/patch_01_update_electricity.py`

**HS861 补丁逻辑：**
- Revenue 字段：`Residential Revenue (Thousands of Dollars)`
- Customers 字段：`Residential Number of Customer Accounts`
- 公式：`avg_monthly_bill = (Revenue × 1000) / (Customers × 12)`
- 交叉验证：与 EIA `table_5A.xlsx`（Wayback Machine 快照）比对，相关系数 = 1.0000

#### 2.3.2 天然气（Natural Gas）

| 属性 | 内容 |
|------|------|
| **数据源** | EIA Natural Gas Residential Price |
| **URL** | `https://www.eia.gov/dnav/ng/xls/NG_PRI_SUM_A_EPG0_PRS_DMCF_A.xls` |
| **原始单位** | $/MCF（每千立方英尺价格） |
| **估算逻辑** | 月均账单 = 价格 × 5 MCF/月（基于全国平均居民年消费 ≈ 60 MCF） |
| **标记** | `construction_flag_gas = "estimated_from_price"` |

**处理方式：**
- 原始数据为宽格式（每列一个州），脚本自动转为长格式
- 州名从列标题中提取（如 `"Alabama Price of Natural Gas..."` → `AL`）

### 2.4 食品（Food）

| 属性 | 内容 |
|------|------|
| **构造方法** | `Food_{s,t} = TFP_national_annual_t × (RPP_goods_{s,t} / 100)` |
| **标记** | `construction_flag_food = "reconstructed"`（AK/HI 标记为 `"reconstructed_AK_HI"`） |

**组成部分 A — USDA Thrifty Food Plan (TFP)：**
- 来源：USDA Food and Nutrition Service, Cost of Food Reports
- URL：https://www.fns.usda.gov/cnpp/usda-food-plans-cost-food-reports
- 数据级别：全国（national），月度成本
- 参考家庭：4人家庭（2成人 19-50岁，2儿童 6-8岁和 9-11岁）
- 获取方式：从 USDA 官方报告**手动编制**年度值，硬编码在脚本中
- 注意：2021年10月 TFP 重新评估后数值显著上升（$577 → $836/月）

**组成部分 B — BEA Regional Price Parities (RPP Goods)：**
- 来源：Bureau of Economic Analysis (BEA)
- 下载地址：https://apps.bea.gov/regional/zip/SARPP.zip
- 表名：SARPP（State Annual Regional Price Parities）
- LineCode = 2（Goods 商品类）
- 单位：指数（US = 100）
- 覆盖范围：2008–2023
- 获取方式：Python 自动下载 zip → 解压 → 读取 CSV → 筛选 LineCode=2

**脚本：** `scripts/05_food/download_food.py`

### 2.5 控制变量（Controls）

| 变量 | 来源 | 表/API | 覆盖范围 |
|------|------|--------|---------|
| RPP All Items | BEA SARPP | LineCode=1 | 2008–2023 |
| RPP Rents | BEA SARPP | LineCode=3 | 2008–2023 |
| 贫困率 | ACS Subject Table S1701 | `S1701_C03_001E` | 2008–2023（无2020） |
| 中位家庭收入 | ACS Table B19013 | `B19013_001E` | 2008–2023（无2020） |

- RPP 数据复用 Food 模块已下载的 `SARPP.zip`
- ACS 控制变量通过 Census API 逐年获取（与 Housing 模块共享限流策略）
- 脚本：`scripts/06_controls/download_controls.py`

---

## 三、数据处理管线

### 3.1 项目目录结构

```
├── config/                     # 配置文件
│   ├── sources.yaml            # 数据源注册表
│   ├── construction_rules.yaml # Survival Line 构造规则
│   └── paths.yaml              # 路径配置
├── scripts/
│   ├── 00_setup/utils.py       # 公共工具（FIPS查找表、日志、覆盖率审计）
│   ├── 01_skeleton/            # Module 0: 构建 51×16 骨架面板
│   ├── 02_min_wage/            # Module 1: 最低工资下载与清洗
│   ├── 03_housing/             # Module 2: 住房数据（ACS API）
│   ├── 04_utilities/           # Module 3: 公用事业（EIA 电力+天然气）
│   ├── 05_food/                # Module 4: 食品成本重构（TFP×RPP）
│   ├── 06_controls/            # Module 5: 控制变量
│   ├── 07_merge/               # Module 6: 合并所有模块
│   ├── 08_construct/           # Module 7: 构造 Survival Line
│   ├── 09_qc/                  # Module 8: 质量控制
│   ├── patches/                # 补丁脚本（2021–2023 电力 + 2023 最低工资）
│   └── run_all.py              # 主运行脚本
├── data_raw/                   # 原始下载数据
├── data_clean/                 # 清洗后中间数据
├── data_final/                 # 最终输出
│   ├── survival_main/          # 全版本面板
│   └── export/                 # 精简导出版
├── qc/                         # 质量控制报告
├── logs/                       # 运行日志
└── docs/                       # 文档
```

### 3.2 执行顺序

管线设计为**幂等**（idempotent），每个模块可独立重新运行。完整执行顺序：

```
Module 0 → 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8
骨架面板 → 最低工资 → 住房 → 公用事业 → 食品 → 控制变量 → 合并 → 构造SL → QC
```

补丁在基线管线之后执行：
```
Patch 01（电力2021–2023） → Patch 02（MW 2023） → Patch 03（重合并） → Patch 04（QC对比）
```

### 3.3 关键合并逻辑

- 主键：`state_fips`（2位零填充字符串）+ `year`（整数）
- 合并方式：`LEFT JOIN` 到骨架面板（保证 816 行不变）
- 重复列自动检测并在合并前删除

---

## 四、输出文件说明

### 4.1 主要输出文件

| 文件 | 路径 | 说明 |
|------|------|------|
| 全版本面板（补丁后） | `data_final/survival_main/survival_line_all_versions_patched.csv` | 三个 SL 版本 + 所有中间变量 |
| **精简导出版（补丁后）** | **`data_final/export/survival_line_main_patched.csv`** | **推荐用于分析** |
| 全版本面板（补丁前） | `data_final/survival_main/survival_line_all_versions.csv` | 原始管线输出 |
| 合并面板（补丁后） | `data_clean/merged/panel_merged_patched.csv` | 合并但未构造 SL |

### 4.2 变量字典（精简导出版）

| 变量名 | 类型 | 单位 | 说明 |
|--------|------|------|------|
| `state_fips` | string | — | 州 FIPS 代码（2位，零填充） |
| `state_abbr` | string | — | 州缩写（如 CA, NY） |
| `state_name` | string | — | 州全名 |
| `year` | int | — | 年份（2008–2023） |
| `census_region` | string | — | Census 区域（Northeast/Midwest/South/West） |
| `census_division` | string | — | Census 分区（9类） |
| `contract_rent_monthly` | float | $/月 | ACS 中位合同租金 |
| `contract_rent_annual` | float | $/年 | = contract_rent_monthly × 12 |
| `electric_bill_monthly` | float | $/月 | 州级平均居民电费月账单 |
| `electric_bill_annual` | float | $/年 | = electric_bill_monthly × 12 |
| `gas_bill_monthly` | float | $/月 | 估算天然气月账单 |
| `gas_bill_annual` | float | $/年 | = gas_bill_monthly × 12 |
| `tfp_national_annual` | float | $/年 | USDA TFP 全国年度食品成本 |
| `rpp_goods_index` | float | 指数 | BEA RPP 商品价格指数（US=100） |
| `food_reconstructed_annual` | float | $/年 | = tfp_national_annual × (rpp_goods_index/100) |
| `construction_flag_food` | string | — | 食品数据构造标记 |
| `survival_line_nominal_main` | float | $/年 | **主版本 Survival Line（名义值）** |
| `min_wage_nominal` | float | $/小时 | 州级名义最低工资 |
| `binding_min_wage` | float | $/小时 | max(州MW, 联邦MW) |
| `annualized_mw_income` | float | $/年 | = binding_min_wage × 2080 |
| `mw_survival_gap_main` | float | $/年 | = annualized_mw_income − survival_line |
| `mw_survival_ratio_main` | float | 比值 | = annualized_mw_income / survival_line |
| `quality_flag` | string | — | 质量标记（ok / gas_missing / acs_2020_missing） |

---

## 五、Survival Line 版本定义

| 版本 | 公式 | 用途 |
|------|------|------|
| **main** | 12×ContractRent + Food + 12×ElecBill + 12×GasBill | 主分析 |
| **grossrent** | 12×GrossRent + Food | 稳健性（Gross Rent 含部分公用事业） |
| **no_gas** | 12×ContractRent + Food + 12×ElecBill | 稳健性（忽略天然气） |

---

## 六、已知数据缺口与处理

| 缺口 | 原因 | 处理 |
|------|------|------|
| ACS 2020 | Census 因 COVID 未发布 1-Year | **保留缺失，不插值** |
| 电力 2021–2023（已修复） | 原始 EIA 文件仅至 2020 | Patch 01: HS861 历史文件 |
| 最低工资 2023（已修复） | V&Z v1.4.0 仅至 2022 | Patch 02: DOL/EPI 手动补充 |
| 天然气账单 | 基于价格估算 | 标记 `estimated_from_price` |
| 食品成本 | 重构值，非直接观测 | 标记 `reconstructed` |

---

## 七、质量控制

QC 检查项目：
1. **覆盖率审计**：每个模块的州×年完整度
2. **缺失值报告**：关键变量的缺失数量和百分比
3. **异常值检测**：IQR × 3 方法
4. **双重计算检查**：Gross Rent 与独立公用事业未同时进入主版本
5. **AK/HI 特殊检查**：食品标记验证
6. **补丁连续性**：电力 2020→2021 变化（均值 +3.0%，无异常跳变）
7. **HS861 交叉验证**：与 table_5A 比对，相关系数 = 1.0000

报告目录：`qc/`

---

## 八、复现指南

### 环境要求
- Python 3.8+
- 依赖包：`pandas`, `requests`, `pyyaml`, `openpyxl`, `numpy`

### 完整执行
```bash
python scripts/run_all.py
python scripts/patches/patch_01_update_electricity.py
python scripts/patches/patch_02_update_minwage_2023.py
python scripts/patches/patch_03_remerge_and_reconstruct.py
python scripts/patches/patch_04_qc_summary.py
```

### 单模块执行
```bash
python scripts/01_skeleton/build_skeleton.py   # 骨架面板
python scripts/03_housing/download_housing.py   # 住房（需联网）
python scripts/05_food/download_food.py          # 食品
```

---

## 九、引用

| 数据源 | 引用 |
|--------|------|
| Minimum Wage | Vaghul, K. & Zipperer, B. Historical state and sub-state minimum wage data. GitHub: benzipperer/historicalminwage |
| Housing | U.S. Census Bureau, ACS 1-Year Estimates, Tables B25058 & B25064 |
| Electricity | U.S. EIA, Form EIA-861 & State Electricity Profiles |
| Natural Gas | U.S. EIA, Natural Gas Prices |
| Food (TFP) | USDA FNS, Thrifty Food Plan Cost of Food Reports |
| Food (RPP) | U.S. BEA, Regional Price Parities by State |
| Poverty & Income | U.S. Census Bureau, ACS Tables S1701 & B19013 |

---

*文档生成日期：2026-03-08*
*项目仓库：https://github.com/chensirou3/Survival-Line*
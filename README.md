# PySpark & Data Engineering Learning Hub

An enterprise-grade, comprehensive learning repository covering PySpark, Pandas, SQL, ETL, Data Warehouse, Delta Lake, PySpark ML, Shell Scripting, and more with interactive 3D visualizations and practical code examples.

## Overview

This repository provides 360-degree coverage of essential data engineering concepts, designed for students, professionals, and enterprises. Each concept includes interactive 3D HTML visualizations to help understand complex data flows and transformations, along with production-ready Python/PySpark code examples.

**122 Concepts across 12 Categories**

## Features

- **Interactive 3D Visualizations**: Each concept comes with Three.js-powered 3D visualizations
- **Comprehensive Code Examples**: Production-ready Python and PySpark code
- **Enterprise-Grade Structure**: Well-organized, documented, and maintainable
- **Progressive Learning Path**: Concepts organized from basics to advanced
- **Cross-Platform**: Works on any modern browser
- **Dark/Light Theme Support**: Comfortable viewing in any environment
- **Delta Lake Integration**: ACID transactions, time travel, and lakehouse patterns
- **Machine Learning**: PySpark MLlib with comprehensive examples
- **Real-World Projects**: Capstone projects with end-to-end implementations

## Repository Structure

```
PySpark/
├── index.html                 # Main navigation portal
├── assets/                    # Shared CSS, JS, and images
│   ├── css/
│   ├── js/
│   └── images/
├── data/                      # Sample datasets
├── pyspark/                   # PySpark concepts (15 topics)
│   ├── 01_rdd_basics/
│   ├── 02_dataframes/
│   ├── 03_transformations/
│   ├── 04_actions/
│   ├── 05_joins/
│   ├── 06_window_functions/
│   ├── 07_aggregations/
│   ├── 08_udfs/
│   ├── 09_spark_sql/
│   ├── 10_partitioning_bucketing/
│   ├── 11_caching_persistence/
│   ├── 12_broadcast_variables/
│   ├── 13_accumulators/
│   ├── 14_spark_streaming/
│   └── 15_performance_optimization/
├── pandas/                    # Pandas concepts (15 topics)
│   ├── 01_series_dataframes/
│   ├── 02_data_selection/
│   ├── 03_filtering_indexing/
│   ├── 04_groupby_operations/
│   ├── 05_merge_join_concat/
│   ├── 06_pivot_tables/
│   ├── 07_reshaping_data/
│   ├── 08_time_series/
│   ├── 09_missing_data/
│   ├── 10_string_operations/
│   ├── 11_apply_lambda/
│   ├── 12_multi_index/
│   ├── 13_data_types/
│   ├── 14_reading_writing/
│   └── 15_performance_tips/
├── sql/                       # SQL concepts (15 topics)
│   ├── 01_select_basics/
│   ├── 02_where_filtering/
│   ├── 03_joins/
│   ├── 04_group_by/
│   ├── 05_having_clause/
│   ├── 06_order_by/
│   ├── 07_subqueries/
│   ├── 08_ctes/
│   ├── 09_window_functions/
│   ├── 10_union_set_operations/
│   ├── 11_case_statements/
│   ├── 12_null_handling/
│   ├── 13_indexes_performance/
│   ├── 14_views/
│   └── 15_stored_procedures/
├── etl/                       # ETL concepts (12 topics)
│   ├── 01_etl_vs_elt/
│   ├── 02_data_extraction/
│   ├── 03_data_transformation/
│   ├── 04_data_loading/
│   ├── 05_incremental_full_load/
│   ├── 06_change_data_capture/
│   ├── 07_data_quality/
│   ├── 08_error_handling/
│   ├── 09_logging_monitoring/
│   ├── 10_scheduling_orchestration/
│   ├── 11_data_lineage/
│   └── 12_etl_best_practices/
├── datawarehouse/             # Data Warehouse concepts (14 topics)
│   ├── 01_dw_architecture/
│   ├── 02_star_schema/
│   ├── 03_snowflake_schema/
│   ├── 04_fact_tables/
│   ├── 05_dimension_tables/
│   ├── 06_slowly_changing_dimensions/
│   ├── 07_olap_vs_oltp/
│   ├── 08_data_marts/
│   ├── 09_kimball_vs_inmon/
│   ├── 10_surrogate_keys/
│   ├── 11_conformed_dimensions/
│   ├── 12_dw_best_practices/
│   ├── 13_scd_implementation/
│   └── 14_cdc_implementation/
├── deltalake/                 # Delta Lake concepts (12 topics)
│   ├── 01_introduction/
│   ├── 02_acid_transactions/
│   ├── 03_time_travel/
│   ├── 04_schema_evolution/
│   ├── 05_merge_upsert/
│   ├── 06_optimize_vacuum/
│   ├── 07_z_ordering/
│   ├── 08_change_data_feed/
│   ├── 09_streaming/
│   ├── 10_constraints/
│   ├── 11_clone_tables/
│   └── 12_best_practices/
├── pyspark_ml/                # PySpark ML concepts (12 topics)
│   ├── 01_ml_basics/
│   ├── 02_feature_engineering/
│   ├── 03_classification/
│   ├── 04_regression/
│   ├── 05_clustering/
│   ├── 06_pipelines/
│   ├── 07_model_evaluation/
│   ├── 08_hyperparameter_tuning/
│   ├── 09_recommendation/
│   ├── 10_nlp/
│   ├── 11_model_persistence/
│   └── 12_best_practices/
├── shell_scripting/           # Shell Scripting concepts (12 topics)
│   ├── 01_bash_basics/
│   ├── 02_variables_data_types/
│   ├── 03_control_structures/
│   ├── 04_functions/
│   ├── 05_file_operations/
│   ├── 06_text_processing/
│   ├── 07_data_pipeline_scripts/
│   ├── 08_etl_automation/
│   ├── 09_log_processing/
│   ├── 10_cron_scheduling/
│   ├── 11_error_handling/
│   └── 12_best_practices/
├── capstone_projects/         # End-to-End Projects (3 topics)
│   ├── 01_lakehouse_pipeline/
│   ├── 02_streaming_pipeline/
│   └── 03_cdc_scd_implementation/
├── performance_lab/           # Performance Optimization (4 topics)
│   ├── 01_small_files/
│   ├── 02_skewed_joins/
│   ├── 03_cache_optimization/
│   └── 04_execution_plans/
├── data_quality/              # Data Quality & Testing (4 topics)
│   ├── 01_unit_testing/
│   ├── 02_data_validation/
│   ├── 03_schema_evolution/
│   └── 04_error_handling/
└── interview_prep/            # Interview Preparation (4 topics)
    ├── 01_concepts/
    ├── 02_coding_challenges/
    ├── 03_system_design/
    └── 04_troubleshooting/
```

## Categories

| Category | Topics | Description |
|----------|--------|-------------|
| PySpark | 15 | Distributed data processing with Apache Spark |
| Pandas | 15 | Single-node data analysis with Python |
| SQL | 15 | Database querying with PySpark DataFrame API and SQL |
| ETL | 12 | Extract, Transform, Load patterns |
| Data Warehouse | 14 | DW architecture, schemas, SCD, CDC |
| Delta Lake | 12 | ACID transactions, time travel, lakehouse |
| PySpark ML | 12 | Machine learning at scale |
| Shell Scripting | 12 | Bash scripting for data engineering |
| Capstone Projects | 3 | End-to-end real-world implementations |
| Performance Lab | 4 | Spark optimization and debugging |
| Data Quality | 4 | Testing and validation patterns |
| Interview Prep | 4 | Interview questions and system design |

## Getting Started

### Prerequisites

- Python 3.8+
- Apache Spark 3.x (for PySpark examples)
- Delta Lake 2.4+ (for Delta Lake examples)
- Modern web browser (Chrome, Firefox, Safari, Edge)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/sunkaramallikarjuna369/PySpark.git
cd PySpark
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Open `index.html` in your browser to start learning!

### Running PySpark Examples

```bash
# Using spark-submit
spark-submit pyspark/01_rdd_basics/rdd_basics.py

# Using Python directly (if PySpark is installed)
python pyspark/01_rdd_basics/rdd_basics.py
```

### Running Delta Lake Examples

```bash
# Delta Lake requires Spark with Delta extension
spark-submit --packages io.delta:delta-core_2.12:2.4.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  deltalake/01_introduction/delta_intro.py
```

### Running PySpark ML Examples

```bash
# PySpark ML is included with PySpark
spark-submit pyspark_ml/01_ml_basics/ml_basics.py
```

## Learning Paths

### Beginner Path
1. Start with **Pandas** basics (Series, DataFrames, Selection)
2. Move to **SQL** fundamentals (SELECT, WHERE, JOINs)
3. Learn **PySpark** basics (RDD, DataFrames)
4. Understand **ETL** concepts
5. Explore **Data Warehouse** architecture

### Intermediate Path
1. **PySpark** intermediate (Joins, Window Functions, Aggregations)
2. **SQL** advanced queries (CTEs, Window Functions)
3. **Delta Lake** fundamentals (ACID, Time Travel, Schema Evolution)
4. **PySpark ML** basics (Feature Engineering, Classification, Regression)
5. **Shell Scripting** for automation

### Advanced Path
1. **PySpark** advanced topics (UDFs, Streaming, Optimization)
2. **Delta Lake** advanced (Merge/Upsert, CDF, Streaming)
3. **PySpark ML** advanced (Pipelines, Hyperparameter Tuning, NLP)
4. **Performance Lab** (Small Files, Skewed Joins, Execution Plans)
5. **Capstone Projects** (Lakehouse Pipeline, Streaming Pipeline)

### Interview Preparation Path
1. **Core Concepts** - Spark architecture, memory management
2. **Coding Challenges** - Common PySpark problems
3. **System Design** - Data platform architecture
4. **Troubleshooting** - Debug OOM, slow jobs, skew issues

## Technology Stack

- **Visualization**: Three.js for 3D visualizations
- **Styling**: Custom CSS with CSS Variables for theming
- **Code Examples**: Python, PySpark, SQL, Shell
- **Data Lake**: Delta Lake for ACID transactions
- **Machine Learning**: PySpark MLlib
- **Documentation**: Markdown

## Key Dependencies

```
pyspark>=3.4.0
delta-spark>=2.4.0
pandas>=2.0.0
numpy>=1.24.0
scikit-learn>=1.3.0
great-expectations>=0.17.0
pytest>=7.4.0
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Code of Conduct

Please read our [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) before contributing.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Apache Spark community
- Delta Lake community
- Pandas development team
- Three.js contributors
- All contributors to this repository

## Support

If you find this repository helpful, please give it a star and share it with others!

For questions or suggestions, please open an issue on GitHub.

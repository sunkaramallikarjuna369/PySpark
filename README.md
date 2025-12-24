# PySpark & Data Engineering Learning Hub

An enterprise-grade, comprehensive learning repository covering PySpark, Pandas, SQL, ETL, and Data Warehouse concepts with interactive 3D visualizations and practical code examples.

## Overview

This repository provides 360-degree coverage of essential data engineering concepts, designed for students, professionals, and enterprises. Each concept includes interactive 3D HTML visualizations to help understand complex data flows and transformations, along with production-ready Python/PySpark code examples.

## Features

- **Interactive 3D Visualizations**: Each concept comes with Three.js-powered 3D visualizations
- **Comprehensive Code Examples**: Production-ready Python and PySpark code
- **Enterprise-Grade Structure**: Well-organized, documented, and maintainable
- **Progressive Learning Path**: Concepts organized from basics to advanced
- **Cross-Platform**: Works on any modern browser
- **Dark/Light Theme Support**: Comfortable viewing in any environment

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
└── datawarehouse/             # Data Warehouse concepts (12 topics)
    ├── 01_dw_architecture/
    ├── 02_star_schema/
    ├── 03_snowflake_schema/
    ├── 04_fact_tables/
    ├── 05_dimension_tables/
    ├── 06_slowly_changing_dimensions/
    ├── 07_olap_vs_oltp/
    ├── 08_data_marts/
    ├── 09_kimball_vs_inmon/
    ├── 10_surrogate_keys/
    ├── 11_conformed_dimensions/
    └── 12_dw_best_practices/
```

## Getting Started

### Prerequisites

- Python 3.8+
- Apache Spark 3.x (for PySpark examples)
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

### Running Pandas Examples

```bash
python pandas/01_series_dataframes/series_dataframes.py
```

## Learning Paths

### Beginner Path
1. Start with **Pandas** basics (Series, DataFrames, Selection)
2. Move to **SQL** fundamentals (SELECT, WHERE, JOINs)
3. Learn **PySpark** basics (RDD, DataFrames)
4. Understand **ETL** concepts
5. Explore **Data Warehouse** architecture

### Advanced Path
1. **PySpark** advanced topics (Window Functions, UDFs, Optimization)
2. **SQL** advanced queries (CTEs, Window Functions, Stored Procedures)
3. **ETL** patterns (CDC, Incremental Loads, Data Quality)
4. **Data Warehouse** design (Star/Snowflake Schema, SCDs)

## Technology Stack

- **Visualization**: Three.js, D3.js
- **Styling**: Custom CSS with CSS Variables for theming
- **Code Examples**: Python, PySpark, SQL
- **Documentation**: Markdown

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Code of Conduct

Please read our [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) before contributing.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Apache Spark community
- Pandas development team
- Three.js contributors
- All contributors to this repository

## Support

If you find this repository helpful, please give it a star and share it with others!

For questions or suggestions, please open an issue on GitHub.

# Database Migration Reconciliation Tool

Compare an old and a new PySpark DataFrame during a data migration. The tool
checks schema differences, selected column values for matching primary keys,
and primary keys that exist in only one DataFrame.

It is designed for use in Databricks notebooks and produces both Spark
DataFrames for follow-up analysis and an interactive HTML report.

## What it checks

- **Schema Comparison** — columns and data types in each DataFrame.
- **Column Comparison** — matching and differing values for primary keys shared
  by both DataFrames.
- **PK Comparison** — distinct-key counts, duplicate-key signals, and primary
  keys found in only one DataFrame.

## Install in Databricks

Install the workspace wheel in a notebook:

```python
%pip install --force-reinstall --no-deps \
  "/Workspace/Users/angus.blance@krystalclarity.com/data-reconciliation-tool/database_migration_reconciliation_tool-0.1.0-py3-none-any.whl"
```

Restart Python if Databricks prompts you to do so.

## Quick start

```python
from column_comparison import ColumnComparison

comparison = ColumnComparison(
    input_dataframes=[
        spark.table("old_customer_table"),
        spark.table("new_customer_table"),
    ],
    join_key_columns=["UserID"],
    columns_to_compare=["FirstName", "LastName"],
    max_rows_per_column=20,
)

comparison.display_pretty()
```

The two input DataFrames must use the same primary-key column names. The tool
accepts exactly two DataFrames: DataFrame 1 is the old/source DataFrame and
DataFrame 2 is the new/target DataFrame.

## Output DataFrames

The instance keeps the generated Spark DataFrames available for notebook work:

```python
display(comparison.schema_comparison_df)
display(comparison.difference_analysis_df)
display(comparison.key_analysis_stats_df)
display(comparison.primary_key_comparison_df)
display(comparison.primary_key_exceptions_df)
```

| DataFrame | Contents |
| --- | --- |
| `schema_comparison_df` | Column presence and data types in both DataFrames. |
| `difference_analysis_df` | A bounded sample of value differences for shared primary keys. |
| `key_analysis_stats_df` | Match/difference counts and percentages per selected column. |
| `primary_key_comparison_df` | Distinct PK count, total row count, and exclusive PK count for each DataFrame. |
| `primary_key_exceptions_df` | The exact primary keys found in only one DataFrame. |

`display_pretty()` presents the same information as three report views. The
report limits displayed exclusive primary keys to 100 per DataFrame by default,
but `primary_key_exceptions_df` remains available for full Spark-based analysis.

## Configuration

| Argument | Default | Purpose |
| --- | --- | --- |
| `input_dataframes` | required | The two Spark DataFrames to compare. |
| `join_key_columns` | `None` | Primary-key columns used for the value and PK comparisons. Omit for schema-only comparison. |
| `columns_to_compare` | `None` | Non-key columns to compare. When omitted, all shared non-key columns are compared. |
| `compare_all_schema_columns` | `True` | Include every input column in Schema Comparison. |
| `max_rows_per_column` | `4` | Maximum differing rows shown per compared column in the report. |
| `max_rows_per_primary_key` | `100` | Maximum exclusive primary keys shown per DataFrame in the report. |
| `logger_level` | `logging.INFO` | Logging level for the comparison instance. |

## How value comparison works

Column Comparison uses an inner join on `join_key_columns`. Therefore it only
compares values for primary keys shared by both DataFrames. Rows that appear in
only one DataFrame are intentionally excluded from this calculation and are
reported in PK Comparison instead.

For example, if DataFrame 1 has 107 rows, DataFrame 2 has 105 rows, and they
share 100 primary keys, value-match percentages are calculated from those 100
shared keys.

## Primary-key expectations

`primary_key_comparison_df` shows both total row counts and distinct PK counts.
If they differ for a DataFrame, that DataFrame contains duplicate primary keys.
Resolve duplicate keys before relying on Column Comparison: duplicate keys can
produce multiple joined rows and inflate the value-comparison totals.

## About

This tool was created by [Krystal Clarity](https://www.krystalclarity.com/) to
make old-to-new data-platform migrations quicker to validate and easier to
explain.

The library is intentionally small. Its implementation and report assets live
in `src/database-migration-reconciliation-tool/`, so it is straightforward to
inspect, adapt, and use.

## Ways to use it

### Use the wheel in Databricks

For a repeatable notebook workflow, use the workspace-wheel install command
shown above. This is the recommended approach when the library is being shared
or reused.

### Use the repository with an AI coding assistant

Point an assistant at this repository and provide the two Spark DataFrames and
their primary-key columns. For example:

```text
Use ColumnComparison from this repository to compare my old and new PySpark
DataFrames. Join them on UserID, compare FirstName and LastName, and show the
interactive report plus the generated Spark DataFrames.
```

### Adapt the source directly

For a one-off or heavily customised use case, copy the implementation files:

- `column_comparison.py`
- `index.html`
- `styles.css`

Keep those files together so the report renderer can load its HTML and CSS
assets.

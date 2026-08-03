# Database Migration Reconciliation Tool

A small PySpark tool from [Krystal Clarity](https://www.krystalclarity.com/)
for checking whether migrated data still matches the old data. It compares
schemas, selected values for shared primary keys, and keys found in only one
DataFrame.

## Use it in Databricks

```python
%pip install --force-reinstall --no-deps \
  "<workspace-wheel-path>/database_migration_reconciliation_tool-0.1.0-py3-none-any.whl"

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

Replace `<workspace-wheel-path>` with the workspace location where your team
publishes the wheel.

The interactive report has Schema Comparison, Column Comparison, and PK
Comparison views. Generated Spark DataFrames are also available on the object,
for example `comparison.schema_comparison_df` and
`comparison.primary_key_exceptions_df`.

## Other ways to use it

- **With an AI coding assistant:** point it at this repository and ask it to
  apply `ColumnComparison` to your old and new PySpark DataFrames.
- **From source:** the implementation and report assets are deliberately kept
  together in `src/database-migration-reconciliation-tool/` so they are easy to
  inspect or adapt.

The tool accepts exactly two DataFrames. Value comparison uses an inner join on
the supplied primary keys, so rows that exist in only one DataFrame appear in
PK Comparison rather than the value-match totals.

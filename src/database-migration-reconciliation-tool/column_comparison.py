import logging
from functools import lru_cache, reduce
from html import escape
from pathlib import Path

import pyspark.sql.functions as spark_functions
from IPython.display import HTML, display
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

_ASSET_DIR = Path(__file__).resolve().parent


@lru_cache(maxsize=1)
def _read_report_asset(filename: str) -> str:
    return (_ASSET_DIR / filename).read_text(encoding="utf-8")


class ColumnComparison:
    """
    Compare Spark DataFrames and optionally display an HTML comparison report.

    Args:
        input_dataframes: Exactly two Spark DataFrames to compare.
        join_key_columns: Columns used to join the input DataFrames.
        columns_to_compare: Non-key columns whose values should be compared.
        compare_all_schema_columns: Whether the Schema tab should include every
            column in the input DataFrames, rather than only join and compared
            columns.
        max_rows_per_primary_key: Maximum exclusive primary-key rows shown for
            each DataFrame in the HTML report.
        logger_level: Logging level for the logger.

    Applicable comparison DataFrames are generated during initialization as
    public attributes. Call ``display_pretty()`` to render those stored
    DataFrames as an interactive HTML report.
    """

    def __init__(
        self,
        input_dataframes: list[DataFrame],
        join_key_columns: list[str] | None = None,
        columns_to_compare: list[str] | None = None,
        max_rows_per_column: int = 4,
        logger_level: int = logging.INFO,
        compare_all_schema_columns: bool = True,
        max_rows_per_primary_key: int = 100,
    ) -> None:
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.logger.setLevel(logger_level)
        self.logger.debug("Initializing ColumnComparison")
        self.logger.debug(
            f"input_dataframes count={len(input_dataframes)}, "
            f"join_key_columns={join_key_columns}, "
            f"columns_to_compare={columns_to_compare}, "
            f"compare_all_schema_columns={compare_all_schema_columns}, "
            f"max_rows_per_column={max_rows_per_column}, "
            f"max_rows_per_primary_key={max_rows_per_primary_key}"
        )
        self._join_key_columns: list[str] = join_key_columns or []
        self._columns_to_compare: list[str] = columns_to_compare or []
        self._compare_all_columns_for_differences: bool = columns_to_compare is None
        self._compare_all_schema_columns: bool = compare_all_schema_columns
        if max_rows_per_primary_key < 1:
            raise ValueError("max_rows_per_primary_key must be at least 1.")
        self._max_rows_per_primary_key: int = max_rows_per_primary_key
        self._dataset_row_counts: list[int] = []
        spark_session: SparkSession = (
            SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        )
        self._spark: SparkSession = spark_session
        if max_rows_per_column < 1:
            raise ValueError("max_rows_per_column must be at least 1.")
        self._max_rows_per_column: int = max_rows_per_column

        self.schema_comparison_df: DataFrame
        self.difference_analysis_df: DataFrame
        self.key_analysis_stats_df: DataFrame
        self.primary_key_comparison_df: DataFrame
        self.primary_key_exceptions_df: DataFrame

        self.input_dataframes = input_dataframes

    def __repr__(self) -> str:
        """Return a compact representation of the configured comparison."""

        def preview_columns(column_names: list[str]) -> str:
            maximum_columns_to_show = 5
            preview = column_names[:maximum_columns_to_show]
            suffix = (
                f", ... ({len(column_names)} total)"
                if len(column_names) > maximum_columns_to_show
                else ""
            )
            return (
                f"[{', '.join(repr(column_name) for column_name in preview)}{suffix}]"
            )

        columns_to_compare = (
            "all shared non-key columns"
            if self._compare_all_columns_for_differences
            else preview_columns(self._columns_to_compare)
        )
        dataframe_column_counts = [
            len(dataframe.columns) for dataframe in self._input_dataframes
        ]
        return (
            "ColumnComparison("
            f"input_dataframes={len(self._input_dataframes)}, "
            f"dataframe_column_counts={dataframe_column_counts}, "
            f"join_key_columns={preview_columns(self._join_key_columns)}, "
            f"columns_to_compare={columns_to_compare!r}, "
            f"compare_all_schema_columns={self._compare_all_schema_columns}, "
            f"max_rows_per_column={self._max_rows_per_column}"
            f", max_rows_per_primary_key={self._max_rows_per_primary_key}"
            ")"
        )

    @property
    def input_dataframes(self) -> list[DataFrame]:
        return self._input_dataframes.copy()

    @input_dataframes.setter
    def input_dataframes(self, value: list[DataFrame]) -> None:
        if len(value) != 2:
            raise ValueError("ColumnComparison requires exactly two DataFrames.")
        self._input_dataframes = value.copy()
        self._initialize_comparison_dataframes()

    @property
    def max_rows_per_column(self) -> int:
        return self._max_rows_per_column

    @max_rows_per_column.setter
    def max_rows_per_column(self, value: int) -> None:
        if value < 1:
            raise ValueError("max_rows_per_column must be at least 1.")
        self._max_rows_per_column = value
        self._initialize_comparison_dataframes()

    @property
    def max_rows_per_primary_key(self) -> int:
        return self._max_rows_per_primary_key

    @max_rows_per_primary_key.setter
    def max_rows_per_primary_key(self, value: int) -> None:
        if value < 1:
            raise ValueError("max_rows_per_primary_key must be at least 1.")
        self._max_rows_per_primary_key = value

    def _empty_difference_analysis_df(self) -> DataFrame:
        return self._spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("column_name", StringType(), True),
                    StructField("dataframe_1_value", StringType(), True),
                    StructField("dataframe_2_value", StringType(), True),
                    StructField("match", BooleanType(), True),
                ]
            ),
        )

    def _empty_key_analysis_stats_df(self) -> DataFrame:
        return self._spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("column_pair", StringType(), True),
                    StructField("diff_count", LongType(), True),
                    StructField("match_count", LongType(), True),
                    StructField("diff_percentage", DoubleType(), True),
                    StructField("match_percentage", DoubleType(), True),
                ]
            ),
        )

    def _empty_primary_key_comparison_df(self) -> DataFrame:
        return self._spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("source", StringType(), True),
                    StructField("distinct_pk_rows", LongType(), True),
                    StructField("total_rows", LongType(), True),
                    StructField("exclusive_pks", LongType(), True),
                ]
            ),
        )

    def _empty_primary_key_exceptions_df(self) -> DataFrame:
        return self._spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("source", StringType(), True),
                    StructField("primary_key", StringType(), True),
                ]
            ),
        )

    def _initialize_comparison_dataframes(self) -> None:
        if not self._join_key_columns:
            # if join keys are not specified then we just give the schema comparison
            self.schema_comparison_df = self.compare_schemas(
                compare_all_columns=True,
            )
            self.difference_analysis_df = self._empty_difference_analysis_df()
            self.key_analysis_stats_df = self._empty_key_analysis_stats_df()
            self.primary_key_comparison_df = self._empty_primary_key_comparison_df()
            self.primary_key_exceptions_df = self._empty_primary_key_exceptions_df()
            return

        self.schema_comparison_df = self.compare_schemas(
            compare_all_columns=self._compare_all_schema_columns,
        )
        mismatched_rows_df, all_comparison_rows_df = self.difference_analysis_on_keys()
        self.key_analysis_stats_df = self.get_difference_analysis_stats(
            comparison_rows_df=all_comparison_rows_df
        )
        self.difference_analysis_df = self.truncate_by_column_limit(
            all_comparison_rows_df=mismatched_rows_df
        )
        self.primary_key_comparison_df = self.compare_pks()

    def _render_report_document(
        self,
        body_html: str,
        script_html: str = "",
    ) -> str:
        return (
            _read_report_asset("index.html")
            .replace("__STYLES__", _read_report_asset("styles.css"))
            .replace("__SCRIPT__", script_html)
            .replace("__BODY__", body_html)
        )

    def compare_pks(self) -> DataFrame:
        """
        Compares primary key stats across the input DataFrames and their inner join.

        In addition to row and distinct-key counts, the result reports the keys
        present exclusively in DataFrame 1 or DataFrame 2. Comparing a
        DataFrame's
        distinct-key count with its own row count only tests uniqueness; it does
        not test whether the two DataFrames contain the same keys.

        Args:
            self : class params

        Returns:
            A new DataFrame with schema:
                [source, distinct_pk_rows, total_rows, exclusive_pks]
        """
        self.logger.debug("compare_pks_multi called")
        key_summary_rows = []
        join_key_columns = self._join_key_columns
        old_keys_df = self._input_dataframes[0].select(join_key_columns).distinct()
        new_keys_df = self._input_dataframes[1].select(join_key_columns).distinct()

        old_only_df = old_keys_df.join(
            new_keys_df, on=join_key_columns, how="left_anti"
        )
        new_only_df = new_keys_df.join(
            old_keys_df, on=join_key_columns, how="left_anti"
        )

        def format_keys_for_display(keys_df: DataFrame, source: str) -> DataFrame:
            key_parts = [
                spark_functions.concat(
                    spark_functions.lit(f"{column_name}="),
                    spark_functions.coalesce(
                        spark_functions.col(column_name).cast("string"),
                        spark_functions.lit("NULL"),
                    ),
                )
                for column_name in join_key_columns
            ]
            return keys_df.select(
                spark_functions.lit(source).alias("source"),
                spark_functions.concat_ws("; ", *key_parts).alias("primary_key"),
            )

        self.primary_key_exceptions_df = format_keys_for_display(
            old_only_df,
            "DataFrame 1",
        ).unionByName(format_keys_for_display(new_only_df, "DataFrame 2"))

        old_only_count = old_only_df.count()
        new_only_count = new_only_df.count()

        # Get unique count and total count for each DataFrame
        self._dataset_row_counts = []
        for dataframe_index, dataframe in enumerate(self._input_dataframes):
            key_columns_df = dataframe.select(join_key_columns)
            distinct_count = key_columns_df.distinct().count()
            total_count = dataframe.count()
            self._dataset_row_counts.append(total_count)
            key_summary_rows.append(
                (
                    f"DataFrame {dataframe_index + 1}",
                    distinct_count,
                    total_count,
                    old_only_count if dataframe_index == 0 else new_only_count,
                )
            )

        # Perform inner join across all DataFrames on PK
        joined_keys_df = old_keys_df
        for dataframe in self._input_dataframes[1:]:
            joined_keys_df = joined_keys_df.join(
                dataframe.select(join_key_columns).distinct(),
                on=join_key_columns,
                how="inner",
            )

        # Get unique count and total count for the joined DataFrame
        joined_distinct_count = joined_keys_df.distinct().count()
        joined_total_count = joined_keys_df.count()
        key_summary_rows.append(
            (
                "Joined DataFrames",
                joined_distinct_count,
                joined_total_count,
                0,
            )
        )
        self.logger.debug("compare_pks_multi completed successfully")
        return self._spark.createDataFrame(
            key_summary_rows,
            [
                "source",
                "distinct_pk_rows",
                "total_rows",
                "exclusive_pks",
            ],
        )

    def compare_schemas(
        self,
        compare_all_columns: bool = False,
    ) -> DataFrame:
        """
        Compare the schemas of the two input DataFrames.

        Args:
            compare_all_columns: Whether to compare every input column instead of
                using the configured columns_to_compare and join_key_columns.

        Returns:
            A Spark DataFrame containing the schema comparison.
        """
        self.logger.debug("compare_schemas called")

        selected_columns: list[str] = []
        for column_name in [
            *self._columns_to_compare,
            *self._join_key_columns,
        ]:
            if column_name not in selected_columns:
                selected_columns.append(column_name)

        # Compare the selected columns, or every column when none were selected.
        if compare_all_columns or not selected_columns:
            dataframe_schemas = [
                dataframe.schema for dataframe in self._input_dataframes
            ]
            schema_columns = [list(schema.fieldNames()) for schema in dataframe_schemas]
        else:
            dataframe_schemas = [
                dataframe.select(*selected_columns).schema
                for dataframe in self._input_dataframes
            ]
            schema_columns = [list(schema.fieldNames()) for schema in dataframe_schemas]
        all_columns = sorted(
            {column_name for columns in schema_columns for column_name in columns}
        )
        rows = []

        for column_name in all_columns:
            schema_presence_flags = [
                column_name in columns for columns in schema_columns
            ]
            data_types = [
                str(dataframe_schemas[dataframe_index][column_name].dataType)
                if schema_presence_flags[dataframe_index]
                else ""
                for dataframe_index in range(2)
            ]
            appears_in_one_schema_only = sum(schema_presence_flags) == 1
            type_match = bool(data_types[0] and data_types[0] == data_types[1])
            rows.append(
                [
                    column_name,
                    *schema_presence_flags,
                    *data_types,
                    appears_in_one_schema_only,
                    type_match,
                ]
            )

        schema_comparison_df = self._spark.createDataFrame(
            rows,
            [
                "column_name",
                "in_dataframe_1",
                "in_dataframe_2",
                "data_type_1",
                "data_type_2",
                "appears_in_one_dataframe_only",
                "data_type_match",
            ],
        )
        self.logger.debug("compare_schemas completed successfully")
        return schema_comparison_df.orderBy("data_type_match", ascending=True)

    def difference_analysis_on_keys(self) -> tuple[DataFrame, DataFrame]:
        """
        Performs a difference analysis on the two input DataFrames by join key.

        Returns:
            2 DataFrames:
                1. mismatched_rows_df: Rows with different values across inputs.
                2. all_comparison_rows_df: All rows used to calculate statistics.
            Schema:
                [*join_key_columns, column_name, <name>_value, ...,
                <name>_value, match]
        """
        self.logger.debug("compare_dataframes_by_keys_multi_wide called")
        join_key_columns = self._join_key_columns

        # Determine the columns shared by every input DataFrame.
        if self._compare_all_columns_for_differences:
            shared_columns = list(
                set.intersection(
                    *(set(dataframe.columns) for dataframe in self._input_dataframes)
                )
            )
        else:
            selected_columns = [
                *self._columns_to_compare,
                *join_key_columns,
            ]
            shared_columns = list(
                set(selected_columns).intersection(
                    *(set(dataframe.columns) for dataframe in self._input_dataframes)
                )
            )
        comparison_value_columns = [
            column_name
            for column_name in shared_columns
            if column_name not in join_key_columns
        ]
        value_column_names = ["dataframe_1_value", "dataframe_2_value"]

        qualified_dataframes = [
            dataframe.select(
                *[
                    spark_functions.col(column_name).alias(
                        f"{column_name}_df{dataframe_index}"
                    )
                    for column_name in shared_columns
                ]
            )
            for dataframe_index, dataframe in enumerate(self._input_dataframes)
        ]

        # join them all
        def join_two(
            left_dataframe: DataFrame,
            right_dataframe: DataFrame,
            left_dataframe_index: int,
            right_dataframe_index: int,
        ) -> DataFrame:
            join_conditions = [
                left_dataframe[f"{join_key_column}_df{left_dataframe_index}"]
                == right_dataframe[f"{join_key_column}_df{right_dataframe_index}"]
                for join_key_column in join_key_columns
            ]
            return left_dataframe.join(
                right_dataframe,
                on=join_conditions,
                how="inner",
            )

        joined_comparison_df = qualified_dataframes[0]
        for dataframe_index in range(1, len(qualified_dataframes)):
            joined_comparison_df = join_two(
                joined_comparison_df,
                qualified_dataframes[dataframe_index],
                dataframe_index - 1,
                dataframe_index,
            )

        # start building the result DataFrame
        # select the join keys and the non-key columns from each DataFrame
        comparison_dataframes = []

        # iterate over each non-key column
        for column_name in comparison_value_columns:
            comparison_select_expressions = []
            # create the join key columns
            for join_key_column in join_key_columns:
                comparison_select_expressions.append(
                    spark_functions.col(f"{join_key_column}_df0").alias(join_key_column)
                )
            #  create the non-key column
            comparison_select_expressions.append(
                spark_functions.lit(column_name).alias("column_name")
            )
            # create value columns
            for dataframe_index, value_column_name in enumerate(value_column_names):
                comparison_select_expressions.append(
                    spark_functions.col(f"{column_name}_df{dataframe_index}")
                    .cast("string")
                    .alias(value_column_name)
                )
            comparison_select_expressions.append(
                spark_functions.when(
                    spark_functions.expr(f"{column_name}_df0 <=> {column_name}_df1"),
                    spark_functions.lit(True),
                )
                .otherwise(spark_functions.lit(False))
                .alias("match")
            )
            # and add them to the result DataFrame
            column_comparison_df = joined_comparison_df.select(
                *comparison_select_expressions
            )
            comparison_dataframes.append(column_comparison_df)

        comparison_rows_df = reduce(
            DataFrame.unionByName,
            comparison_dataframes,
        )

        all_comparison_rows_df = comparison_rows_df

        # filter to only mismatches across all value columns
        # build the array of values, distinct it, and require size > 1
        value_columns = [
            spark_functions.col(value_column_name)
            for value_column_name in value_column_names
        ]
        mismatch_condition = (
            spark_functions.size(
                spark_functions.array_distinct(spark_functions.array(*value_columns))
            )
            > 1
        )
        mismatched_rows_df = comparison_rows_df.filter(mismatch_condition)

        self.logger.debug("difference_analysis_on_keys completed successfully")
        return mismatched_rows_df, all_comparison_rows_df

    def display_pretty(self) -> None:
        """
        Display the initialized comparison DataFrames as an interactive HTML report.
        """
        self.logger.debug("display_pretty called")

        # if no keys provided, then just compare the schema
        if not self._join_key_columns:
            schema_only_html = self.generate_html_tables(
                input_dataframes=[self.schema_comparison_df],
                titles=["Schema Summary"],
                descriptions=["Columns and data types available in each DataFrame"],
            )
            comparison_report_html = self.generate_html_button_switch(
                view_html_fragments=[schema_only_html],
                data_snapshot_html=self.generate_html_data_snapshot(),
            )
            display(HTML(comparison_report_html))
            return

        selected_columns_schema_html = self.generate_html_tables(
            input_dataframes=[self.schema_comparison_df],
            titles=["Schema Summary"],
            descriptions=[
                "Columns and data types available in each DataFrame",
            ],
        )

        shared_primary_key_count = (
            self.primary_key_comparison_df.filter(
                spark_functions.col("source") == "Joined DataFrames"
            )
            .select("distinct_pk_rows")
            .first()[0]
        )
        value_comparison_html = self.generate_html_tables(
            input_dataframes=[
                self.key_analysis_stats_df,
                self.difference_analysis_df,
            ],
            titles=["Column Comparison Summary", "Differing Values"],
            descriptions=[
                (
                    "Value comparison for "
                    f"{shared_primary_key_count:,} primary keys shared by both "
                    "DataFrames"
                ),
                (
                    "Values that differ for shared primary keys. Keys that appear "
                    "in only one DataFrame are shown in PK Comparison"
                ),
            ],
        )

        primary_key_summary_html = self.generate_html_tables(
            input_dataframes=[self.primary_key_comparison_df],
            titles=["Primary Key Summary"],
            descriptions=[
                "Distinct primary keys, physical row counts, and keys not found in the other DataFrame",
            ],
        )
        primary_key_exceptions_html = self.generate_html_tables(
            input_dataframes=[self.truncate_primary_key_exceptions()],
            titles=["Exclusive Primary Keys"],
            descriptions=["Exact primary-key values found in only one DataFrame"],
        )
        key_uniqueness_html = primary_key_summary_html + primary_key_exceptions_html

        # Create final report views with a button switcher.
        comparison_report_html = self.generate_html_button_switch(
            view_html_fragments=[
                selected_columns_schema_html,
                value_comparison_html,
                key_uniqueness_html,
            ],
            titles=["Schema Comparison", "Column Comparison", "PK Comparison"],
            data_snapshot_html=self.generate_html_data_snapshot(),
        )
        self.logger.debug("Finished display_pretty")
        display(HTML(comparison_report_html))

    def generate_html_button_switch(
        self,
        view_html_fragments: list[str],
        titles: list[str] | None = None,
        data_snapshot_html: str = "",
    ) -> str:
        """
        Generate HTML with toggle buttons to switch between report views.
        titles and descriptions will usually be defined and not an option for the user to define.

        Args:
            view_html_fragments: HTML report-view strings from generate_html_tables.
            titles: Optional titles for each button.

        Returns:
            HTML + JavaScript for views visible by button click.
        """
        self.logger.debug(
            "Building button-switch HTML for %s views",
            len(view_html_fragments),
        )

        # Generate unique divs for each report view.
        view_divs = ""
        for view_index, view_html in enumerate(view_html_fragments):
            div_id = f"view{view_index + 1}"
            css_display = "block" if view_index == 0 else "none"
            view_divs += (
                f'<div id="{div_id}" role="tabpanel" '
                f'aria-labelledby="btn-{div_id}" style="display:{css_display};">'
                f"{view_html}</div>\n"
            )

        # JavaScript toggle logic
        view_ids = ",".join(
            f"'view{view_index + 1}'" for view_index in range(len(view_html_fragments))
        )
        script = f"""
        <script>
        function showView(id) {{
            [{view_ids}].forEach(viewId => {{
                const isActive = viewId === id;
                document.getElementById(viewId).style.display = isActive ? 'block' : 'none';
                const button = document.getElementById('btn-' + viewId);
                button.classList.toggle('active', isActive);
                button.setAttribute('aria-selected', isActive.toString());
            }});
        }}
        </script>
        """

        # Buttons
        button_html = '<div class="seg" role="tablist" aria-label="Comparison views">\n'
        for view_index in range(len(view_html_fragments)):
            label = (
                titles[view_index]
                if titles and view_index < len(titles)
                else f"View {view_index + 1}"
            )
            div_id = f"view{view_index + 1}"
            active_class = ' class="active"' if view_index == 0 else ""
            aria_selected = "true" if view_index == 0 else "false"
            button_html += (
                f'<button type="button" onclick="showView(\'{div_id}\')" '
                f'id="btn-{div_id}" role="tab" aria-controls="{div_id}" '
                f'aria-selected="{aria_selected}"{active_class}>'
                f"{escape(str(label))}</button>\n"
            )
        button_html += "</div>\n"

        report_toolbar_html = (
            f'<div class="report-toolbar">{button_html}{data_snapshot_html}</div>'
        )

        # Final HTML assembly
        full_html = self._render_report_document(
            '<div class="report-wrap">' + report_toolbar_html + view_divs + "</div>",
            script,
        )
        self.logger.debug("Finished generate_button_switch_html")
        return full_html

    def generate_html_data_snapshot(self) -> str:
        """Generate the compact dataset row-count summary shown on every tab."""
        row_counts = self._dataset_row_counts or [
            dataframe.count() for dataframe in self._input_dataframes
        ]
        snapshot_items = "".join(
            (
                '<div class="data-snapshot-item">'
                f'<span class="data-snapshot-source">DataFrame {index}</span>'
                f'<strong class="data-snapshot-value">{row_count:,}</strong>'
                '<span class="data-snapshot-unit">rows</span>'
                "</div>"
            )
            for index, row_count in enumerate(row_counts, start=1)
        )
        return (
            '<aside class="data-snapshot" aria-label="Dataset snapshot">'
            '<span class="data-snapshot-title">Dataset snapshot</span>'
            f'<div class="data-snapshot-items">{snapshot_items}</div>'
            "</aside>"
        )

    def generate_html_tables(
        self,
        input_dataframes: list[DataFrame],
        titles: list[str] | None = None,
        descriptions: list[str] | None = None,
    ) -> str:
        """
        Generates HTML fragments for a list of Spark DataFrames.
        The output HTML is used as input for the interactive report views.
        titles and descriptions will usually be defined and not an option for the user to define.

        Args:
            input_dataframes: Spark DataFrames containing report data.
            titles: Optional titles for each report section.
            descriptions: Optional descriptions to display below each section.

        Returns:
            HTML fragments for the generated comparison report sections.
        """
        self.logger.debug(
            "Generating HTML for %s report sections",
            len(input_dataframes),
        )
        all_tables_html = ""
        # Iterate through each DataFrame and generate report HTML.
        for table_index, dataframe in enumerate(input_dataframes):
            column_names = dataframe.columns
            rows = dataframe.collect()

            schema_presence_columns = [
                column_name
                for column_name in column_names
                if column_name.startswith("in_dataframe_")
            ]
            match_columns = [
                column_name
                for column_name in column_names
                if column_name == "match"
                or column_name in {"column_name_match", "data_type_match"}
            ]
            value_columns = [
                column_name
                for column_name in column_names
                if column_name in {"dataframe_1_value", "dataframe_2_value"}
            ]
            is_schema_table = bool(schema_presence_columns) or {
                "column_name_match",
                "data_type_match",
            }.issubset(column_names)
            is_pk_table = {
                "distinct_pk_rows",
                "total_rows",
            }.issubset(column_names)
            is_exclusive_pk_table = {"source", "primary_key"}.issubset(column_names)
            is_value_table = bool(value_columns and match_columns)
            group_column = (
                "source"
                if is_exclusive_pk_table
                else "column_name"
                if is_value_table and "column_name" in column_names
                else None
            )
            has_status = is_schema_table or is_value_table or is_pk_table

            status_source_columns = set(match_columns)
            if is_schema_table and "appears_in_one_dataframe_only" in column_names:
                status_source_columns.add("appears_in_one_dataframe_only")
            display_columns = [
                column_name
                for column_name in column_names
                if column_name not in status_source_columns
            ]

            title = titles[table_index] if titles and table_index < len(titles) else ""
            description = (
                descriptions[table_index]
                if descriptions and table_index < len(descriptions)
                else ""
            )

            title_html = f"<h4>{escape(title)}</h4>" if title else ""
            description_html = (
                f'<p class="section-desc">{escape(description)}</p>'
                if description
                else ""
            )

            def display_header(column_name: str) -> str:
                header_mapping = {
                    "column_name": "Column",
                    "column_pair": "Column",
                    "position": "Position",
                    "diff_count": "Different values",
                    "match_count": "Matching values",
                    "diff_percentage": "Different %",
                    "match_percentage": "Matching %",
                    "dataframe_1_value": "DataFrame 1 value",
                    "dataframe_2_value": "DataFrame 2 value",
                    "distinct_pk_rows": "Distinct PKs",
                    "total_rows": "Total rows",
                    "exclusive_pks": "Exclusive PKs",
                    "source": "DataFrame",
                    "primary_key": "Primary key",
                }
                if column_name in header_mapping:
                    return header_mapping[column_name]
                if column_name.startswith("in_dataframe_"):
                    dataframe_number = column_name.removeprefix("in_dataframe_")
                    return f"In DataFrame {dataframe_number}"
                if column_name.startswith("data_type_"):
                    dataframe_number = column_name.removeprefix("data_type_")
                    return f"Type — DataFrame {dataframe_number}"
                if column_name.startswith("dataframe_") and column_name.endswith(
                    "_column"
                ):
                    dataframe_number = column_name.removeprefix(
                        "dataframe_"
                    ).removesuffix("_column")
                    return f"Column — DataFrame {dataframe_number}"
                if column_name.startswith("dataframe_") and column_name.endswith(
                    "_type"
                ):
                    dataframe_number = column_name.removeprefix(
                        "dataframe_"
                    ).removesuffix("_type")
                    return f"Type — DataFrame {dataframe_number}"
                return column_name.replace("_", " ")

            # Column names for the report header.
            header_columns = display_columns + (["Status"] if has_status else [])
            header_html = (
                "<thead><tr>"
                + "".join(
                    f"<th>{escape(display_header(column_name))}</th>"
                    for column_name in header_columns
                )
                + "</tr></thead>"
            )

            # Create the report body.
            body_html = ""
            previous_group_value = None
            previous_schema_group = None
            for row in rows:
                row_values: dict[str, object] = dict(
                    zip(column_names, row, strict=True)
                )
                status_label = ""

                if is_schema_table:
                    if len(schema_presence_columns) >= 2:
                        dataframe_1_present = (
                            row_values[schema_presence_columns[0]] is True
                        )
                        dataframe_2_present = (
                            row_values[schema_presence_columns[1]] is True
                        )
                        if not dataframe_1_present and dataframe_2_present:
                            status_label = "added"
                        elif dataframe_1_present and not dataframe_2_present:
                            status_label = "removed"

                    if not status_label:
                        status_label = "match"
                        if match_columns and not all(
                            value is True or str(value).lower() == "true"
                            for value in (
                                row_values[column] for column in match_columns
                            )
                        ):
                            status_label = "differs"
                elif is_value_table:
                    status_label = (
                        "match"
                        if all(
                            value is True or str(value).lower() == "true"
                            for value in (
                                row_values[column] for column in match_columns
                            )
                        )
                        else "differs"
                    )
                elif is_pk_table:
                    status_label = (
                        "differs"
                        if (row_values.get("exclusive_pks", 0) or 0) > 0
                        else "match"
                    )

                highlighted_columns = set()
                if status_label and status_label != "match":
                    highlighted_columns.update(
                        column_name
                        for column_name in (
                            "dataframe_2_column",
                            "dataframe_2_type",
                            "dataframe_2_value",
                        )
                        if column_name in row_values
                    )

                row_classes = []
                if (
                    group_column is not None
                    and previous_group_value is not None
                    and row_values[group_column] != previous_group_value
                ):
                    row_classes.append("dgrid-group-start")
                previous_group_value = row_values.get(group_column)
                if is_schema_table:
                    schema_group = "match" if status_label == "match" else "difference"
                    if (
                        previous_schema_group == "difference"
                        and schema_group == "match"
                    ):
                        row_classes.append("dgrid-group-start")
                    previous_schema_group = schema_group
                row_class_attribute = (
                    f' class="{" ".join(row_classes)}"' if row_classes else ""
                )
                row_html = f"<tr{row_class_attribute}>"
                for column_name in display_columns:
                    css_classes = []
                    if column_name in highlighted_columns:
                        css_classes.append("dgrid-hl")
                    if column_name in {
                        "dataframe_1_column",
                        "dataframe_1_type",
                        "dataframe_1_value",
                    }:
                        css_classes.append("dgrid-old")
                    if column_name == "diff_percentage":
                        css_classes.append("dgrid-percent-diff")
                    elif column_name == "match_percentage":
                        css_classes.append("dgrid-percent-match")

                    value: object = row_values[column_name]
                    if column_name in schema_presence_columns:
                        display_value = "yes" if value is True else "—"
                    elif value is None or value == "":
                        display_value = "—"
                    elif column_name.endswith("_percentage"):
                        display_value = f"{float(value):.1f}%"
                    elif (
                        "count" in column_name.lower() or "rows" in column_name.lower()
                    ):
                        display_value = f"{int(value):,}"
                    else:
                        display_value = str(value)

                    class_attribute = (
                        f' class="{" ".join(css_classes)}"' if css_classes else ""
                    )
                    row_html += f"<td{class_attribute}>{escape(display_value)}</td>"

                if has_status:
                    status_class = (
                        "dgrid-status-match"
                        if status_label == "match"
                        else "dgrid-status-diff"
                    )
                    row_html += (
                        '<td><span class="dgrid-status '
                        f'{status_class}">● {status_label}</span></td>'
                    )
                row_html += "</tr>"
                body_html += row_html

            section_classes = ["report-section"]
            if title in {
                "Schema Summary",
                "Column Comparison Summary",
                "Primary Key Summary",
            }:
                section_classes.append("report-section-summary")
            table_html = (
                f'<section class="{" ".join(section_classes)}">'
                f"{title_html}{description_html}"
                f'<div class="dgrid-wrap"><table class="dgrid">{header_html}'
                f"<tbody>{body_html}</tbody></table></div></section>"
            )
            all_tables_html += table_html

        self.logger.debug("Finished generate_html_tables")

        return all_tables_html

    def get_difference_analysis_stats(
        self,
        comparison_rows_df: DataFrame,
    ) -> DataFrame:
        """
        Calculate difference and match statistics for each compared column.

        Args:
            comparison_rows_df: The complete comparison output returned by
                difference_analysis_on_keys.

        Returns:
            Summary per column_name with columns:
                ["column_pair", "diff_count", "match_count",
                "diff_percentage", "match_percentage"]
        """
        self.logger.debug("get_difference_analysis_stats called")
        # Identify all match-flag columns
        match_flag_columns = [
            column_name
            for column_name in comparison_rows_df.columns
            if column_name == "match"
        ]

        # Convert boolean flags to integers and sum per column_name
        # row_count: how many rows per column
        # *_sum: total number of True flags per pair
        aggregation_expressions = [spark_functions.count("*").alias("row_count")] + [
            spark_functions.sum(
                spark_functions.col(match_flag_column).cast("int")
            ).alias(f"{match_flag_column}_sum")
            for match_flag_column in match_flag_columns
        ]
        column_statistics_df = comparison_rows_df.groupBy("column_name").agg(
            *aggregation_expressions
        )

        comparison_flag_count = len(match_flag_columns)

        # Sum all match flags to get total match_count across all pairs
        match_sum_expressions = [
            spark_functions.col(f"{match_flag_column}_sum")
            for match_flag_column in match_flag_columns
        ]
        # start from 0 so reduce([], 0) == 0 instead of error
        column_statistics_df = column_statistics_df.withColumn(
            "match_count",
            reduce(
                lambda left_expression, right_expression: (
                    left_expression + right_expression
                ),
                match_sum_expressions,
                spark_functions.lit(0),
            ),
        )

        # Compute total comparisons and diff_count
        column_statistics_df = column_statistics_df.withColumn(
            "total_comparisons",
            spark_functions.col("row_count")
            * spark_functions.lit(comparison_flag_count),
        ).withColumn(
            "diff_count",
            spark_functions.col("total_comparisons")
            - spark_functions.col("match_count"),
        )

        # Compute percentages
        column_statistics_df = column_statistics_df.withColumn(
            "diff_percentage",
            (
                spark_functions.col("diff_count")
                / spark_functions.col("total_comparisons")
            )
            * 100,
        ).withColumn(
            "match_percentage",
            (
                spark_functions.col("match_count")
                / spark_functions.col("total_comparisons")
            )
            * 100,
        )

        column_pair_expression = spark_functions.col("column_name").alias("column_pair")

        # Final select and rename
        self.logger.debug("get_difference_analysis_stats completed successfully")
        return column_statistics_df.select(
            column_pair_expression,
            "diff_count",
            "match_count",
            "diff_percentage",
            "match_percentage",
        )

    def truncate_by_column_limit(
        self,
        all_comparison_rows_df: DataFrame,
    ) -> DataFrame:
        """
        Limit the displayed comparison rows for each value of column_name.

        Args:
            all_comparison_rows_df: Comparison rows to truncate.

        Returns:
            A DataFrame containing limited rows and ellipsis markers where needed.
        """
        self.logger.debug("truncate_by_column_limit called")
        # Cast to string so the ellipsis marker can go into any column.
        string_rows_df = all_comparison_rows_df.select(
            *[
                spark_functions.col(column_name).cast("string").alias(column_name)
                for column_name in all_comparison_rows_df.columns
            ]
        )

        order_columns = [
            spark_functions.col(column_name).asc_nulls_last()
            for column_name in self._join_key_columns
        ]
        ranking_window = Window.partitionBy("column_name").orderBy(*order_columns)
        count_window = Window.partitionBy("column_name")
        ranked_rows_df = string_rows_df.withColumn(
            "_display_rank",
            spark_functions.row_number().over(ranking_window),
        ).withColumn("_column_row_count", spark_functions.count("*").over(count_window))

        limited_rows_df = ranked_rows_df.filter(
            spark_functions.col("_display_rank") <= self._max_rows_per_column
        ).select(
            *all_comparison_rows_df.columns,
            spark_functions.col("_display_rank").alias("_display_order"),
        )
        ellipsis_rows_df = ranked_rows_df.filter(
            (spark_functions.col("_display_rank") == 1)
            & (spark_functions.col("_column_row_count") > self._max_rows_per_column)
        ).select(
            *[
                (
                    spark_functions.col("column_name")
                    if output_column == "column_name"
                    else spark_functions.lit("…")
                ).alias(output_column)
                for output_column in all_comparison_rows_df.columns
            ],
            spark_functions.lit(self._max_rows_per_column + 1).alias("_display_order"),
        )

        self.logger.debug("truncate_by_column_limit completed successfully")
        return (
            limited_rows_df.unionByName(ellipsis_rows_df)
            .orderBy(
                "column_name",
                "_display_order",
            )
            .drop("_display_order")
        )

    def truncate_primary_key_exceptions(self) -> DataFrame:
        """Limit displayed exclusive primary keys without collecting them to Python."""
        ranking_window = Window.partitionBy("source").orderBy("primary_key")
        count_window = Window.partitionBy("source")
        ranked_exceptions_df = self.primary_key_exceptions_df.withColumn(
            "_display_rank",
            spark_functions.row_number().over(ranking_window),
        ).withColumn("_source_row_count", spark_functions.count("*").over(count_window))
        limited_exceptions_df = ranked_exceptions_df.filter(
            spark_functions.col("_display_rank") <= self._max_rows_per_primary_key
        ).select(
            "source",
            "primary_key",
            spark_functions.col("_display_rank").alias("_display_order"),
        )
        ellipsis_exceptions_df = ranked_exceptions_df.filter(
            (spark_functions.col("_display_rank") == 1)
            & (
                spark_functions.col("_source_row_count")
                > self._max_rows_per_primary_key
            )
        ).select(
            "source",
            spark_functions.concat(
                spark_functions.lit("… "),
                (
                    spark_functions.col("_source_row_count")
                    - self._max_rows_per_primary_key
                ).cast("string"),
                spark_functions.lit(" more primary keys"),
            ).alias("primary_key"),
            spark_functions.lit(self._max_rows_per_primary_key + 1).alias(
                "_display_order"
            ),
        )
        return (
            limited_exceptions_df.unionByName(ellipsis_exceptions_df)
            .orderBy(
                "source",
                "_display_order",
            )
            .drop("_display_order")
        )

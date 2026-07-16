import logging
from html import escape
from functools import lru_cache, reduce
from itertools import combinations
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
        include_pairwise_comparisons: Whether to add a match flag for every
            pair of DataFrames in addition to the all-DataFrame match flag.
        columns_to_compare: Non-key columns whose values should be compared.
        logger_level: Logging level for the logger.

    Applicable comparison DataFrames are generated during initialization and
    exposed through read-only properties. Call ``display_pretty()`` to render
    those stored DataFrames as an interactive HTML report.
    """

    def __init__(
        self,
        input_dataframes: list[DataFrame],
        join_key_columns: list[str] | None = None,
        include_pairwise_comparisons: bool = False,
        columns_to_compare: list[str] | None = None,
        max_rows_per_column: int = 4,
        logger_level: int = logging.INFO,
    ) -> None:
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.logger.setLevel(logger_level)
        self.logger.debug("Initializing ColumnComparison")
        self.logger.debug(
            "input_dataframes count=%s, join_key_columns=%s, "
            "include_pairwise_comparisons=%s, columns_to_compare=%s, "
            "max_rows_per_column=%s",
            len(input_dataframes),
            join_key_columns,
            include_pairwise_comparisons,
            columns_to_compare,
            max_rows_per_column,
        )
        self._join_key_columns: list[str] = join_key_columns or []
        self._include_pairwise_comparisons: bool = include_pairwise_comparisons
        self._columns_to_compare: list[str] = columns_to_compare or []
        self._compare_all_columns_for_differences = columns_to_compare is None
        spark_session: SparkSession | None = SparkSession.getActiveSession()
        if spark_session is None:
            spark_session = SparkSession.builder.getOrCreate()
        if not isinstance(spark_session, SparkSession):
            raise RuntimeError("A Spark session is required for column comparison.")
        self._spark: SparkSession = spark_session
        self._max_rows_per_column = self._validate_max_rows_per_column(
            max_rows_per_column
        )

        self._schema_comparison_df: DataFrame
        self._difference_analysis_df: DataFrame
        self._key_analysis_stats_df: DataFrame
        self._primary_key_comparison_df: DataFrame

        self._set_input_dataframes(input_dataframes)
        self._initialize_comparison_dataframes()

    @property
    def input_dataframes(self) -> list[DataFrame]:
        return self._input_dataframes.copy()

    @input_dataframes.setter
    def input_dataframes(self, value: list[DataFrame]) -> None:
        self._set_input_dataframes(value)
        if hasattr(self, "_spark"):
            self._initialize_comparison_dataframes()

    def _set_input_dataframes(self, value: list[DataFrame]) -> None:
        if len(value) != 2:
            raise ValueError("ColumnComparison requires exactly two DataFrames.")
        self._input_dataframes = value.copy()

    @property
    def max_rows_per_column(self) -> int:
        return self._max_rows_per_column

    @max_rows_per_column.setter
    def max_rows_per_column(self, value: int) -> None:
        self._max_rows_per_column = self._validate_max_rows_per_column(value)
        if hasattr(self, "_spark"):
            self._initialize_comparison_dataframes()

    def _validate_max_rows_per_column(self, value: int) -> int:
        if not isinstance(value, int):
            raise TypeError("max_rows_per_column must be an integer.")
        if value < 1:
            raise ValueError("max_rows_per_column must be at least 1.")
        return value

    def _initialize_comparison_dataframes(self) -> None:
        if not self._join_key_columns:
            self._schema_comparison_df = self.compare_schemas(
                respect_column_order=False,
                compare_all_columns=True,
            )
            self._difference_analysis_df = self._empty_difference_analysis_df()
            self._key_analysis_stats_df = self._empty_key_analysis_stats_df()
            self._primary_key_comparison_df = self._empty_primary_key_comparison_df()
            return

        self._schema_comparison_df = self.compare_schemas(respect_column_order=False)
        mismatched_rows_df, all_comparison_rows_df = self.difference_analysis_on_keys()
        self._key_analysis_stats_df = self.get_difference_analysis_stats(
            comparison_rows_df=all_comparison_rows_df
        )
        self._difference_analysis_df = self.truncate_by_column_limit(
            all_comparison_rows_df=mismatched_rows_df
        )
        self._primary_key_comparison_df = self.compare_pks()

    @property
    def schema_comparison_df(self) -> DataFrame:
        return self._schema_comparison_df

    @property
    def difference_analysis_df(self) -> DataFrame:
        return self._difference_analysis_df

    @property
    def key_analysis_stats_df(self) -> DataFrame:
        return self._key_analysis_stats_df

    @property
    def primary_key_comparison_df(self) -> DataFrame:
        return self._primary_key_comparison_df

    def _empty_difference_analysis_df(self) -> DataFrame:
        return self._spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("column_name", StringType(), True),
                    StructField("df0_value", StringType(), True),
                    StructField("df1_value", StringType(), True),
                    StructField("column_match_0_1", BooleanType(), True),
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
                    StructField("Source", StringType(), True),
                    StructField("Distinct PK Rows", LongType(), True),
                    StructField("Total Rows", LongType(), True),
                ]
            ),
        )

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

    def generate_html_tables(
        self,
        input_dataframes: list[DataFrame],
        titles: list[str] | None = None,
        descriptions: list[str] | None = None,
    ) -> str:
        """
        Generates HTML fragments for a list of Spark DataFrames.
        The outputed HTML for this will be used as input for the interactive table with buttons.
        titles and descriptions will usually be defined and not an option for the user to define.

        Args:
            input_dataframes: Spark DataFrames containing report table data.
            titles: Optional titles for each table.
            descriptions: Optional descriptions to display below each table.

        Returns:
            HTML fragments for the generated comparison tables.
        """
        self.logger.debug(
            "Generating HTML for %s tables",
            len(input_dataframes),
        )
        all_tables_html = ""
        # Iterate through each DataFrame and generate HTML
        for table_index, dataframe in enumerate(input_dataframes):
            column_names = dataframe.columns
            rows = dataframe.collect()

            schema_presence_columns = [
                column_name
                for column_name in column_names
                if column_name.startswith("In Schema ")
            ]
            match_columns = [
                column_name
                for column_name in column_names
                if column_name.startswith("column_match_")
                or column_name in {"Match", "Type Match"}
            ]
            value_columns = [
                column_name
                for column_name in column_names
                if column_name.startswith("df") and column_name.endswith("_value")
            ]
            is_schema_table = bool(schema_presence_columns) or (
                "Match" in column_names
                and any(
                    column_name.startswith("Schema ") for column_name in column_names
                )
            )
            is_value_table = bool(value_columns and match_columns)
            is_pk_table = {"Distinct PK Rows", "Total Rows"}.issubset(column_names)
            has_status = is_schema_table or is_value_table or is_pk_table

            status_source_columns = set(match_columns)
            if is_schema_table and "Appears In One Schema Only" in column_names:
                status_source_columns.add("Appears In One Schema Only")
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
                    "Column": "Column",
                    "Schema 1": "Column — old",
                    "Schema 2": "Column — new",
                    "Type 1": "Type — old",
                    "Type 2": "Type — new",
                    "column_name": "Column",
                    "df0_value": "Old value",
                    "df1_value": "New value",
                    "column_pair": "Column",
                    "diff_count": "Diff count",
                    "match_count": "Match count",
                    "diff_percentage": "Diff %",
                    "match_percentage": "Match %",
                }
                if column_name in header_mapping:
                    return header_mapping[column_name]
                if column_name.startswith("In Schema "):
                    schema_number = column_name.removeprefix("In Schema ")
                    if schema_number == "1":
                        return "In old"
                    if schema_number == "2":
                        return "In new"
                    return f"In table {schema_number}"
                if column_name.startswith("Type "):
                    table_number = column_name.removeprefix("Type ")
                    return f"Type — table {table_number}"
                if column_name.startswith("df") and column_name.endswith("_value"):
                    table_number = column_name.removeprefix("df").removesuffix("_value")
                    return f"Value — table {int(table_number) + 1}"
                return column_name.replace("_", " ")

            # column names for top row of table
            header_columns = display_columns + (["Status"] if has_status else [])
            header_html = (
                "<thead><tr>"
                + "".join(
                    f"<th>{escape(display_header(column_name))}</th>"
                    for column_name in header_columns
                )
                + "</tr></thead>"
            )

            # create the body of the table
            body_html = ""
            for row in rows:
                row_values = dict(zip(column_names, row, strict=True))
                status_label = ""

                if is_schema_table:
                    if len(schema_presence_columns) >= 2:
                        old_present = row_values[schema_presence_columns[0]] is True
                        new_present = row_values[schema_presence_columns[1]] is True
                        if not old_present and new_present:
                            status_label = "added"
                        elif old_present and not new_present:
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
                        "match"
                        if row_values["Distinct PK Rows"] == row_values["Total Rows"]
                        else "differs"
                    )

                highlighted_columns = set()
                if status_label and status_label != "match":
                    highlighted_columns.update(
                        column_name
                        for column_name in ("Schema 2", "Type 2", "df1_value")
                        if column_name in row_values
                    )

                row_html = "<tr>"
                for column_name in display_columns:
                    css_classes = []
                    if column_name in highlighted_columns:
                        css_classes.append("dgrid-hl")
                    if column_name in {"Schema 1", "Type 1", "df0_value"}:
                        css_classes.append("dgrid-old")
                    if column_name == "diff_percentage":
                        css_classes.append("dgrid-percent-diff")
                    elif column_name == "match_percentage":
                        css_classes.append("dgrid-percent-match")

                    value = row_values[column_name]
                    if column_name in schema_presence_columns:
                        display_value = "yes" if value is True else "—"
                    elif value is None or value == "":
                        display_value = "—"
                    elif column_name.endswith("_percentage") and isinstance(
                        value, (int, float)
                    ):
                        display_value = f"{value:.1f}%"
                    elif isinstance(value, int) and (
                        "count" in column_name.lower() or "rows" in column_name.lower()
                    ):
                        display_value = f"{value:,}"
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

            table_html = (
                f'<section class="report-section">{title_html}{description_html}'
                f'<div class="dgrid-wrap"><table class="dgrid">{header_html}'
                f"<tbody>{body_html}</tbody></table></div></section>"
            )
            all_tables_html += table_html

        self.logger.debug("Finished generate_html_tables")

        return all_tables_html

    def generate_html_button_switch(
        self,
        table_html_fragments: list[str],
        titles: list[str] | None = None,
    ) -> str:
        """
        Generates HTML with toggle buttons to switch between multiple tables given input html tables from generate_html_tables.
        titles and descriptions will usually be defined and not an option for the user to define.

        Args:
            table_html_fragments: HTML table strings from generate_html_tables.
            titles: Optional titles for each button.

        Returns:
            HTML + javascript for tables visible by button click
        """
        self.logger.debug(
            "Building button-switch HTML for %s tables",
            len(table_html_fragments),
        )

        # Generate unique divs for each table
        table_divs = ""
        for table_index, table_html in enumerate(table_html_fragments):
            div_id = f"table{table_index + 1}"
            css_display = "block" if table_index == 0 else "none"
            table_divs += (
                f'<div id="{div_id}" role="tabpanel" '
                f'aria-labelledby="btn-{div_id}" style="display:{css_display};">'
                f"{table_html}</div>\n"
            )

        # JavaScript toggle logic
        table_ids = ",".join(
            f"'table{table_index + 1}'"
            for table_index in range(len(table_html_fragments))
        )
        script = f"""
        <script>
        function showTable(id) {{
            [{table_ids}].forEach(tableId => {{
                const isActive = tableId === id;
                document.getElementById(tableId).style.display = isActive ? 'block' : 'none';
                const button = document.getElementById('btn-' + tableId);
                button.classList.toggle('active', isActive);
                button.setAttribute('aria-selected', isActive.toString());
            }});
        }}
        </script>
        """

        # Buttons
        button_html = '<div class="seg" role="tablist" aria-label="Comparison views">\n'
        for table_index in range(len(table_html_fragments)):
            label = (
                titles[table_index]
                if titles and table_index < len(titles)
                else f"Table {table_index + 1}"
            )
            div_id = f"table{table_index + 1}"
            active_class = ' class="active"' if table_index == 0 else ""
            aria_selected = "true" if table_index == 0 else "false"
            button_html += (
                f'<button type="button" onclick="showTable(\'{div_id}\')" '
                f'id="btn-{div_id}" role="tab" aria-controls="{div_id}" '
                f'aria-selected="{aria_selected}"{active_class}>'
                f"{escape(str(label))}</button>\n"
            )
        button_html += "</div>\n"

        # Final HTML assembly
        full_html = self._render_report_document(
            '<div class="report-wrap">' + button_html + table_divs + "</div>",
            script,
        )
        self.logger.debug("Finished generate_button_switch_html")
        return full_html

    def compare_schemas(
        self,
        respect_column_order: bool = False,
        compare_all_columns: bool = False,
    ) -> DataFrame:
        """
        Compare schemas of multiple DataFrames and return the result.

        Args:
            respect_column_order: Whether column position is part of the comparison.
            compare_all_columns: Whether to compare every input column instead of
                using the configured columns_to_compare and join_key_columns.

        Returns:
            A Spark DataFrame containing the schema comparison.
        """
        self.logger.debug("compare_schemas called")

        dataframe_count = len(self._input_dataframes)

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
        # Check if all DataFrames have the same schema
        if respect_column_order:
            # Build rows based on column position
            maximum_column_count = max(len(columns) for columns in schema_columns)
            rows: list[list[object]] = []
            for position in range(maximum_column_count):
                column_names_at_position = [
                    schema_columns[dataframe_index][position]
                    if position < len(schema_columns[dataframe_index])
                    else ""
                    for dataframe_index in range(dataframe_count)
                ]
                data_types_at_position = [
                    str(
                        dataframe_schemas[dataframe_index][
                            column_names_at_position[dataframe_index]
                        ].dataType
                    )
                    if column_names_at_position[dataframe_index]
                    else ""
                    for dataframe_index in range(dataframe_count)
                ]
                column_name_match = bool(
                    column_names_at_position[0]
                    and all(
                        column_name == column_names_at_position[0]
                        for column_name in column_names_at_position
                    )
                )
                type_match = bool(
                    data_types_at_position[0]
                    and all(
                        data_type == data_types_at_position[0]
                        for data_type in data_types_at_position
                    )
                )
                schema_row: list[object] = [position + 1]
                # Add the schema name and type for each input DataFrame.
                for column_name, data_type in zip(
                    column_names_at_position,
                    data_types_at_position,
                    strict=True,
                ):
                    schema_row.extend([column_name, data_type])
                schema_row.extend([column_name_match, type_match])
                rows.append(schema_row)

            # Construct headers dynamically
            headers = ["Position"]
            for dataframe_number in range(1, dataframe_count + 1):
                headers.extend(
                    [
                        f"Schema {dataframe_number}",
                        f"Type {dataframe_number}",
                    ]
                )
            headers.extend(["Match", "Type Match"])
            schema_comparison_df = self._spark.createDataFrame(rows, headers)

        else:
            # Unordered schema comparison
            # This seems more useful for comparing schemas

            # Get all column names across all DataFrames
            all_columns = sorted(
                {column_name for columns in schema_columns for column_name in columns}
            )
            rows = []

            # Iterate through all columns and check their presence, types and uniqueness
            for column_name in all_columns:
                schema_presence_flags = [
                    column_name in columns for columns in schema_columns
                ]
                data_types = [
                    str(dataframe_schemas[dataframe_index][column_name].dataType)
                    if schema_presence_flags[dataframe_index]
                    else ""
                    for dataframe_index in range(dataframe_count)
                ]
                appears_in_one_schema_only = sum(schema_presence_flags) == 1
                type_match = bool(
                    data_types[0]
                    and all(data_type == data_types[0] for data_type in data_types)
                )
                comparison_row: list[object] = [column_name]
                comparison_row.extend(schema_presence_flags)
                comparison_row.extend(data_types)
                comparison_row.extend([appears_in_one_schema_only, type_match])
                rows.append(comparison_row)

            headers = ["Column"]
            headers.extend(
                f"In Schema {dataframe_number}"
                for dataframe_number in range(1, dataframe_count + 1)
            )
            headers.extend(
                f"Type {dataframe_number}"
                for dataframe_number in range(1, dataframe_count + 1)
            )
            headers.extend(["Appears In One Schema Only", "Type Match"])
            schema_comparison_df = self._spark.createDataFrame(rows, headers)
        self.logger.debug("compare_schemas completed successfully")
        return schema_comparison_df.orderBy("Type Match", ascending=True)

    def compare_pks(self) -> DataFrame:
        """
        Compares primary key stats (distinct and total counts) across multiple DataFrames
        and their inner join.

        Args:
            self : class params

        Returns:
            A new table created from input tables with schema:
                [Source, Distinct PK Rows, Total Rows]
        """
        self.logger.debug("compare_pks_multi called")
        key_summary_rows = []
        join_key_columns = self._join_key_columns

        # Get unique count and total count for each DataFrame
        for dataframe_index, dataframe in enumerate(self._input_dataframes):
            key_columns_df = dataframe.select(join_key_columns)
            distinct_count = key_columns_df.distinct().count()
            total_count = dataframe.count()
            key_summary_rows.append(
                (
                    f"DataFrame_{dataframe_index + 1}",
                    distinct_count,
                    total_count,
                )
            )

        # Perform inner join across all DataFrames on PK
        joined_keys_df = self._input_dataframes[0].select(join_key_columns)
        for dataframe in self._input_dataframes[1:]:
            joined_keys_df = joined_keys_df.join(
                dataframe.select(join_key_columns),
                on=join_key_columns,
                how="inner",
            )

        # Get unique count and total count for the joined DataFrame
        joined_distinct_count = joined_keys_df.distinct().count()
        joined_total_count = joined_keys_df.count()
        key_summary_rows.append(
            ("Joined Tables", joined_distinct_count, joined_total_count)
        )
        self.logger.debug("compare_pks_multi completed successfully")
        return self._spark.createDataFrame(
            key_summary_rows,
            ["Source", "Distinct PK Rows", "Total Rows"],
        )

    def difference_analysis_on_keys(self) -> tuple[DataFrame, DataFrame]:
        """
        Performs a difference analysis on multiple DataFrames based on specified join keys.

        Returns:
            2 DataFrames:
                1. mismatched_rows_df: Rows with different values across inputs.
                2. all_comparison_rows_df: All rows used to calculate statistics.
            Schema:
                [*join_key_columns, column_name, df0_value, ...,
                df{N-1}_value, column_match_i_j_..._N]
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
        dataframe_count = len(self._input_dataframes)
        dataframe_index_pairs = list(combinations(range(dataframe_count), 2))
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
            # create dfX_value column
            for dataframe_index in range(dataframe_count):
                comparison_select_expressions.append(
                    spark_functions.col(f"{column_name}_df{dataframe_index}")
                    .cast("string")
                    .alias(f"df{dataframe_index}_value")
                )
            # Add pairwise flags only when they differ from the all-input flag.
            if self._include_pairwise_comparisons and dataframe_count > 2:
                for (
                    left_dataframe_index,
                    right_dataframe_index,
                ) in dataframe_index_pairs:
                    comparison_select_expressions.append(
                        spark_functions.when(
                            spark_functions.expr(
                                f"{column_name}_df{left_dataframe_index} <=> "
                                f"{column_name}_df{right_dataframe_index}"
                            ),
                            spark_functions.lit(True),
                        )
                        .otherwise(spark_functions.lit(False))
                        .alias(
                            "column_match_"
                            f"{left_dataframe_index}_{right_dataframe_index}"
                        )
                    )

            # check if all columns match across all DataFrames
            all_values_match_condition = " AND ".join(
                f"{column_name}_df0 <=> {column_name}_df{dataframe_index}"
                for dataframe_index in range(1, dataframe_count)
            )
            dataframe_index_suffix = "_".join(
                str(dataframe_index) for dataframe_index in range(dataframe_count)
            )
            comparison_select_expressions.append(
                spark_functions.when(
                    spark_functions.expr(all_values_match_condition),
                    spark_functions.lit(True),
                )
                .otherwise(spark_functions.lit(False))
                .alias(f"column_match_{dataframe_index_suffix}")
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

        all_comparison_rows_df = comparison_rows_df.orderBy("column_name")

        # filter to only mismatches across all df?_value columns
        # build the array of values, distinct it, and require size > 1
        value_columns = [
            spark_functions.col(f"df{dataframe_index}_value")
            for dataframe_index in range(dataframe_count)
        ]
        mismatch_condition = (
            spark_functions.size(
                spark_functions.array_distinct(spark_functions.array(*value_columns))
            )
            > 1
        )
        mismatched_rows_df = comparison_rows_df.filter(mismatch_condition).orderBy(
            "column_name"
        )

        self.logger.debug("difference_analysis_on_keys completed successfully")
        return mismatched_rows_df, all_comparison_rows_df

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
            if column_name.startswith("column_match_")
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

        # Build column_pair including DataFrame suffixes (e.g. "purchase_date_df0_df1_df2")
        dataframe_indices = sorted(
            {
                int(dataframe_index)
                for match_flag_column in match_flag_columns
                for dataframe_index in match_flag_column.split("_")[2:]
            }
        )
        dataframe_suffixes = [
            spark_functions.lit(f"df{dataframe_index}")
            for dataframe_index in dataframe_indices
        ]
        column_pair_expression = spark_functions.concat_ws(
            "_",
            spark_functions.col("column_name"),
            *dataframe_suffixes,
        ).alias("column_pair")

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

        comparison_column_names = [
            row["column_name"]
            for row in string_rows_df.select("column_name")
            .distinct()
            .orderBy("column_name")
            .collect()
        ]

        count_rows = string_rows_df.groupBy("column_name").count().collect()
        row_count_by_column = {row["column_name"]: row["count"] for row in count_rows}

        truncated_column_dataframes = []
        for display_index, column_name in enumerate(comparison_column_names):
            column_rows_df = string_rows_df.filter(
                spark_functions.col("column_name") == column_name
            )

            limited_rows_df = column_rows_df.limit(
                self._max_rows_per_column
            ).withColumn(
                "display_order",
                spark_functions.lit(display_index * 2),
            )
            truncated_column_dataframes.append(limited_rows_df)

            if row_count_by_column.get(column_name, 0) > self._max_rows_per_column:
                ellipsis_row_df = (
                    column_rows_df.limit(1)
                    .select(
                        *[
                            spark_functions.lit("…").alias(output_column)
                            for output_column in all_comparison_rows_df.columns
                        ]
                    )
                    .withColumn(
                        "display_order",
                        spark_functions.lit(display_index * 2 + 1),
                    )
                )
                truncated_column_dataframes.append(ellipsis_row_df)

        truncated_rows_df = reduce(
            lambda left_dataframe, right_dataframe: left_dataframe.unionByName(
                right_dataframe
            ),
            truncated_column_dataframes,
        )

        self.logger.debug("truncate_by_column_limit completed successfully")
        return truncated_rows_df.orderBy("display_order").drop("display_order")

    def display_pretty(self) -> None:
        """
        Display the initialized comparison DataFrames as an interactive HTML report.
        """
        self.logger.debug("display_pretty called")

        # if no keys provided, then just compare the schema
        if not self._join_key_columns:
            schema_only_html = self.generate_html_tables(
                input_dataframes=[self._schema_comparison_df]
            )
            display(HTML(self._render_report_document(schema_only_html)))
            return

        selected_columns_schema_html = self.generate_html_tables(
            input_dataframes=[self._schema_comparison_df],
            titles=["Just specified column names compare"],
            descriptions=[
                "Comparison of each column name and type to see if there are any differences in either"
            ],
        )

        value_comparison_html = self.generate_html_tables(
            input_dataframes=[
                self._difference_analysis_df,
                self._key_analysis_stats_df,
            ],
            titles=["Difference Analysis On Key", "Key Analysis Stats"],
            descriptions=[
                "Display joined rows whose compared values differ",
                "Statistics comparing matching and differing values",
            ],
        )

        key_uniqueness_html = self.generate_html_tables(
            input_dataframes=[self._primary_key_comparison_df],
            titles=["Distinct Primary Keys for Given Key"],
            descriptions=[
                "the count of dinstinct primary key values for the given PK, for every table and the join of tables"
            ],
        )

        # create final html table to display with buttons
        comparison_report_html = self.generate_html_button_switch(
            table_html_fragments=[
                selected_columns_schema_html,
                value_comparison_html,
                key_uniqueness_html,
            ],
            titles=["Schema Comparison", "Column Comparison", "PK Comparison"],
        )
        self.logger.debug("Finished display_pretty")
        display(HTML(comparison_report_html))

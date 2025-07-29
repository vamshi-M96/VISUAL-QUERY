import pandas as pd
import streamlit as st
import numpy as np
import re   


def generate_sql_query_for_step(step):
    if step["type"] == "Filter Rows":
        table = step["table"]
        expr = step["expression"]
        return f"SELECT * FROM {table} WHERE {expr}"

    elif step["type"] == "Sort Rows":
        table = step["table"]
        columns = ", ".join(step["columns"])
        order = "ASC" if step.get("ascending", True) else "DESC"
        return f"SELECT * FROM {table} ORDER BY {columns} {order}"

   
    elif step["type"] == "Group By":
        table = step["table"]
        group_cols = ", ".join(step["group_cols"])
        aggregations = step["aggregations"]
        having_conditions = step.get("having_conditions", [])

        # SELECT aggregation parts
        agg_select = ", ".join([f"{func.upper()}({col}) AS {func}_{col}" for col, func in aggregations.items()])

        # HAVING clause
        having_clause = ""
        if having_conditions:
            having_parts = [
                f"{cond['function'].upper()}({cond['column']}) {cond['operator']} {cond['value']}"
                for cond in having_conditions
            ]
            having_clause = " HAVING " + " AND ".join(having_parts)

        return f"""
        SELECT {group_cols}, {agg_select}
        FROM {table}
        GROUP BY {group_cols}{having_clause}
        """.strip()
    elif step["type"] == "Join Tables":
        lt = step["left_table"]
        rt = step["right_table"]
        lcol = step["left_on"]
        rcol = step["right_on"]
        join_type = step["join_type"].upper()
        return f"SELECT * FROM {lt} {join_type} JOIN {rt} ON {lt}.{lcol} = {rt}.{rcol}"

    elif step["type"] == "Modify Column":
        expr = step.get("expression", "")
        new_col = step.get("alias", "new_column")
        table = step.get("table", step.get("table1", "UNKNOWN"))
        return f"SELECT *, ({expr}) AS {new_col} FROM {table}"

    elif step["type"] == "INSERT":
        table = step["table"]
        cols = ", ".join(step["columns"])
        vals = ", ".join([f"'{v}'" for v in step["values"].values()])
        return f"INSERT INTO {table} ({cols}) VALUES ({vals})"

    elif step["type"] == "UPDATE":
        table = step["table"]
        col = step["update_col"]
        val = step["new_value"]
        cond_col = step["condition_col"]
        cond_val = step["condition_val"]
        return f"UPDATE {table} SET {col} = '{val}' WHERE {cond_col} = '{cond_val}'"

    elif step["type"] == "DELETE":
        table = step.get("table", "your_table")
        cond_col = step.get("condition_col")
        cond_val = step.get("condition_val")

        if not cond_col or cond_val is None:
            return "-- Error generating SQL: missing condition"

        return f"DELETE FROM {table} WHERE {cond_col} = '{cond_val}';"

    elif step["type"] == "Aggregate Column":
        table = step["table"]
        col = step["column"]
        func = step["function"]
        alias = step["alias"]
        return f"SELECT {func.upper()}({col}) AS {alias} FROM {table}"

    elif step["type"] == "Create New Table with Foreign Link":
        
        table = step.get("output_name", "new_table")
        columns = step.get("columns", [])
        dtypes = step.get("dtypes", {})
        rows = step.get("data", [])

        if not columns or not rows:
            return f"-- ❌ Cannot generate SQL: Missing data or columns for {table}"

        # Map Python to SQL types
        dtype_map = {
            "int": "INTEGER",
            "float": "FLOAT",
            "str": "VARCHAR(255)",
            "datetime": "TIMESTAMP"
        }

        # CREATE TABLE
        column_defs = []
        for col in columns:
            py_dtype = dtypes.get(col, "str")
            sql_type = dtype_map.get(py_dtype, "VARCHAR(255)")
            column_defs.append(f"{col} {sql_type}")
        create_sql = f"CREATE TABLE {table} ({', '.join(column_defs)});"

        # INSERT INTO
        insert_stmts = []
        for row in rows:
            formatted = []
            for idx, val in enumerate(row):
                col = columns[idx]
                dtype = dtypes.get(col, "str")
                if val in [None, "", "null"]:
                    formatted.append("NULL")
                elif dtype in ["str", "datetime"]:
                    val = str(val).replace("'", "''")
                    formatted.append(f"'{val}'")
                else:
                    formatted.append(str(val))
            insert_stmts.append(f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(formatted)});")

        return create_sql + "\n" + "\n".join(insert_stmts)

    elif step["type"] == "Handle Missing Values":
        table = step["table"]
        col = step["column"]
        strategy = step["strategy"]
        if strategy == "Drop Rows":
            return f"SELECT * FROM {table} WHERE {col} IS NOT NULL"
        elif strategy == "Fill with Custom Value":
            val = step["custom_value"]
            return f"SELECT *, COALESCE({col}, '{val}') AS {col}_filled FROM {table}"
        else:
            return f"-- Strategy {strategy} may require preprocessing"

    elif step["type"] == "Set Operation":
        table1 = step.get("table1")
        table2 = step.get("table2")
        operation = step.get("operation", "UNION")

        if not table1 or not table2:
            sql = "-- Error: Missing input tables for set operation."
        else:
            sql = f"-- Perform {operation} on {table1} and {table2}\n"
            sql += f"SELECT * FROM {table1}\n{operation}\nSELECT * FROM {table2};"

        st.code(sql, language="sql")


    elif step["type"] == "Create Table with Primary Key":
        table = step.get("output_name", "new_table")
        columns = step.get("columns", [])
        dtypes = step.get("dtypes", {})
        pk = step.get("primary_key", "")
        row_data = step.get("data", "")

        if not columns or not dtypes:
            return "-- Error: Columns or data types missing"

        # Generate CREATE TABLE
        col_defs = []
        for col in columns:
            dtype = dtypes.get(col, "TEXT")
            col_defs.append(f"{col} {dtype}")

        sql = f"CREATE TABLE {table} (\n    " + ",\n    ".join(col_defs)
        if pk:
            sql += f",\n    PRIMARY KEY ({pk})"
        sql += "\n);\n"

        # Generate INSERT INTO if data provided
        if row_data.strip():
            rows = [row.strip() for row in row_data.strip().split("\n") if row.strip()]
            for row in rows:
                values = [val.strip() for val in row.split(",")]
                val_str = ", ".join(
                    [f"'{v}'" if dtypes[columns[i]] in ["TEXT", "DATE"] else v for i, v in enumerate(values)]
                )
                sql += f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({val_str});\n"

        return sql

    elif step["type"] == "Modify Table Structure":
        action = step.get("action", "")
        table = step["table"]

        if action == "Add Column":
            col = step.get("new_column", "new_col")
            dtype = step.get("dtype", "str").lower()
            default = step.get("default", "").strip()
            value_mode = step.get("value_mode", "Use a single default value for all rows")

            dtype_map = {"int": "INTEGER", "float": "REAL", "str": "TEXT"}
            sql_dtype = dtype_map.get(dtype, "TEXT")

            sql_statements = []

            if default and value_mode == "Use a single default value for all rows":
                sql_statements.append(
                    f"ALTER TABLE {table} ADD COLUMN {col} {sql_dtype} DEFAULT '{default}';"
                )
            else:
                sql_statements.append(
                    f"ALTER TABLE {table} ADD COLUMN {col} {sql_dtype};"
                )

            if value_mode == "Enter different values per row":
                values = step.get("values", [])
                for i, val in enumerate(values):
                    if pd.isna(val):
                        continue
                    val_sql = f"'{val}'" if dtype == "str" else str(val)
                    sql_statements.append(
                        f"UPDATE {table} SET {col} = {val_sql} WHERE id = {i + 1};"
                    )

            return "\n".join(sql_statements)

        elif action == "Delete Column":
            return f"ALTER TABLE {table} DROP COLUMN {', '.join(step['columns'])};"

        elif action == "Rename Columns":
            renames = [f"RENAME COLUMN {old} TO {new}" for old, new in step["rename_dict"].items()]
            return f"ALTER TABLE {table} " + " ".join(renames) + ";"

        elif action == "Add Row":
            values = step.get("values", [])
            if values:
                value_str = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in values)
                return f"INSERT INTO {table} VALUES ({value_str});"
            else:
                return "-- ⚠️ No values provided for Add Row"

        elif action == "Delete Row":
            col = step.get("condition_col")
            val = step.get("condition_val", "").strip()
            if not col or val == "":
                return "-- ⚠️ Missing column or value for Delete Row"
            if isinstance(val, str) and not val.replace('.', '', 1).isdigit():
                val_sql = f"'{val}'"
            else:
                val_sql = val
            return f"DELETE FROM {table} WHERE {col} = {val_sql};"


        elif action == "Convert Data Types":
            conversions = [f"ALTER COLUMN {col} TYPE {dtype}" for col, dtype in step["dtype_dict"].items()]
            return f"ALTER TABLE {table} " + ", ".join(conversions) + ";"
        
        

        elif action == "Rename Columns":
            renames = [f"RENAME COLUMN {old} TO {new}" for old, new in step["rename_dict"].items()]
            return f"ALTER TABLE {table} " + " ".join(renames)
        
        elif action == "Convert Data Types":
            conversions = [f"ALTER COLUMN {col} TYPE {dtype}" for col, dtype in step["dtype_dict"].items()]
            return f"ALTER TABLE {table} " + ", ".join(conversions)

    return "-- Unsupported step or missing data"

def chain_sql_steps(sql_list):
    with_blocks = []
    for i, sql in enumerate(sql_list[:-1]):
        with_blocks.append(f"step_{i+1} AS (\n  {sql}\n)")
    final = sql_list[-1]
    if with_blocks:
        return "WITH " + ",\n".join(with_blocks) + f"\nSELECT * FROM step_{len(sql_list)};"
    else:
        return final

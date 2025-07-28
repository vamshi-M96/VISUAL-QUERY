import streamlit as st
import os
from sql_generator import generate_sql_query_for_step
import pandas as pd
from datetime import datetime
import numpy as np

import io


sql_to_pandas_dtype = {
    "INT": "int64",
    "INTEGER": "int64",
    "TEXT": "object",
    "VARCHAR": "object",
    "FLOAT": "float64",
    "REAL": "float64",
    "DOUBLE": "float64",
    "BOOLEAN": "bool",
    "DATE": "datetime64[ns]"
}


def sql_pipeline_ui(prefix="basic"):

    pipeline_key = f"{prefix}_sql_pipeline"
    output_key = f"{prefix}_sql_step_outputs"

    custom_dir = st.sidebar.text_input(
        "📁 Enter folder path to save step outputs:",
        value=os.path.abspath("sql_outputs"),  # Default fallback
        key=f"{prefix}_sql_output_path",
        help="Path where step output files will be stored."
    )

     # Validate and create main folder
    if custom_dir:
        try:
            os.makedirs(custom_dir, exist_ok=True)
            st.success(f"✅ Folder ready: `{custom_dir}`")
        except Exception as e:
            st.error(f"❌ Failed to create folder: {e}")
            return
    else:
        st.warning("⚠️ Please enter a valid folder path.")
        return

     # 🗂️ Create step output subfolder
    step_output_dir = os.path.join(custom_dir, "step_outputs")
    try:
        os.makedirs(step_output_dir, exist_ok=True)
        st.caption(f"📁 Step outputs will be saved to: `{step_output_dir}`")
    except Exception as e:
        st.error(f"❌ Could not create step_outputs folder: {e}")
        return

    # 💾 Store in session state so other steps/modules can access
    st.session_state["step_output_dir"] = step_output_dir

    
    SQL_STEP_OPTIONS = ["Filter Rows", "Sort Rows", "Group By", "Join Tables", "Aggregate Column",
                         "Modify Column","Create & Save New Table","INSERT", "UPDATE", "DELETE","Set Operation","Handle Missing Values",
                         "Modify Table Structure","Create New Table with Foreign Link","Create Table with Primary Key"]

    # Ensure pipeline exists
    if "sql_pipeline" not in st.session_state:
        st.session_state.sql_pipeline = []

    
    dataframes = st.session_state.get("uploaded_tables", {})
    if not dataframes:
        st.warning("⚠️ Upload at least one dataset first.")
        return

        # --- Select Step Type using Horizontal Buttons ---
    st.markdown("### ➕ Select Step Type")
    # Arrange buttons in rows of 4 columns
    num_cols = 5
    button_style = """
    <style>
    div[data-testid="column"] > div {
        display: flex;
        justify-content: center;
    }
    button[kind="secondary"] {
        width: 100% !important;
        min-width: 100px;

        font-weight: bold;
        border-radius: 8px;
        color: white;
        background: 	linear-gradient(135deg, #FF512F, #DD2476);  /* Green to Blue */
        border: none;
        transition: background 0.3s ease;
    }
    button[kind="secondary"]:hover {
        background: linear-gradient(135deg, #360033, #0b8793);  /* Hover reverse */
        cursor: pointer;
        color: #FFD790;  /* 💛 Change text to gold on hover */
        transform: scale(1.09); 
    }
    </style>
    """
    st.markdown(button_style, unsafe_allow_html=True)


    for i in range(0, len(SQL_STEP_OPTIONS), num_cols):
        cols = st.columns(num_cols)
        for j, opt in enumerate(SQL_STEP_OPTIONS[i:i+num_cols]):
            with cols[j]:
                if st.button(opt, key=f"step_btn_{opt}"):
                    st.session_state.sql_pipeline.append({"type": opt})
                    st.session_state.selected_step_index = len(st.session_state.sql_pipeline) - 1
                    st.rerun()

 
    final_output = None
    # Show Forms for Each Step
    for i, step in enumerate(st.session_state.sql_pipeline):
        with st.expander(f"Step {i+1}: {step['type']}", expanded=True):
            
            build_step_form(i, step, dataframes,prefix="basic")

            col1, col2 = st.columns([1, 1])
            
            with col2:
                if st.button("❌ Delete", key=f"basic_delete_{i}"):
                    st.session_state.sql_pipeline.pop(i)
                    st.rerun()

            # Apply step logic
            df = apply_step(step, dataframes)
            if df is not None:
                st.markdown(f"#### 📄 Output of Step {i+1}")
                st.dataframe(df)
                step_filename = f"step_{i+1}_{step['type'].replace(' ', '_').lower()}.csv"
                step_path = os.path.join(step_output_dir, step_filename)
                df.to_csv(step_path, index=False)
                with open(step_path, "rb") as f:
                    st.download_button(f"⬇️ Download Step {i+1} Output", f, file_name=step_filename)




def build_step_form(step_count, step, dataframes,prefix="basic"):
    SQL_STEP_OPTIONS = ["Filter Rows", "Sort Rows", "Group By", "Join Tables"]
    step_type = step.get("type", SQL_STEP_OPTIONS[0])

    step["type"] = step_type
    if step_type == "Filter Rows":
        table = st.selectbox("Select Table", list(dataframes.keys()), 
                            index=list(dataframes.keys()).index(step.get("table", list(dataframes.keys())[0])),
                            key=f"{prefix}_filter_table_{step_count}")
        column = st.selectbox("Column", dataframes[table].columns, key=f"{prefix}_filter_col_{step_count}")
        col_dtype = dataframes[table][column].dtype

        if pd.api.types.is_numeric_dtype(col_dtype):
            operator_options = ["==", "!=", ">", "<", ">=", "<=", "between", "not between"]
        else:
            operator_options = ["==", "!=", "contains"]

        operator = st.selectbox("Operator", operator_options, key=f"{prefix}_filter_op_{step_count}")

        use_manual = st.checkbox("Manual Input", key=f"{prefix}_manual_input_{step_count}")

        if use_manual:
            if operator in ["between", "not between"]:
                value_low = st.text_input("Lower Bound", key=f"{prefix}_filter_val_manual_low_{step_count}")
                value_high = st.text_input("Upper Bound", key=f"{prefix}_filter_val_manual_high_{step_count}")
                if operator == "between":
                    expr = f"({column} >= {value_low}) & ({column} <= {value_high})"
                else:
                    expr = f"({column} < {value_low}) | ({column} > {value_high})"
                step.update({"value": (value_low, value_high)})
            else:
                value = st.text_input("Enter value manually", key=f"{prefix}_filter_val_manual_{step_count}")
                expr = f"{column}.str.contains('{value}')" if operator == "contains" else f"{column} {operator} {repr(value)}"
                step.update({"value": value})
        else:
            if pd.api.types.is_numeric_dtype(col_dtype):
                min_val = float(dataframes[table][column].min())
                max_val = float(dataframes[table][column].max())
                if operator == "between":
                    value = st.slider("Select range", min_value=min_val, max_value=max_val, value=(min_val, max_val), key=f"{prefix}_filter_val_range_{step_count}")
                    expr = f"({column} >= {value[0]}) & ({column} <= {value[1]})"
                elif operator == "not between":
                    value = st.slider("Select excluded range", min_value=min_val, max_value=max_val, value=(min_val, max_val), key=f"{prefix}_filter_val_range_not_{step_count}")
                    expr = f"({column} < {value[0]}) | ({column} > {value[1]})"
                else:
                    value = st.slider("Select value", min_value=min_val, max_value=max_val, value=min_val, key=f"{prefix}_filter_val_slider_{step_count}")
                    expr = f"{column} {operator} {value}"
                step.update({"value": value})
            else:
                options = dataframes[table][column].dropna().unique().tolist()
                value = st.selectbox("Select value", options, key=f"{prefix}_filter_val_cat_{step_count}")
                expr = f"{column}.str.contains('{value}')" if operator == "contains" else f"{column} {operator} '{value}'"
                step.update({"value": value})

        step.update({"table": table, "column": column, "operator": operator, "expression": expr})
        


    elif step_type == "Group By":
        table = st.selectbox("Select Table", list(dataframes.keys()),
                            index=list(dataframes.keys()).index(step.get("table", list(dataframes.keys())[0])),
                            key=f"{prefix}_group_table_{step_count}")
        
        available_cols = dataframes[table].columns.tolist()
        default_group_cols = [col for col in step.get("group_cols", []) if col in available_cols]

        group_cols = st.multiselect("Group by columns", available_cols,
                                    default=default_group_cols,
                                    key=f"{prefix}_group_cols_{step_count}")

        numeric_cols = dataframes[table].select_dtypes(include='number').columns.tolist()
        selected_agg_cols = st.multiselect("Select numeric columns to aggregate", numeric_cols,
                                        default=[col for col in step.get("aggregations", {}).keys() if col in numeric_cols],
                                        key=f"{prefix}_agg_select_cols_{step_count}")

        agg_dict = step.get("aggregations", {})
        updated_agg = {}
        st.markdown("### Aggregation Functions")
        for col in selected_agg_cols:
            func = st.selectbox(f"Aggregation for {col}", ["sum", "mean", "count", "min", "max"],
                                index=["sum", "mean", "count", "min", "max"].index(agg_dict.get(col, "sum")) if col in agg_dict else 0,
                                key=f"{prefix}_agg_{step_count}_{col}")
            updated_agg[col] = func

        step.update({"table": table, "group_cols": group_cols, "aggregations": updated_agg})
        

    elif step_type == "Sort Rows":
        table = st.selectbox("Select Table", list(dataframes.keys()),
                             index=list(dataframes.keys()).index(step.get("table", list(dataframes.keys())[0])),
                             key=f"{prefix}_sort_table_{step_count}")
        available_cols = dataframes[table].columns.tolist()
        default_sort_cols = [col for col in step.get("columns", []) if col in available_cols]
            
        sort_cols = st.multiselect("Sort by columns", available_cols,
                                    default=default_sort_cols,
                                    key=f"{prefix}_sort_cols_{step_count}")
        order = st.radio("Sort Order", ["Ascending", "Descending"],
                         index=0 if step.get("ascending", True) else 1,
                         horizontal=True, key=f"{prefix}_sort_order_{step_count}")
        step.update({"table": table, "columns": sort_cols, "ascending": order == "Ascending"})
        
    
    elif step_type == "Join Tables":
        left_table = st.selectbox("Left Table", list(dataframes.keys()),
                                index=list(dataframes.keys()).index(step.get("left_table", list(dataframes.keys())[0])),
                                key=f"{prefix}_join_left_{step_count}")

        right_table = st.selectbox("Right Table", list(dataframes.keys()),
                                index=list(dataframes.keys()).index(step.get("right_table", list(dataframes.keys())[0])),
                                key=f"{prefix}_join_right_{step_count}")

        join_type = st.selectbox("Join Type", ["inner", "left", "right", "outer"],
                                index=["inner", "left", "right", "outer"].index(step.get("join_type", "inner")),
                                key=f"{prefix}_join_type_{step_count}")

        left_cols = list(dataframes[left_table].columns)
        right_cols = list(dataframes[right_table].columns)

        # Match columns ignoring case
        left_cols_lower = {col.lower(): col for col in left_cols}
        right_cols_lower = {col.lower(): col for col in right_cols}
        common_keys = list(set(left_cols_lower.keys()) & set(right_cols_lower.keys()))
        common_cols = [left_cols_lower[k] for k in common_keys]

        join_mode = st.radio("Join Key Mode", ["Use Common Column", "Choose Custom Columns"],
                            index=0 if step.get("left_on") in common_cols else 1,
                            key=f"{prefix}_join_mode_{step_count}")

        if join_mode == "Use Common Column":
            default_common = step.get("left_on", common_cols[0] if common_cols else left_cols[0])
            common_key = st.selectbox("Join Key (common in both tables)", common_cols,
                                    index=common_cols.index(default_common) if default_common in common_cols else 0,
                                    key=f"{prefix}_common_key_{step_count}")
            left_on = right_on = common_key
        else:
            zipped_options = [f"{lcol} ↔ {rcol}" for lcol in left_cols for rcol in right_cols]
            default_pair = f"{step.get('left_on', left_cols[0])} ↔ {step.get('right_on', right_cols[0])}"
            selected_pair = st.selectbox("Match Columns", zipped_options,
                                        index=zipped_options.index(default_pair) if default_pair in zipped_options else 0,
                                        key=f"{prefix}_custom_join_pair_{step_count}")
            left_on, right_on = selected_pair.split(" ↔ ")

        # Show dtype mismatch warning
        try:
            dtype_left = dataframes[left_table][left_on].dtype
            dtype_right = dataframes[right_table][right_on].dtype
            if dtype_left != dtype_right:
                st.warning(f"⚠️ Mismatched data types: `{left_on}` is {dtype_left}, `{right_on}` is {dtype_right}")
        except Exception as e:
            st.error(f"Error checking dtypes: {e}")

        cast_to_str = st.checkbox("Convert join columns to string before joining?", value=True, key=f"{prefix}_cast_str_{step_count}")

        step.update({
            "left_table": left_table,
            "right_table": right_table,
            "join_type": join_type,
            "left_on": left_on,
            "right_on": right_on,
            "cast_to_str": cast_to_str,
            "is_foreign_key_link": True,
            "depends_on": [left_table, right_table]

        })

    elif step_type == "Aggregate Column":
        table = st.selectbox("Select Table", list(dataframes.keys()), 
                                index=list(dataframes.keys()).index(step.get("table", list(dataframes.keys())[0])),
                                key=f"{prefix}_aggcol_table_{step_count}")
        col = st.selectbox("Column to Aggregate", dataframes[table].select_dtypes(include='number').columns.tolist(),
                            index=0, key=f"{prefix}_aggcol_col_{step_count}")
        func = st.selectbox("Aggregation Function", ["sum", "mean", "count", "min", "max"],
                            index=0, key=f"{prefix}_aggcol_func_{step_count}")
        alias = st.text_input("Output Column Name (alias)", f"{func}_{col}", key=f"{prefix}_aggcol_alias_{step_count}")
        step.update({"table": table, "column": col, "function": func, "alias": alias})

    elif step_type == "Modify Column":
        st.subheader("🔧 Modify Column with Operation")

        all_tables = list(dataframes.keys())
        all_columns = {table: list(dataframes[table].columns) for table in all_tables}

        use_manual_expr = st.checkbox("Use Manual Expression?", key=f"{prefix}_mod_expr_mode_{step_count}")

        if use_manual_expr:
            expr = st.text_area("Enter custom expression (e.g. tables['table1']['col1'] + tables['table2']['col2'])",
                                value=step.get("expression", ""),
                                key=f"{prefix}_mod_expr_{step_count}")
            new_col = st.text_input("New Column Name", step.get("new_column", "new_col"),
                                    key=f"{prefix}_mod_expr_colname_{step_count}")

            step.update({
                "type": "Modify Column",
                "expression": expr,
                "new_column": new_col,
                "alias": new_col,
                "mode": "manual",
                "use_manual_expr": True
            })

        else:
            table1 = st.selectbox("Table 1", all_tables, index=0, key=f"{prefix}_mod_table1_{step_count}")
            col1 = st.selectbox("Column from Table 1", all_columns[table1], index=0, key=f"{prefix}_mod_col1_{step_count}")
            col1_dtype = str(dataframes[table1][col1].dtype)

            mode = st.radio("Use another column or constant?", ["Column from another table", "Manual constant"],
                            key=f"{prefix}_mod_rhs_mode_{step_count}")

            if mode == "Column from another table":
                table2 = st.selectbox("Table 2", all_tables, index=0, key=f"{prefix}_mod_table2_{step_count}")
                col2 = st.selectbox("Column from Table 2", all_columns[table2], index=0, key=f"{prefix}_mod_col2_{step_count}")
                col2_dtype = str(dataframes[table2][col2].dtype)
                right_expr = f"tables['{table2}']['{col2}']"
            else:
                const_value = st.text_input("Enter constant value", key=f"{prefix}_mod_const_{step_count}")
                col2_dtype = type(const_value).__name__
                right_expr = const_value

            # Determine allowed operations
            numeric_ops = {"+": "Addition (+)", "-": "Subtraction (-)", "*": "Multiplication (*)", "/": "Division (/)", "%": "Modulo (%)", "**": "Power (**)","==": "Equal (==)", "!=": "Not Equal (!=)", ">": "Greater Than (>)", "<": "Less Than (<)", ">=": "Greater or Equal (>=)", "<=": "Less or Equal (<=)"}
            string_ops = {"+": "Concatenate (+)", "==": "Equal (==)", "!=": "Not Equal (!=)"}

            if ("object" in col1_dtype or "str" in col1_dtype) or ("object" in col2_dtype or "str" in col2_dtype):
                operator_display = string_ops
            else:
                operator_display = numeric_ops

            operator = st.selectbox(
                "Operation",
                options=list(operator_display.keys()),
                format_func=lambda op: operator_display[op],
                index=0,
                key=f"{prefix}_mod_op_{step_count}"
            )

            new_col = st.text_input("New Column Name", f"{col1}_{operator}_new", key=f"{prefix}_mod_new_col_{step_count}")
            full_expr = f"tables['{table1}']['{col1}'] {operator} {right_expr}"

            st.code(f"{new_col} = {full_expr}", language="python")

            step.update({
                "type": "Modify Column",
                "table": table1,
                "table1": table1,
                "col1": col1,
                "operator": operator,
                "rhs_mode": mode,
                "table2": table2 if mode == "Column from another table" else "",
                "col2": col2 if mode == "Column from another table" else "",
                "constant": const_value if mode == "Manual constant" else "",
                "new_column": new_col,
                "alias": new_col,
                "mode": "standard",
                "expression": full_expr,
                "use_manual_expr": False,
                "col1_table": table1,
                "col2_table": table2 if mode == "Column from another table" else None,
                "col1_dtype": col1_dtype,
                "col2_dtype": col2_dtype
            })

    elif step_type == "Create & Save New Table":
        st.markdown("### 🧱 Create and Save a New Table")

        table_names = list(dataframes.keys())
        use_existing = st.checkbox("Use columns from existing table?", key=f"{prefix}_use_existing_{id(step)}")

        base_df = None
        base_columns = []
        if use_existing and table_names:
            base_table = st.selectbox("Select Base Table", table_names, key=f"{prefix}_base_table_{id(step)}")
            base_df = dataframes[base_table]
            base_columns = st.multiselect(
                "Select Columns from Base Table",
                options=base_df.columns.tolist(),
                default=base_df.columns.tolist(),
                key=f"base_table_selected_cols_{id(step)}"
            )

        st.markdown("#### ➕ Define New Columns")
        custom_columns_str = st.text_input(
            "Enter New Column Names (comma-separated)",
            value=step.get("custom_columns_str", "col1, col2"),
            key=f"{prefix}_custom_columns_str_{id(step)}"
        )
        custom_columns = [col.strip() for col in custom_columns_str.split(",") if col.strip()]

        st.markdown("#### 🧬 Select Data Types for Custom Columns")
        dtypes = {}
        for col in custom_columns:
            dtype = st.selectbox(
                f"Data Type for {col}",
                ["str", "int", "float", "datetime"],
                key=f"{prefix}_dtype_{col}_{id(step)}"
            )
            dtypes[col] = dtype

        st.markdown("#### 📝 Enter Row Data for New Columns")
        st.caption("Separate rows with `;` and values with `,` — e.g. `a,1.5; b,2.3`")
        row_data_input = st.text_area(
            "Enter Data",
            value=step.get("row_data_input", ""),
            key=f"{prefix}_row_data_input_{id(step)}"
        )

        # Parse rows only if input is not empty
        rows = []
        if row_data_input.strip():
            for row in row_data_input.strip().split(";"):
                values = [val.strip() for val in row.strip().split(",")]
                if len(values) == len(custom_columns):
                    rows.append(values)

        # Only proceed if base table or manual rows are available
        if (use_existing and base_df is not None and base_columns) or rows:
            new_df = pd.DataFrame(rows, columns=custom_columns if custom_columns else None)

            # Convert data types
            for col, dtype in dtypes.items():
                try:
                    if dtype == "datetime":
                        new_df[col] = pd.to_datetime(new_df[col], errors="coerce")
                    else:
                        new_df[col] = new_df[col].astype(dtype)
                except Exception as e:
                    st.warning(f"⚠️ Could not convert {col} to {dtype}: {e}")

            # Merge with base columns if needed
            if use_existing and base_df is not None and base_columns:
                new_df = pd.concat([base_df[base_columns].reset_index(drop=True), new_df], axis=1)

            st.markdown("#### ✅ Final Preview")
            st.dataframe(new_df)

            # Save step info
            step["data"] = new_df.to_dict(orient="records")
            step["columns"] = new_df.columns.tolist()
            step["dtypes"] = dtypes
            step["custom_columns_str"] = custom_columns_str
            step["row_data_input"] = row_data_input
            output_name = st.text_input("Output Table Name", value=step.get("output_name", "new_table"), key=f"{prefix}_output_name_{id(step)}")
            step["output_name"] = output_name
        else:
            st.error("❌ Please select a base table or enter row data manually to create a new table.")

    elif step_type == "INSERT":
        st.markdown("### ➕ INSERT INTO Table")
        table_name = st.selectbox("Select Table to Insert Into", list(dataframes.keys()), key=f"{prefix}_insert_table")
        insert_df = dataframes[table_name]
        all_columns = insert_df.columns.tolist()

        columns = st.multiselect("Select Columns to Insert", options=all_columns, key=f"{prefix}_insert_columns")

        if columns:
            values = {}
            for col in columns:
                values[col] = st.text_input(f"Enter value for `{col}`", key=f"{prefix}_insert_value_{col}")

            if st.button("Run INSERT", key=f"{prefix}_run_insert"):
                try:
                    # Step 1: Build a full row with None for missing columns
                    full_row = {col: values.get(col, None) for col in all_columns}

                    # Step 2: Append safely using pd.concat
                    new_row_df = pd.DataFrame([full_row])
                    insert_df = pd.concat([insert_df, new_row_df], ignore_index=True)

                    st.success("✅ Row inserted successfully.")
                    st.dataframe(insert_df)

                    # Step 3: Save updated dataframe
                    dataframes[table_name] = insert_df

                    # Optional: save step SQL
                    step["sql"] = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join([repr(values[col]) for col in columns])});"
                    step["table"] = table_name
                    step["columns"] = columns
                    step["values"] = values

                except Exception as e:
                    st.error(f"❌ Error during insert: {e}")

    elif step_type == "UPDATE":
        st.markdown("### 🔄 UPDATE Table")

        table_name = st.selectbox("Select Table to Update", list(dataframes.keys()), key=f"{prefix}_update_table")
        df = dataframes[table_name]
        col_options = df.columns.tolist()

        # Condition to filter matching rows
        condition_col = st.selectbox("Update where Column =", col_options, key=f"{prefix}_cond_col")
        st.dataframe(df)
        condition_val = st.text_input("Match value (WHERE column = ...)", key=f"{prefix}_cond_val")

        update_col = st.selectbox("Select Column to Update", col_options, key=f"{prefix}_update_col")
        new_value = st.text_input("New Value for selected column", key=f"{prefix}_new_val")

        step["type"] = "UPDATE"
        step["table"] = table_name
        step["condition_col"] = condition_col
        step["condition_val"] = condition_val
        step["update_col"] = update_col
        step["new_value"] = new_value

        if condition_col and condition_val:
            mask = df[condition_col].astype(str).str.strip() == condition_val.strip()
            matched_df = df[mask]

            if not matched_df.empty:
                st.write("🔍 Matching Rows Found:")
                st.dataframe(matched_df)

                # Allow user to select which row indices to update
                selectable_indices = matched_df.index.tolist()
                selected_indices = st.multiselect(
                    "✅ Select rows (by index) to apply the update:", 
                    options=selectable_indices,
                    default=selectable_indices  # default to all
                )
            else:
                st.warning("⚠️ No rows matched. Try checking the condition value.")
                selected_indices = []

        else:
            selected_indices = []

        if st.button("Run UPDATE", key=f"{prefix}_run_update"):
            try:
                if not selected_indices:
                    st.warning("⚠️ No rows selected to update.")
                else:
                    # Show before update
                    st.write("🟡 Rows BEFORE update:")
                    st.dataframe(df.loc[selected_indices])

                    # Apply update to only selected rows
                    df.loc[selected_indices, update_col] = new_value
                    dataframes[table_name] = df  # save to session

                    st.success(f"✅ UPDATE applied to `{len(selected_indices)}` row(s) in `{table_name}`.")

                    # Show after update
                    st.write("🟢 Rows AFTER update:")
                    st.dataframe(df.loc[selected_indices])

            except Exception as e:
                st.error(f"❌ Error applying UPDATE: {e}")


    elif step_type == "DELETE":
        st.markdown("### ❌ DELETE From Table")

        table_name = st.selectbox("Select Table to Delete From", list(dataframes.keys()), key=f"{prefix}_delete_table")
        step["table"] = table_name
        delete_df = dataframes[table_name]

        condition_col = st.selectbox("Select Condition Column", delete_df.columns, key=f"{prefix}_delete_cond_col")
        condition_val = st.text_input(f"Delete where `{condition_col}` =", key=f"{prefix}_delete_cond_val")

        # ✅ Save condition to step
        step["condition_col"] = condition_col
        step["condition_val"] = condition_val

        # ✅ Type-safe comparison
        try:
            val_casted = delete_df[condition_col].dtype.type(condition_val)
        except:
            val_casted = condition_val

        matching_rows = delete_df[delete_df[condition_col] == val_casted]

        if not matching_rows.empty:
            st.write("### Matching Rows:")
            matching_rows_display = matching_rows.reset_index()
            selected_indices = st.multiselect(
                "Select row(s) to delete",
                options=matching_rows_display["index"].tolist(),
                format_func=lambda i: f"Row {i}: {delete_df.loc[i].to_dict()}"
            )

            if st.button("Run DELETE", key=f"{prefix}_run_delete"):
                try:
                    before_count = len(delete_df)
                    delete_df = delete_df.drop(index=selected_indices)
                    after_count = len(delete_df)
                    dataframes[table_name] = delete_df.reset_index(drop=True)

                    step["sql"] = f"DELETE FROM {table_name} WHERE {condition_col} = '{condition_val}' AND index IN ({','.join(map(str, selected_indices))})"
                    st.success(f"✅ Deleted {before_count - after_count} row(s).")
                    st.dataframe(dataframes[table_name])
                except Exception as e:
                    st.error(f"❌ Error during delete: {e}")
        else:
            st.info("ℹ️ No matching rows found.")

    elif step_type == "Set Operation":
        st.markdown("### 📊 Set Operation (UNION / INTERSECT / EXCEPT)")

        # 👈 Select first table
        table1 = st.selectbox(
            "Select First Table",
            options=list(dataframes.keys()),
            key=f"{prefix}_setop_table1"
        )

        # 👈 Select second table (exclude same table)
        table2 = st.selectbox(
            "Select Second Table",
            options=[t for t in dataframes.keys() if t != table1],
            key=f"{prefix}_setop_table2"
        )

        # 👈 Select set operation
        operation = st.selectbox(
            "Select Set Operation",
            options=["UNION", "UNION ALL", "INTERSECT", "EXCEPT"],
            key=f"{prefix}_setop_operation"
        )

        # ✅ Save selected values into the step dictionary
        step["table1"] = table1
        step["table2"] = table2
        step["operation"] = operation
        step["output_name"] = f"{table1}_{operation.replace(' ', '_')}_{table2}"

    elif step_type == "Create Table with Primary Key":
        st.markdown("### 🏗️ Create New Table with Primary Key")
        table_name = st.text_input("🆕 Table name", key=f"{prefix}_table_name")

        cols_input = st.text_area("📋 Enter column names (comma-separated)", key=f"{prefix}_cols")

        if cols_input:
            columns = [col.strip() for col in cols_input.split(",") if col.strip()]
            dtypes = {}
            st.markdown("### 🧬 Select Data Types for Each Column")

            # Step 2: Choose data type per column
            for col in columns:
                dtype = st.selectbox(
                    f"📌 Data type for `{col}`",
                    ["INT", "TEXT", "FLOAT", "DATE", "BOOLEAN"],
                    key=f"{prefix}_dtype_{col}"
                )
                dtypes[col] = dtype

            # Step 3: Choose primary key
            pk_column = st.selectbox("🔑 Select Primary Key", options=columns, key=f"{prefix}_pk_select")

            row_input = st.text_area(
                "📝 Enter row values (one row per line, comma-separated)",
                placeholder="e.g.\n1, Alice, 100.5\n2, Bob, 200.3",
                key=f"{prefix}_row_input"
            )
            step["data"] = row_input 

            parsed_rows = []
            if row_input:
                try:
                    for line in row_input.strip().splitlines():
                        values = [val.strip() for val in line.split(",")]
                        if len(values) != len(columns):
                            st.error(f"❌ Row `{line}` does not match number of columns.")
                            parsed_rows = []
                            break
                        parsed_rows.append(values)
                except Exception as e:
                    st.error(f"❌ Error parsing row values: {e}")
                    parsed_rows = []
                


            if st.button("✅ Confirm Table Definition", key=f"{prefix}_confirm_create_table"):
                step["columns"] = columns
                step["dtypes"] = dtypes
                step["primary_key"] = pk_column
                step["output_name"] = table_name
                step["data"] = row_input
                st.success(f"✅ Table `{table_name}` with primary key `{pk_column}` defined.")

    elif step_type == "Handle Missing Values":
        st.markdown("### 🩹 Handle Missing Values")

        # Select table
        table_name = st.selectbox("Select Table", list(dataframes.keys()), key=f"{prefix}_missing_table")
        df = dataframes[table_name]

        # Show null summary
        st.subheader("🔍 Null Summary")
        null_summary = df.isnull().sum()
        null_df = null_summary[null_summary > 0].to_frame(name="Null Count")
        if not null_df.empty:
            st.dataframe(null_df)
        else:
            st.success("✅ No missing values in the dataset.")

        # Proceed only if nulls exist
        if not null_df.empty:
            # Select column to handle
            col = st.selectbox("Select Column to Apply Strategy", null_df.index.tolist(), key=f"{prefix}_missing_column")

            # Select strategy
            strategy = st.selectbox("Fill Strategy", ["Drop Rows", "Fill with Mean", "Fill with Median", "Fill with Mode", "Fill with Custom Value"], key=f"{prefix}_missing_strategy")

            # Custom input if selected
            custom_value = None
            if strategy == "Fill with Custom Value":
                custom_value = st.text_input("Enter Custom Fill Value", key=f"{prefix}_missing_custom")
            # Custom input if selected

            # ✅ Always save to step (important for execution engine)
            step["table"] = table_name
            step["column"] = col
            step["strategy"] = strategy
            if custom_value:
                step["custom_value"] = custom_value

            # Run button
            if st.button("Run Missing Value Treatment", key=f"{prefix}_missing_run"):
                st.success("✅ Step configured.")

    elif step_type == "Modify Table Structure":
        st.markdown("### 🛠️ Modify Table Structure")
        
        table = st.selectbox("Select Table", list(dataframes.keys()), key=f"{prefix}_modstruct_table_{id(step)}")
        df = dataframes[table]
        st.markdown("#### 🔍 Table Preview Before Modification")
        st.dataframe(df, use_container_width=True)
        
        action = st.radio(
            "Select Action",
            ["Add Column", "Delete Column", "Add Row", "Delete Row", "Rename Columns", "Convert Data Types"],
            key=f"{prefix}_modstruct_action_{id(step)}"
        )
        
        step["table"] = table
        step["action"] = action

        # 🔹 Add Column

        # 🔹 Add Column
        if action == "Add Column":
            new_col = st.text_input("🆕 New Column Name", key=f"{prefix}_modstruct_addcol_name_{id(step)}_{step.get('action', '')}")
            dtype = st.selectbox("🧬 Select Data Type", ["str", "int", "float"], key=f"{prefix}_modstruct_addcol_dtype_{id(step)}")

            # 🔘 Choose input mode
            value_mode = st.radio(
                "How do you want to assign values?",
                ["Use a single default value for all rows", "Enter different values per row"],
                key=f"{prefix}_modstruct_addcol_mode_{id(step)}"
            )
            step["value_mode"] = value_mode

            # 🧬 Column name and type
            new_col = st.text_input("🆕 New Column Name", key=f"{prefix}_modstruct_addcol_name_{id(step)}")
            dtype = st.selectbox("🧬 Select Data Type", ["str", "int", "float"], key=f"{prefix}_modstruct_addcol_dtype_{id(step)}_{step.get('action', '')}")
            step["new_column"] = new_col
            step["dtype"] = dtype

            df = step.get("data")

            # 📥 Mode 1: Use a default value for all rows
            if value_mode == "Use a single default value for all rows":
                default_value = st.text_input(
                    "📝 Default Value for All Rows",
                    key=f"{prefix}_modstruct_addcol_default_{id(step)}"
                )
                step["default"] = default_value
                step["values"] = []
                step["raw_values"] = ""

                # Preview
                if isinstance(df, pd.DataFrame) and new_col:
                    df[new_col] = default_value
                    st.success(f"✅ Column `{new_col}` added with default value `{default_value}` and type `{dtype}`.")
                    st.dataframe(df.head())

            # 📥 Mode 2: Use per-row values
            else:
                values_text = st.text_input(
                    "📋 Enter Values (comma-separated for each row)",
                    value="1,2,3,,5",
                    key=f"{prefix}_modstruct_addcol_vals_{id(step)}",
                    help="Use commas to separate values. Leave blank between commas to insert NaN."
                )
                step["raw_values"] = values_text

                # Parse and convert values
                raw_list = values_text.strip().split(",") if values_text.strip() else []

                def convert(val):
                    val = val.strip()
                    if val == "":
                        return np.nan
                    try:
                        return {"int": int, "float": float, "str": str}[dtype](val)
                    except:
                        return np.nan

                values_list = [convert(v) for v in raw_list]
                step["values"] = values_list
                default_value = next((v for v in values_list if pd.notna(v)), None)
                step["default"] = default_value  # For SQL default fallback

                # Preview
                st.write(f"✅ Will add column `{new_col}` with `{len(values_list)}` values and type `{dtype}`.")

                if isinstance(df, pd.DataFrame) and new_col:
                    padded_values = values_list + [np.nan] * (len(df) - len(values_list))
                    df[new_col] = pd.Series(padded_values[:len(df)]).astype(dtype if dtype != "str" else "object")
                    st.dataframe(df.head())

        # 🔹 Delete Columns
        elif action == "Delete Column":
            del_cols = st.multiselect("Select Columns to Delete", df.columns, key=f"{prefix}_modstruct_delcols")
            step["columns"] = del_cols

        # 🔹 Add Row
        elif action == "Add Row":
            st.markdown("### ➕ Add Row to Table")

            all_table_names = list(dataframes.keys())  # ✅ Fix here

            table = st.selectbox("Select Table", all_table_names, key=f"{prefix}_addrow_table_{id(step)}")
            step["table"] = table

            if table and table in dataframes:
                columns = dataframes[table].columns.tolist()

                # 🧾 Show expected input format
                example = ', '.join([col for col in columns])
                st.info(f"📌 **Expected order:** `{example}`")


            input_str = st.text_input(f"📝 Enter row values (comma-separated)", key=f"{prefix}_addrow_input_{id(step)}")

            if input_str:
                try:
                    row_values = [x.strip() for x in input_str.split(",")]
                    st.success("✅ Row parsed successfully!")
                    step["values"] = row_values
                except Exception as e:
                    st.error(f"❌ Error parsing row values: {e}")



        # 🔹 Delete Row
        elif action == "Delete Row":
            condition_col = st.selectbox("Condition Column", df.columns, key=f"{prefix}_modstruct_delrow_col")
            condition_val = st.text_input("Value to Match for Deletion", key=f"{prefix}_modstruct_delrow_val")
            step["condition_col"] = condition_col
            step["condition_val"] = condition_val

        # 🔹 Rename Columns
        elif action == "Rename Columns":
            st.markdown("Enter new names for columns (leave blank to keep original):")
            rename_dict = {}
            for col in df.columns:
                new_name = st.text_input(f"Rename `{col}` to:", key=f"{prefix}_modstruct_rename_{col}")
                if new_name and new_name != col:
                    rename_dict[col] = new_name
            step["rename_dict"] = rename_dict

        # 🔹 Convert Data Types
        elif action == "Convert Data Types":
            selected_cols = st.multiselect(
                "Select Columns to Convert",
                df.columns.tolist(),
                key=f"{prefix}_modstruct_dtype_cols"
            )

            dtype_options = ["int", "float", "str", "bool", "datetime"]
            dtype_dict = {}

            if selected_cols:
                st.markdown("#### Select Target Data Type for Each Column:")
                for col in selected_cols:
                    dtype = st.selectbox(
                        f"→ `{col}`:",
                        dtype_options,
                        key=f"{prefix}_modstruct_dtype_for_{col}"
                    )
                    dtype_dict[col] = dtype

                step["dtype_dict"] = dtype_dict

    elif step_type == "Create New Table with Foreign Link":
        st.markdown("### 🧱 Create and Save a New Table (with Optional Foreign Key Column)")

        table_names = list(dataframes.keys())
        use_fk = st.checkbox("🔗 Add Foreign Key Column from Existing Table", key=f"{prefix}_use_fk_{id(step)}")
        step["is_foreign_key_link"] = use_fk

        fk_values = []
        fk_column_name_in_new_table = None
        if use_fk and table_names:
            base_table = st.selectbox("Select Foreign Key Table", table_names, key=f"{prefix}_fk_base_table_{id(step)}")
            base_df = dataframes[base_table]
            fk_column = st.selectbox("Select Foreign Key Column", base_df.columns.tolist(), key=f"{prefix}_fk_column_{id(step)}")
            fk_values = base_df[fk_column].drop_duplicates().tolist()
            
            fk_column_name_in_new_table = fk_column
            step["fk_column_name"] = fk_column_name_in_new_table

        st.markdown("#### ➕ Define Custom Columns (excluding FK)")
        custom_columns_str = st.text_input(
            "Enter Custom Column Names (comma-separated)",
            value=step.get("custom_columns_str", "col1, col2"),
            key=f"{prefix}_custom_columns_str_{id(step)}"
        )
        custom_columns = [col.strip() for col in custom_columns_str.split(",") if col.strip()]

        if use_fk and fk_values:
            full_columns = [fk_column_name_in_new_table] + custom_columns
        else:
            full_columns = custom_columns

        st.markdown("#### 🧬 Select Data Types for Custom Columns")
        dtypes = {}
        if use_fk:
            dtypes[fk_column_name_in_new_table] = str  # Assume FK is string or int
        for col in custom_columns:
            dtype = st.selectbox(
                f"Data Type for {col}",
                ["str", "int", "float", "datetime"],
                key=f"{prefix}_dtype_{col}_{id(step)}"
            )
            dtypes[col] = dtype

        st.markdown("#### 📝 Enter Row Data")
        if use_fk:
            st.caption(f"Include {len(full_columns)} columns per row: FK + your custom columns")
            st.caption(f"Example: `{fk_values[0]},{'value1'},10`")
        else:
            st.caption("Separate rows with `;` and values with `,` — e.g. `val1,1.5; val2,2.3`")

        row_data_input = st.text_area(
            "Enter Data",
            value=step.get("row_data_input", ""),
            key=f"{prefix}_row_data_input_{id(step)}"
        )

        # Parse input rows
        rows = []
        if row_data_input.strip():
            for row in row_data_input.strip().split(";"):
                row = row.strip()
                if not row:  # 🔒 Skip empty rows (from trailing ;)
                    continue
                values = [val.strip() for val in row.split(",")]
                if len(values) == len(full_columns):
                    rows.append(values)
                else:
                    st.warning(f"⚠️ Skipped row due to column mismatch: `{row}`")
        
        if not rows:
            st.error("❌ No valid rows detected. Please ensure all rows have correct number of values.")
            return
        
        if rows:
            new_df = pd.DataFrame(rows, columns=full_columns)

            # Convert column dtypes
            for col, dtype in dtypes.items():
                try:
                    if dtype == "datetime":
                        new_df[col] = pd.to_datetime(new_df[col], errors="coerce")
                    else:
                        new_df[col] = new_df[col].astype(dtype)
                except Exception as e:
                    st.warning(f"⚠️ Could not convert {col} to {dtype}: {e}")

            st.markdown("#### ✅ Final Preview of New Table")
            st.dataframe(new_df)
            # 🧾 SQL Preview

            # ✅ Let user enter the table name
      
          
            # Save all info
            step["data"] = new_df.to_dict(orient="records")
            step["columns"] = new_df.columns.tolist()
            step["dtypes"] = dtypes
            step["custom_columns_str"] = custom_columns_str
            step["row_data_input"] = row_data_input

            output_name = st.text_input("Output Table Name", value=step.get("output_name", "new_table"), key=f"{prefix}_output_name_{id(step)}")
            step["output_name"] = output_name
            step["data"] = new_df.to_dict(orient="records")
            step["columns"] = new_df.columns.tolist()
            
        else:
            st.error("❌ Please enter row data with the correct number of values.")

    # Inside build_step_form, just do:
        
    try:
        sql_code = generate_sql_query_for_step(step)
    except Exception as e:
        sql_code = f"-- Error generating SQL: {e}"

    st.markdown("🧾 **SQL for this step:**")
    st.code(sql_code, language="sql")



def apply_step(step, dataframes):
    try:
        if step["type"] == "Filter Rows":
            df = dataframes[step["table"]]
            return df.query(step["expression"])

        elif step["type"] == "Group By":
            df = dataframes[step["table"]]
            return df.groupby(step["group_cols"]).agg(step["aggregations"]).reset_index()

        elif step["type"] == "Sort Rows":
            df = dataframes[step["table"]]
            return df.sort_values(by=step["columns"], ascending=step["ascending"])

        elif step["type"] == "Join Tables":

            left = dataframes[step["left_table"]].copy()
            right = dataframes[step["right_table"]].copy()
            left_on = step["left_on"]
            right_on = step["right_on"]
            right_on = step["right_on"]
            join_type = step["join_type"]
            is_foreign_key = step.get("is_foreign_key_link", False)

            # Optional casting to string
            if step.get("cast_to_str", False):
                try:
                    left[left_on] = left[left_on].astype(str)
                    right[right_on] = right[right_on].astype(str)
                except Exception as e:
                    st.error(f"⚠️ Failed to convert join columns to string: {e}")
                    return None
                
            if left_on not in left.columns or right_on not in right.columns:
                    st.error("❌ Foreign key columns not found.")
                    return None
                

                # Foreign key filtering (only if explicitly selected)
            if is_foreign_key:
                
                original_count = len(right)
                right = right[right[right_on].isin(left[left_on])]
                filtered_count = len(right)

                st.info(f"🔗 Foreign key mode: Filtered right table from {original_count} to {filtered_count} rows based on foreign key match.")


            try:
                return pd.merge(left, right, how=step["join_type"], left_on=left_on, right_on=right_on)
            except Exception as e:
                st.error(f"❌ Join error: {e}")
                return None

        elif step["type"] == "Aggregate Column":
            df = dataframes[step["table"]]
            col = step["column"]
            func = step["function"]
            alias = step["alias"]

            # Perform aggregation and return as 1-row DataFrame
            result_value = getattr(df[col], func)()
            return pd.DataFrame({alias: [result_value]})

        elif step["type"] == "Modify Column":
            table = step["table"]
            alias = step["alias"]
            result_df = dataframes[table].copy()

            try:
                if step.get("use_manual_expr", False):
                    tables = dataframes  # required for eval scope
                    result = eval(step["expression"])

                else:
                    operator = step.get("operator", "+")
                    rhs_mode = step.get("rhs_mode", "Manual constant")
                    is_string_op = operator in ["+", "==", "!="]

                    # --- Prepare Left-Hand Side
                    if is_string_op:
                        col1_series = dataframes[step["col1_table"]][step["col1"]].astype(str)
                    else:
                        col1_series = pd.to_numeric(dataframes[step["col1_table"]][step["col1"]], errors="coerce").fillna(0)

                    # --- Prepare Right-Hand Side
                    if rhs_mode == "Column from another table":
                        if is_string_op:
                            col2_series = dataframes[step["col2_table"]][step["col2"]].astype(str)
                        else:
                            col2_series = pd.to_numeric(dataframes[step["col2_table"]][step["col2"]], errors="coerce").fillna(0)
                    else:
                        const = step.get("constant", "0")
                        if is_string_op:
                            col2_series = str(const)
                        else:
                            try:
                                col2_series = float(const)
                            except:
                                col2_series = 0.0

                    # --- Perform the Operation
                    if isinstance(col2_series, pd.Series):
                        result = eval(f"col1_series {operator} col2_series")
                    else:
                        # constant value
                        result = eval(f"col1_series {operator} col2_series")

                # --- Store result
                result_df[alias] = result
                return result_df

            except Exception as e:
                st.error(f"❌ Error applying operation: {e}")
                return result_df



            except Exception as e:
                st.error(f"❌ Failed to create new table: {e}")
                return pd.DataFrame()

        elif step["type"] == "Create & Save New Table":
            try:
                # Use existing data
                use_existing = step.get("use_existing", False)
                base_table = step.get("base_table", "")
                base_columns = step.get("base_columns", [])

                # Get base dataframe if applicable
                base_df = dataframes.get(base_table) if use_existing and base_table in dataframes else None

                # Convert stored row data to DataFrame
                custom_columns = step.get("columns", [])
                row_data = step.get("data", [])
                dtypes = step.get("dtypes", {})

                # If no base_df and no row data, throw error
                if (not row_data or len(row_data) == 0) and base_df is None:
                    raise ValueError("No data provided from base table or manual input.")

                # Build custom DataFrame from row data
                new_df = pd.DataFrame(row_data, columns=custom_columns)

                # Apply datatypes
                for col, dtype in dtypes.items():
                    try:
                        if dtype == "datetime":
                            new_df[col] = pd.to_datetime(new_df[col], errors="coerce")
                        else:
                            new_df[col] = new_df[col].astype(dtype)
                    except Exception as e:
                        st.warning(f"⚠️ Could not convert {col} to {dtype}: {e}")

                # Merge with base table columns if needed
                if base_df is not None and base_columns:
                    new_df = pd.concat([base_df[base_columns].reset_index(drop=True), new_df], axis=1)

                # Save result
                output_name = step.get("output_name", "new_table")
                dataframes[output_name] = new_df
                st.success(f"✅ Created new table: `{output_name}`")
                st.dataframe(new_df)

            except Exception as e:
                st.error(f"❌ Error applying step: {e}")

        elif step["type"] == "INSERT":
            try:
                table = step["table"]
                if table not in dataframes:
                    raise ValueError(f"❌ Table `{table}` not found.")

                df = dataframes[table]
                columns = step.get("columns", [])
                values = step.get("values", {})  # dict of {col: val}

                # Create new row aligned with df columns
                new_row = [values.get(col, None) for col in df.columns]
                new_df = pd.concat([df, pd.DataFrame([new_row], columns=df.columns)], ignore_index=True)

                dataframes[table] = new_df
                st.success(f"✅ INSERT applied: Row added to `{table}`")
                st.dataframe(new_df)
                return new_df

            except Exception as e:
                st.error(f"❌ Error applying INSERT: {e}")
                return df if "df" in locals() else pd.DataFrame()

        elif step["type"] == "UPDATE":
            try:
                table = step["table"]
                if table not in dataframes:
                    raise ValueError(f"❌ Table `{table}` not found.")

                df = dataframes[table]
                condition_col = step.get("condition_col")
                condition_val = step.get("condition_val")
                update_col = step.get("update_col")
                new_value = step.get("new_value")

                if not condition_col or not update_col:
                    raise ValueError("⚠️ Missing condition or update column.")

                updated_rows = df[condition_col] == condition_val
                df.loc[updated_rows, update_col] = new_value

                dataframes[table] = df
                st.success(f"✅ UPDATE applied on `{table}`: {updated_rows.sum()} row(s) modified.")
                st.dataframe(df)
                return df

            except Exception as e:
                st.error(f"❌ Error applying UPDATE: {e}")
                return df if "df" in locals() else pd.DataFrame()

        elif step["type"] == "DELETE":
            try:
                table = step["table"]
                if table not in dataframes:
                    raise ValueError(f"❌ Table `{table}` not found.")

                df = dataframes[table]
                condition_col = step.get("condition_col")
                condition_val = step.get("condition_val")

                if not condition_col:
                    raise ValueError("⚠️ DELETE step missing condition column.")

                before_count = len(df)
                df = df[df[condition_col] != condition_val].reset_index(drop=True)
                after_count = len(df)

                deleted = before_count - after_count
                dataframes[table] = df
                st.success(f"🗑️ DELETE applied on `{table}`: {deleted} row(s) removed.")
                st.dataframe(df)
                return df

            except Exception as e:
                st.error(f"❌ Error applying DELETE: {e}")
                return df if "df" in locals() else pd.DataFrame()

        elif step["type"] == "Set Operation":
   
            table1 = step["table1"]
            table2 = step["table2"]
            operation = step["operation"]

            if table1 not in dataframes or table2 not in dataframes:
                st.error("❌ One or both tables not found.")
                return dataframes

            df1 = dataframes[table1]
            df2 = dataframes[table2]

            try:
                if not df1.columns.equals(df2.columns):
                    st.error("❌ Columns must match in name and order for Set Operation.")
                    return dataframes

                if operation == "UNION":
                    result = pd.concat([df1, df2]).drop_duplicates().reset_index(drop=True)
                elif operation == "UNION ALL":
                    result = pd.concat([df1, df2]).reset_index(drop=True)
                elif operation == "INTERSECT":
                    result = pd.merge(df1, df2, how="inner")
                elif operation == "EXCEPT":
                    result = df1.merge(df2, how="outer", indicator=True).query("_merge == 'left_only'").drop(columns=["_merge"])
                else:
                    st.warning("⚠️ Invalid Set Operation selected.")
                    return dataframes

                new_name = f"{table1}_{operation.replace(' ', '_')}_{table2}"
                dataframes[new_name] = result
                st.success(f"✅ Set Operation completed. Result saved as `{new_name}`.")
                st.dataframe(result)

            except Exception as e:
                st.error(f"❌ Error in Set Operation: {e}")


        elif step["type"] == "Handle Missing Values":
            table = step["table"]
            col = step["column"]
            strategy = step["strategy"]
            custom_value = step.get("custom_value")

            df = dataframes[table]

            if strategy == "Drop Rows":
                df = df[df[col].notnull()]
                sql = f"DELETE FROM {table} WHERE {col} IS NULL"

            elif strategy == "Fill with Mean":
                fill_val = df[col].mean()
                df[col] = df[col].fillna(fill_val)
                sql = f"UPDATE {table} SET {col} = {fill_val} WHERE {col} IS NULL"

            elif strategy == "Fill with Median":
                fill_val = df[col].median()
                df[col] = df[col].fillna(fill_val)
                sql = f"UPDATE {table} SET {col} = {fill_val} WHERE {col} IS NULL"

            elif strategy == "Fill with Mode":
                fill_val = df[col].mode().iloc[0]
                df[col] = df[col].fillna(fill_val)
                sql = f"UPDATE {table} SET {col} = '{fill_val}' WHERE {col} IS NULL"

            elif strategy == "Fill with Custom Value":
                fill_val = custom_value
                df[col] = df[col].fillna(fill_val)
                sql = f"UPDATE {table} SET {col} = '{fill_val}' WHERE {col} IS NULL"

            dataframes[table] = df
            step["sql"] = sql

        elif step["type"] == "Modify Table Structure":
            table = step["table"]
            action = step["action"]
            df = dataframes[table]

            if step["type"] == "Modify Table Structure" and step.get("action")== "Add Column":
                new_col = step["new_column"]
                dtype = step.get("dtype", "str")
                values = step.get("values", [])
                # Ensure values are padded/truncated to match DataFrame length
                values_padded = values + [np.nan] * (len(df) - len(values))
                df[new_col] = pd.Series(values_padded[:len(df)])

                # Cast to correct dtype
                if dtype == "int":
                    df[new_col] = pd.to_numeric(df[new_col], errors="coerce").astype("Int64")
                elif dtype == "float":
                    df[new_col] = pd.to_numeric(df[new_col], errors="coerce")
                else:
                    df[new_col] = df[new_col].astype(str).replace("nan", np.nan)

            elif step["type"] == "Modify Table Structure" and step.get("action") == "Delete Column":
                cols = step["columns"]
                df.drop(columns=cols, inplace=True)

            elif step["type"] == "Modify Table Structure" and step.get("action") == "Add Row":
                table_name = step.get("table")
                row_values = step.get("values", [])

                if table_name and row_values:
                    df = dataframes.get(step["table"])
                    if df is not None:
                        if len(row_values) == len(df.columns):
                            new_row_df = pd.DataFrame([row_values], columns=df.columns)
                            dataframes[table_name]  = pd.concat([df, new_row_df], ignore_index=True)
                            st.success(f"✅ Row added to {table_name}")
                        else:
                            st.error("❌ Row length does not match number of columns in the table")
                    else:
                        st.error(f"❌ Table '{table_name}' not found")


            elif step["type"] == "Modify Table Structure" and step.get("action") == "Delete Row":
                    col = step["condition_col"]
                    val = step["condition_val"].strip()

                    # Try to cast to the same type as column
                    try:
                        val_casted = df[col].dtype.type(val)
                    except Exception:
                        val_casted = val

                    if df[col].dtype == "O":  # object/string column
                        df = df[~df[col].astype(str).str.strip().str.lower().eq(str(val_casted).strip().lower())]
                    else:
                        df = df[df[col] != val_casted]
                
            elif step["type"] == "Modify Table Structure" and step.get("action") == "Rename Columns":
                rename_dict = step.get("rename_dict", {})
                if rename_dict:
                    df.rename(columns=rename_dict, inplace=True)

            elif step["type"] == "Modify Table Structure" and step.get("action")== "Convert Data Types":
                dtype_dict = step.get("dtype_dict", {})
                for col, dtype in dtype_dict.items():
                    try:
                        if dtype == "datetime":
                            df[col] = pd.to_datetime(df[col], errors="coerce")
                        elif dtype == "int":
                            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                        elif dtype == "float":
                            df[col] = pd.to_numeric(df[col], errors="coerce")
                        elif dtype == "bool":
                            df[col] = df[col].astype("bool")
                        else:
                            df[col] = df[col].astype(dtype)
                    except Exception as e:
                        st.warning(f"⚠️ Failed to convert `{col}` to {dtype}: {e}")

            dataframes[table] = df
            st.markdown(f"### 📄 Updated `{table}` Table Preview")
            st.dataframe(df)

        elif step["type"] ==  "Create New Table with Foreign Link":
            table = step.get("output_name") or step.get("new_table_name", "new_table")
            # Load metadata
            rows = step.get("data", [])
            columns = step.get("columns", [])
            dtypes = step.get("dtypes", {})
            output_name = step.get("Output Table Name", "new_table")

            # Create DataFrame from stored rows
            if not rows or not columns:
                raise ValueError("No data provided to create the new table.")
            
            df = pd.DataFrame(rows,columns=columns)

            # Apply column names if not already set
            if df.columns.tolist() != columns:
                df.columns = columns

            # Apply dtypes
            for col, dtype in dtypes.items():
                try:
                    if dtype == "datetime":
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                    else:
                        df[col] = df[col].astype(dtype)
                except Exception as e:
                    print(f"[Warning] Failed to cast {col} to {dtype}: {e}")
            
                    # ✅ Generate SQL
            dtype_map = {
                "int": "INTEGER",
                "float": "FLOAT",
                "str": "VARCHAR(255)",
                "datetime": "TIMESTAMP"
            }


                # CREATE TABLE
            column_defs = []
            for col in columns:
                dtype = dtypes.get(col, "str")
                sql_dtype = dtype_map.get(dtype, "VARCHAR(255)")
                column_defs.append(f"{col} {sql_dtype}")
            create_sql = f"CREATE TABLE {table} ({', '.join(column_defs)});"

            # INSERT INTO
            insert_statements = []
            for _, row in df.iterrows():
                formatted_values = []
                for col in columns:
                    val = row[col]
                    dtype = dtypes.get(col, "str")
                    if pd.isnull(val):
                        formatted_values.append("NULL")
                    elif dtype in ["str", "datetime"]:
                        val = str(val).replace("'", "''")
                        formatted_values.append(f"'{val}'")
                    else:
                        formatted_values.append(str(val))
                insert_statements.append(f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(formatted_values)});")

            # Save full SQL
            step["sql_code"] = create_sql + "\n" + "\n".join(insert_statements)



            # Return named output
            return df

        elif step["type"] == "Create Table with Primary Key":
            try:
                columns = step.get("columns", [])
                dtypes_sql = step.get("dtypes", {})
                pk = step.get("primary_key", "")
                table_name = step.get("output_name", "new_table")
                rows_input = step.get("data", "")  # 🆕 get raw row input

                # Convert to pandas dtypes
                pandas_dtypes = {
                    col: sql_to_pandas_dtype.get(dtype.upper(), "object")
                    for col, dtype in dtypes_sql.items()
                }

                # Parse input rows
                rows = []
                if rows_input:
                    for line in rows_input.strip().split("\n"):
                        row = [val.strip() for val in line.split(",")]
                        rows.append(row)

                # Create DataFrame
                df = pd.DataFrame(rows, columns=columns).astype(pandas_dtypes)
                dataframes[table_name] = df

                for col, dtype in pandas_dtypes.items():
                    try:
                        df[col] = df[col].astype(dtype)
                    except Exception as e:
                        st.warning(f"⚠️ Failed to convert `{col}` to `{dtype}`: {e}")

                dataframes[table_name] = df

                st.success(f"✅ Table `{table_name}` created with primary key `{pk}`.")
                st.write("📊 Preview of created table:")
                st.dataframe(df)

            except Exception as e:
                st.error(f"❌ Error creating table: {e}")

    except Exception as e:
        st.error(f"❌ Error executing step {step['type']}: {e}")
        return None

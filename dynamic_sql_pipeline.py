import streamlit as st
import os
import pandas as pd

def dynamic_sql_pipeline_ui(prefix="dynamic"):
    

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

    # Step types
    SQL_STEP_OPTIONS = [
        "Filter Rows", "Sort Rows", "Group By", "Join Tables","Create Table with Primary Key",
        "Aggregate Column", "Modify Column", "Create & Save New Table","INSERT", "UPDATE", "DELETE","Set Operation","Handle Missing Values","Modify Table Structure","Create New Table with Foreign Link"
    ]

    # Init pipeline and outputs
    if "sql_pipeline" not in st.session_state:
        st.session_state.sql_pipeline = []
    if "sql_step_outputs" not in st.session_state:
        st.session_state.sql_step_outputs = {}

    dataframes = st.session_state.get("uploaded_tables", {})
    if not dataframes:
        st.warning("⚠️ Upload at least one dataset first.")
        return
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
    }
    </style>
    """
    st.markdown(button_style, unsafe_allow_html=True)


    for i in range(0, len(SQL_STEP_OPTIONS), num_cols):
        cols = st.columns(num_cols)
        for j, opt in enumerate(SQL_STEP_OPTIONS[i:i+num_cols]):
            with cols[j]:
                if st.button(opt, key=f"{prefix}_step_btn_{opt}"):
                    st.session_state.sql_pipeline.append({"type": opt})
                    st.session_state.selected_step_index = len(st.session_state.sql_pipeline) - 1
                    st.rerun()

 
    # Track outputs
    for i, step in enumerate(st.session_state.sql_pipeline):
        with st.expander(f"Step {i+1}: {step['type']}", expanded=True):
            step_type = st.selectbox("Step Type", SQL_STEP_OPTIONS, index=SQL_STEP_OPTIONS.index(step["type"]), key=f"step_type_{i}")
            step["type"] = step_type

            # --- Select input source (with emoji-labeled display names) ---
            uploaded_keys = [f"📥 {k}" for k in dataframes.keys()]
            step_keys = [f"🪜 step_{j+1}" for j in range(i)]
            all_sources = uploaded_keys + step_keys

            # Handle previous value
            prev_input = step.get("input_source", uploaded_keys[0].split(" ", 1)[1])
            matching_label = f"📥 {prev_input}" if f"📥 {prev_input}" in all_sources else f"🪜 {prev_input}"
            default_index = all_sources.index(matching_label) if matching_label in all_sources else 0

            display_input = st.selectbox("📊 Input Table (Uploaded or Previous Step)", all_sources, index=default_index, key=f"input_source_{i}")
            actual_source_key = display_input.split(" ", 1)[1]  # Remove emoji
            step["input_source"] = actual_source_key


            # --- Merge inputs for build form and apply step ---
            available_data = dataframes.copy()
            available_data.update(st.session_state.sql_step_outputs)

            # Form builder
            from sql_steps import build_step_form, apply_step
            build_step_form(i, step, available_data, prefix="dynamic")

            # Actions
            col1, col2 = st.columns([1, 1])
            with col1:
                if st.button("❌ Delete", key=f"dynamic_delete_{i}"):
                    st.session_state.sql_pipeline.pop(i)
                    st.rerun()

            # --- Apply step using dynamic source ---
            source_df = available_data.get(actual_source_key)
            if source_df is not None:
                df = apply_step(step, {actual_source_key: source_df})
                if df is not None:
                    st.markdown(f"#### 📄 Output of Step {i+1}")
                    st.dataframe(df)
                    step_key = f"step_{i+1}"
                    st.session_state.sql_step_outputs[step_key] = df

                    filename = f"{step_key}_{step['type'].replace(' ', '_').lower()}.csv"
                    filepath = os.path.join(step_output_dir, filename)
                    df.to_csv(filepath, index=False)

                    with open(filepath, "rb") as f:
                        st.download_button(f"⬇️ Download {step_key} Output", f, file_name=filename)

import streamlit as st
import pandas as pd
import os
import io

def load_file(filename, file_obj):
    try:
        if filename.endswith(".csv"):
            return pd.read_csv(file_obj)
        elif filename.endswith(".xlsx"):
            return pd.read_excel(file_obj)
    except Exception as e:
        st.error(f"Failed to load {filename}: {e}")
        return None


def upload_data():
    st.sidebar.markdown("## 📂 Load Your Data")
    upload_mode = st.sidebar.radio("Select input method", ["Upload File(s)", "Enter Folder Path"])

    if "uploaded_tables" not in st.session_state:
        st.session_state.uploaded_tables = {}

    if upload_mode == "Upload File(s)":
        uploaded_files = st.sidebar.file_uploader("Upload CSV/XLSX files", type=["csv", "xlsx"], accept_multiple_files=True)
        if uploaded_files:
            st.session_state.upload_folder = os.path.abspath("sql_outputs")
            for file in uploaded_files:
                df = load_file(file.name, file)
                if df is not None:
                    st.session_state.uploaded_tables[file.name] = df

    elif upload_mode == "Enter Folder Path":

        folder_path = st.sidebar.text_input("Enter folder path containing .csv or .xlsx files")
        
        if folder_path and os.path.isdir(folder_path):
            st.session_state.upload_folder = os.path.abspath(folder_path)  # ✅ Add this line

            files = [f for f in os.listdir(folder_path) if f.lower().endswith((".csv", ".xlsx"))]
            
            if not files:
                st.sidebar.warning("⚠️ No CSV or Excel files found in this folder.")
            else:
                for fname in files:
                    fpath = os.path.join(folder_path, fname)
                    df = load_file(fname, fpath)
                    if df is not None:
                        st.session_state.uploaded_tables[fname] = df
                st.sidebar.success(f"✅ Loaded {len(files)} files from folder.")
        
        elif folder_path:
            st.sidebar.warning("⚠️ Invalid folder path.")

def show_file_info():
    if st.session_state.get("uploaded_tables"):
        st.sidebar.markdown("## 📄 File Info")
        for name, df in st.session_state.uploaded_tables.items():
            with st.sidebar.expander(f"📘 {name}"):
                st.write(f"**Shape:** {df.shape}")
                st.write("**Columns:** {df.columns.tolist()}")
                buf = io.StringIO()
                df.info(buf=buf)
                st.code(buf.getvalue(), language="text")
                st.dataframe(df)

 
        return True
    else:
        st.info("👈🏼 Upload📌 or enter a valid file path to continue.")
        return False


def display_shared_columns():
    
    if not st.session_state.get("uploaded_tables"):
        st.warning("⚠️ No data loaded yet.")
        return

    available_datasets = list(st.session_state.uploaded_tables.keys())
    dataset_labels = [name.rsplit(".", 1)[0] for name in available_datasets]

    selected_labels = st.multiselect(
        "📌 Select Datasets to Compare",
        options=dataset_labels,
        default=dataset_labels
    )

    if not selected_labels:
        st.info("Please select at least one dataset.")
        return

    # All dataset columns
    dataset_columns_full = {
        fname.rsplit(".", 1)[0]: set(df.columns)
        for fname, df in st.session_state.uploaded_tables.items()
    }

    if len(selected_labels) == 1:
        selected_dataset = selected_labels[0]
        selected_cols = dataset_columns_full[selected_dataset]

        shared_cols = {}
        unique_cols = []

        for col in sorted(selected_cols):
            used_in = [
                ds for ds, cols in dataset_columns_full.items()
                if col in cols and ds != selected_dataset
            ]
            if used_in:
                shared_cols[col] = used_in
            else:
                unique_cols.append(col)

        st.markdown(f"## 📊 Column Comparison for `{selected_dataset}`")

        # 🔵 Shared Columns
        if shared_cols:
            st.subheader("🔵 Shared Columns")
            for col, others in shared_cols.items():
                st.markdown(f"- **{col}** → Also in: `{', '.join(others)}`")
        else:
            st.markdown("🔵 *No shared columns with other datasets.*")

        st.markdown("---")

        # 🟢 Unique Columns
        if unique_cols:
            st.subheader("🟢 Unique Columns")
            for col in unique_cols:
                st.markdown(f"- **{col}**")
        else:
            st.markdown("🟢 *No unique columns in this dataset.*")

    else:
        # MULTI-DATASET LOGIC
        selected_dataset_columns = {
            ds: dataset_columns_full[ds]
            for ds in selected_labels
        }

        # Compute shared and unique columns
        all_columns = {}
        for dataset, columns in selected_dataset_columns.items():
            for col in columns:
                if col not in all_columns:
                    all_columns[col] = set()
                all_columns[col].add(dataset)

        shared = {col: sorted(list(dsets)) for col, dsets in all_columns.items() if len(dsets) > 1}
        unique = {dataset: [] for dataset in selected_dataset_columns}
        for col, datasets in all_columns.items():
            if len(datasets) == 1:
                only_dataset = list(datasets)[0]
                unique[only_dataset].append(col)

        st.markdown("## 🔗 Shared Columns & Dataset Overview")

        # ✨ Shared Columns
        st.subheader("✨ Shared Columns")
        if shared:
            for col, dsets in shared.items():
                st.markdown(f"- **{col}** → `{', '.join(dsets)}`")
        else:
            st.markdown("*No shared columns among selected datasets.*")

        st.markdown("---")

        # 🧩 Unique Columns
        st.subheader("🧩 Unique Columns by Dataset")
        for dataset, columns in unique.items():
            if columns:
                st.markdown(f"**{dataset}** → `{', '.join(columns)}`")
            else:
                st.markdown(f"**{dataset}** → *No unique columns*")

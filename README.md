# 🧠 SQLFlow

**Visual SQL Builder for CSV Data**  
Build SQL queries step-by-step — no code required.

---

## ⚡ What It Does

- 📂 Upload CSV files  
- 🧱 Add steps: Filter, Join, GroupBy, Sort, Set Ops  
- 🧾 Auto-generate SQL at every stage  
- 👁️ Preview output tables instantly  
- 💾 Download SQL & final dataset

---

## 🧠 Modules

| File | What It Does |
|------|---------------|
| `sql.py` | Main Streamlit app |
| `sql_steps.py` | UI for building SQL steps |
| `sql_generator.py` | Generates SQL code |
| `dynamic_sql_pipeline.py` | Applies steps to dataframes |
| `file_loader.py` | Handles CSV file uploads |

---

## 🚀 Getting Started

```bash
pip install -r requirements.txt
streamlit run sql.py

import streamlit as st
from file_loader import upload_data, show_file_info, display_shared_columns
from sql_steps import sql_pipeline_ui
from dynamic_sql_pipeline import dynamic_sql_pipeline_ui 
#from trail import sql_pipeline_ui
st.set_page_config(page_title="🧩 SQL Pipeline Builder", layout="wide")



st.markdown(
    """
    <style>
    .wave-container {
        display: flex;
        justify-content: center;
        align-items: center;
        margin-top: 60px;
        flex-direction: column;
    }

    .wave-text {
        font-size: 4em;
        font-weight: bold;
        color: ##e67e22;
        font-family: 'Segoe UI', sans-serif;
        letter-spacing: 0.08em;
        display: flex;
        gap: 0.05em;
    }

    .wave-text span {
        display: inline-block;
        animation: wave 1.8s ease-in-out infinite;
    }

    .reflection {
        transform: scaleY(-1);
        opacity: 0.3;
        filter: blur(1.2px);
        margin-top: 10px;
        pointer-events: none;
    }

    /* Animate individual letters with delay */
    .wave-text span:nth-child(1)  { animation-delay: 0.0s; }
    .wave-text span:nth-child(2)  { animation-delay: 0.1s; }
    .wave-text span:nth-child(3)  { animation-delay: 0.2s; }
    .wave-text span:nth-child(4)  { animation-delay: 0.3s; }
    .wave-text span:nth-child(5)  { animation-delay: 0.4s; }
    .wave-text span:nth-child(6)  { animation-delay: 0.5s; }
    .wave-text span:nth-child(7)  { animation-delay: 0.6s; }
    .wave-text span:nth-child(8)  { animation-delay: 0.7s; }
    .wave-text span:nth-child(9)  { animation-delay: 0.8s; }
    .wave-text span:nth-child(10) { animation-delay: 0.9s; }
    .wave-text span:nth-child(11) { animation-delay: 1.0s; }
    .wave-text span:nth-child(12) { animation-delay: 1.1s; }
    .wave-text span:nth-child(13) { animation-delay: 1.2s; }
    .wave-text span:nth-child(14) { animation-delay: 1.3s; }
    .wave-text span:nth-child(15) { animation-delay: 1.4s; }
    .wave-text span:nth-child(16) { animation-delay: 1.5s; }

    @keyframes wave {
        0%, 100% { transform: translateY(0); }
        50% { transform: translateY(-12px); }
    }
    </style>

    <div class="wave-container">
        <div class="wave-text">
            <span>🧩</span><span> </span>
            <span>V</span><span>i</span><span>s</span><span>u</span><span>a</span><span>l</span><span> </span>
            <span>Q</span><span>u</span><span>e</span><span>r</span><span>y</span><span> </span>
            <span>B</span><span>u</span><span>i</span><span>l</span><span>d</span><span>e</span><span>r</span>
        </div>
        <div class="wave-text reflection">
            <span>🧩</span><span> </span>
            <span>V</span><span>i</span><span>s</span><span>u</span><span>a</span><span>l</span><span> </span>
            <span>Q</span><span>u</span><span>e</span><span>r</span><span>y</span><span> </span>
            <span>B</span><span>u</span><span>i</span><span>l</span><span>d</span><span>e</span><span>r</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)


st.divider()

# 1️⃣ Upload & Display Files
upload_data()

has_data = show_file_info()

# 2️⃣ Build SQL Pipeline

if has_data:

    with st.expander("🔍 Column Comparison Across Tables", expanded=True):
        display_shared_columns()
    

    tab1, tab2 = st.tabs(["🧩 Basic SQL Pipeline", "🔗 Dynamic SQL Pipeline"])

    with tab1:
        sql_pipeline_ui(prefix="basic")

    with tab2:
        dynamic_sql_pipeline_ui(prefix="dynamic")




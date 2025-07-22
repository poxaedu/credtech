import streamlit as st

pages = [
    st.Page(
        "home.py",
        title="Home",
        icon="🏠" # Using emoji icon for simplicity
    ),
    st.Page(
        "pages/visao.py", # Assumed path for new page
        title="Visão Geral",
        icon="📊"
    ),
    st.Page(
        "pages/segmento.py", # Assumed path for new page
        title="Análise por Segmento",
        icon="👥"
    ),
    st.Page(
        "pages/cluster.py", # Assumed path for new page
        title="Análise de Cluster",
        icon="🔍"
    ),
    st.Page(
        "pages/temporal.py", # Assumed path for new page
        title="Tendência Temporal",
        icon="📈"
    ),
    st.Page(
        "pages/comparativo_riscos.py", # Assumed path for new page
        title="Comparativo de Riscos",
        icon="⚖️"
    ),
]

page = st.navigation(pages)
page.run()
st.divider()
st.sidebar.caption(
    "This dashboard was developed by X-Men Squad"
)

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

def chatbot_ui(context_keys: list = None, model: str = "mistral-7b"):
    """Streamlit chatbot UI with multiple dataframe contexts"""

    if "messages" not in st.session_state:
        st.session_state.messages = []

    st.markdown("## 💬 Cortex Chat Assistant")

    # --- Display chat messages like bubbles ---
    for msg in st.session_state.messages:
        if msg["role"] == "user":
            st.markdown(
                f"<div style='text-align: right; background-color: #DCF8C6; color: #1a237e; padding: 8px; border-radius: 10px; margin: 4px 0; display: inline-block;'>🧑 {msg['content']}</div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"<div style='text-align: left; background-color: #F1F0F0; color: #1a237e; padding: 8px; border-radius: 10px; margin: 4px 0; display: inline-block;'>🤖 {msg['content']}</div>",
                unsafe_allow_html=True,
            )

    # --- Chat input (Enter to send) ---
    user_input = st.chat_input("Type your question...")  # replaces text_input + button
    if user_input:
        st.session_state.messages.append({"role": "user", "content": user_input})

        # --- Build context from multiple dfs ---
        context = ""
        if context_keys:
            for key in context_keys:
                if key in st.session_state:
                    df = st.session_state[key]
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        sample = df.head(5).to_string(index=False)
                        context += f"\n[{key} sample]\n{sample}\n"

        # --- Call Cortex ---
        query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{model}',
                $$You are a financial analyst. Use the following data samples to answer:

                {context}

                Question: {user_input}
                Give only the final answer in 1–2 sentences. Do not show calculations or code.
                $$
            ) AS RESPONSE
        """
        try:
            df_resp = session.sql(query).to_pandas()
            bot_answer = df_resp["RESPONSE"].iloc[0]
        except Exception as e:
            bot_answer = f"⚠️ Error calling Cortex: {e}"

        st.session_state.messages.append({"role": "bot", "content": bot_answer})
        st.rerun()

    # --- Clear chat button ---
    if st.button("🗑️ Clear Chat"):
        st.session_state.messages = []
        st.rerun()

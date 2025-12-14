import os
import json
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

HF_TOKEN = os.getenv("HF_TOKEN")

SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AGENT_LOG_PATH =os.path.join(SCRIPTS_DIR, "agent", "AGENT_LOGS.JSON") 
QUERY_FILE = os.path.join(SCRIPTS_DIR, "agent", "user_query.txt")


class HuggingFaceSQLAgent:
    def __init__(self, csv_path, model="HuggingFaceH4/zephyr-7b-beta", ):
        self.model = model
        self.hf_token = HF_TOKEN
        self.CSV_PATH = csv_path

    def _call_hf(self, prompt: str) -> str:
        if not self.hf_token:
            return "[OpenRouter API Error] OPENROUTER_KEY missing"

        url = "https://openrouter.ai/api/v1/chat/completions"

        headers = {
            "Authorization": f"Bearer {self.hf_token}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": self.model,  # e.g., "nex-agi/deepseek-v3.1-nex-n1:free"
            "messages": [
                {"role": "system", "content": "You are an AI assistant that converts natural language questions into SQL. Only output valid SQL. No explanations."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 256,
            "temperature": 0.2
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=120)
            
            if response.status_code != 200:
                return f"[HF Error] {response.status_code}: {response.text}"

            data = response.json()
            return data["choices"][0]["message"]["content"].strip()

        except Exception as e:
            return f"[HF Exception] {e}"


    def load_user_query(self) -> str:
        with open(QUERY_FILE, "r") as f:
            return f.read().strip()

    def load_column_names(self) -> str:
        df = pd.read_csv(self.CSV_PATH, nrows=1)
        return ", ".join(df.columns)

    def build_prompt(self, user_query: str, columns: str) -> str:
        return f"""
        User Question:
        {user_query}

        Available Columns:
        {columns}

        Generate a SQL query that answers the question.
        """

    def log_result(self, user_query: str, sql: str):
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "user_query": user_query,
            "agent_response": sql,
        }

        if os.path.exists(AGENT_LOG_PATH):
            with open(AGENT_LOG_PATH, "r") as f:
                logs = json.load(f)
        else:
            logs = []

        logs.append(entry)

        with open(AGENT_LOG_PATH, "w") as f:
            json.dump(logs, f, indent=2)

    def run(self):
        user_query = self.load_user_query()
        columns = self.load_column_names()
        prompt = self.build_prompt(user_query, columns)
        sql = self._call_hf(prompt)
        self.log_result(user_query, sql)
        return sql


def process_with_ai_agent(final_stocks_filepath):
    agent = HuggingFaceSQLAgent(model="nex-agi/deepseek-v3.1-nex-n1:free", csv_path=final_stocks_filepath)
    sql = agent.run()
    print("Generated SQL:\n", sql)


import pandas as pd
from pandasai import Agent, SmartDataframe
from pandasai.llm import OpenAI
import os

llm = OpenAI(api_token=os.getenv("OPENAI_KEY"), temperature=0, seed=42)

df = SmartDataframe(
            pd.read_csv('https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv'),
            config={
                "llm": llm,
                "max_retries": 5,
                "enforce_privacy": True,
                "enable_charts": False,
                "enable_cache": False,
            },
        )

# data=pd.read_csv('https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv')
# print(data.head())
out = df.chat('how many people were on the titanic?')
print(out)
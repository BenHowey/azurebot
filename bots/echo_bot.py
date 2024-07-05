# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

from botbuilder.core import ActivityHandler, MessageFactory, TurnContext
from botbuilder.schema import ChannelAccount
import pandas as pd
from pandasai.llm import OpenAI
from pandasai import Agent, SmartDataframe
import os

import numpy as np


def create_df():
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'))
    # gen a random number
    random_number = np.random.randint(0, 100)
    return str(random_number)


def gen_ai():
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

    agent = Agent(
        df,
        config={
            "llm": llm,
            "max_retries": 5,
            "enforce_privacy": True,
            "enable_charts": False,
            "enable_cache": False,
        },
        memory_size=10,
        description="""
        This is a simple agent that can help you with your data analysis tasks
        for the titanic dataset.
        """,
    )
    return agent


class EchoBot(ActivityHandler):

    async def on_members_added_activity(
        self, members_added: [ChannelAccount], turn_context: TurnContext
    ):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                self.agent = gen_ai()
                await turn_context.send_activity("""Hello, I'm Petey Puffin - I can help you find RNLI facts and figures.
                                                 For now I can only talk to you about the titanic dataset as a test case.
                                                 """)

    async def on_message_activity(self, turn_context: TurnContext):
        # rand_int = create_df()
        response = self.agent.chat(turn_context.activity.text)

        return await turn_context.send_activity(
            # MessageFactory.text(f"Echo: {turn_context.activity.text}_{rand_int}_i just updated main")
            MessageFactory.text(response)
        )
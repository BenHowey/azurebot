# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

from botbuilder.core import ActivityHandler, MessageFactory, TurnContext
from botbuilder.schema import ChannelAccount
import pandas as pd

import numpy as np

def create_df():
    df = pd.DataFrame(np.random.randint(0,100,size=(100, 4)), columns=list('ABCD'))
    # gen a random number
    random_number = np.random.randint(0,100)
    return str(random_number)




class EchoBot(ActivityHandler):
    async def on_members_added_activity(
        self, members_added: [ChannelAccount], turn_context: TurnContext
    ):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity("Hello, I'm Petey Puffin - I can help you find RNLI facts and figures")


    async def on_message_activity(self, turn_context: TurnContext):
        rand_int = create_df()
        return await turn_context.send_activity(
            MessageFactory.text(f"Echo: {turn_context.activity.text}_{rand_int}_i just updated main")
        )
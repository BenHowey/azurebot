# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import datetime
import json
import logging
import os
import urllib
from threading import Timer

from botbuilder.core import (ActivityHandler, ConversationState,
                             StatePropertyAccessor, TurnContext, UserState)
from botbuilder.dialogs import Dialog, DialogSet, DialogTurnStatus
from botbuilder.schema import ChannelAccount
from sqlalchemy import (NVARCHAR, TEXT, Column, DateTime, Integer, String,
                        create_engine)
from sqlalchemy.orm import declarative_base, sessionmaker
from tenacity import retry, stop_after_attempt, wait_fixed

from dialogs.ai_chatgtp import AIBotDialog

logger = logging.getLogger(__name__)

TIMEOUT = 600

Base = declarative_base()


class Message(Base):
    __tablename__ = 'botmessagesdb'
    id = Column(Integer, primary_key=True, autoincrement=True)
    userid = Column(TEXT, nullable=False)
    datetime = Column(DateTime, nullable=False)
    conversation = Column(TEXT, nullable=True)


class DialogHelper:
    @staticmethod
    async def run_dialog(
        dialog: Dialog, turn_context: TurnContext, accessor: StatePropertyAccessor
    ):
        dialog_set = DialogSet(accessor)
        dialog_set.add(dialog)

        dialog_context = await dialog_set.create_context(turn_context)
        results = await dialog_context.continue_dialog()
        if results.status == DialogTurnStatus.Empty:
            await dialog_context.begin_dialog(dialog.id)


class PeteBot(ActivityHandler):
    # def __init__(self, conversation_state: ConversationState, user_state: UserState, dialog: Dialog):
    def __init__(self, conversation_state: ConversationState, user_state: UserState):
        self.conversation_state = conversation_state
        self.user_state = user_state
        # self.dialog = dialog
        self.last_activity_property = self.user_state.create_property("LastActivity")
        self.dialog_state_property = self.conversation_state.create_property("DialogState")
        self.initialize_dialog()
        self.inactivity_timer = None

    def initialize_dialog(self):
        # Reinitialize the dialog instance
        self.dialog = AIBotDialog(self.user_state)  # Replace with your actual dialog class and initialization if needed

    def reset_inactivitytimer(self):
        if self.inactivity_timer:
            self.inactivity_timer.cancel()

        self.inactivity_timer = Timer(TIMEOUT, self.log_prompt)
        self.inactivity_timer.start()

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(5))
    def log_prompt(self):
        print("loggine conversation")
        parmas = urllib.parse.quote_plus(os.getenv("SQLAZURECONNSTR_SQLAZURECONNSTR_"))
        conn_str = 'mssql+pyodbc://?odbc_connect=' + parmas
        engine = create_engine(conn_str, echo=True)
        message = Message(datetime=datetime.datetime.now(), userid=self.user_id, conversation=json.dumps(self.dialog.prompt, indent=4))

        # Create a Session class
        Session = sessionmaker(bind=engine)

        # Create a session instance
        session = Session()
        # Add the new message to the session
        session.add(message)
        session.commit()
        session.close()
        print("Prompt logged")

    async def on_members_added_activity(
        self, members_added: [ChannelAccount], turn_context: TurnContext
    ):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                current_time = datetime.datetime.now(datetime.timezone.utc)

                await turn_context.send_activity("""Hello, I'm Petey Puffin -
                                                 I can help you find RNLI
                                                 facts and figures.
                                                 I am trained on the RNLI
                                                 incident data.
                                                 """)

                self.user_id = turn_context.activity.recipient.id

                await self.last_activity_property.set(turn_context, current_time)
                await self.user_state.save_changes(turn_context)

                # re initialise the dialog
                self.initialize_dialog()

                await DialogHelper.run_dialog(
                    self.dialog,
                    turn_context,
                    self.conversation_state.create_property("DialogState"),
                )

    async def on_turn(self, turn_context: TurnContext):
        await super().on_turn(turn_context)

        # Save any state changes that might have ocurred during the turn.
        await self.conversation_state.save_changes(turn_context)
        await self.user_state.save_changes(turn_context)

    async def on_message_activity(self, turn_context: TurnContext):
        current_time = datetime.datetime.now(datetime.timezone.utc)
        last_activity_time = await self.last_activity_property.get(turn_context, None)
        self.user_id = turn_context.activity.recipient.id
        if last_activity_time:
            self.reset_inactivitytimer()

            elapsed_time = current_time - last_activity_time
            if elapsed_time.total_seconds() > TIMEOUT:  # 600 seconds = 10 minutes
                # Restart the session
                await self.dialog_state_property.delete(turn_context)
                await turn_context.send_activity("Session has been inactive for 10 minutes. Restarting session.")
                await self.on_members_added_activity([turn_context.activity.from_property], turn_context)
                return

        # Update the last activity time
        await self.last_activity_property.set(turn_context, current_time)
        await self.user_state.save_changes(turn_context)

        await DialogHelper.run_dialog(
            self.dialog,
            turn_context,
            self.conversation_state.create_property("DialogState"),
        )

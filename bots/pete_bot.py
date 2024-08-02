# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import datetime
import logging
import os
import shutil

from botbuilder.core import (ActivityHandler, ConversationState,
                             StatePropertyAccessor, TurnContext, UserState)
from botbuilder.dialogs import Dialog, DialogSet, DialogTurnStatus
from botbuilder.schema import ChannelAccount

from dialogs.ai_chatgtp import AIBotDialog

logger = logging.getLogger(__name__)


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

    def initialize_dialog(self):
        # Reinitialize the dialog instance
        self.dialog = AIBotDialog(self.user_state)  # Replace with your actual dialog class and initialization if needed


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

        if last_activity_time:
            elapsed_time = current_time - last_activity_time
            if elapsed_time.total_seconds() > 600:  # 600 seconds = 10 minutes
                # Restart the session
                await self.dialog_state_property.delete(turn_context)
                await turn_context.send_activity("Session has been inactive for 10 minutes. Restarting session.")
                # if the bot is restarted, remove the last database if its a sqlite database
                if self.dialog.data_connector.database_type == 'sqlite':
                    shutil.rmtree(os.path.dirname(self.dialog.data_connector.databse_path), ignore_errors=True)
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

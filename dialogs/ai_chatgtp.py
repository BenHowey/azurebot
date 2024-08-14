# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import datetime
import json
import os
import re
from threading import Timer

import openai
import pandas as pd
# import pandasai.exceptions as pd_exceptions
from botbuilder.core import MessageFactory, StatePropertyAccessor, UserState
from botbuilder.dialogs import (ComponentDialog, DialogTurnResult,
                                WaterfallDialog, WaterfallStepContext)
from botbuilder.dialogs.prompts import PromptOptions, TextPrompt

# from databricks import sql
# from connectors.databricks_connector import DatabricksConnector
from connectors.sqlite_connector import SQLliteConnector

# from . import training_config as tc


class AIBotDialog(ComponentDialog):
    def __init__(self, user_state: UserState):
        super(AIBotDialog, self).__init__(AIBotDialog.__name__)

        self.user_profile_accessor = user_state.create_property("UserProfile")
        self.user_conversations_accessor: StatePropertyAccessor = user_state.create_property("UserConversations")

        # set up the diaglog waterfall flow
        self.add_dialog(
            WaterfallDialog(
                WaterfallDialog.__name__,
                [
                    self.ai_bot_question,
                    self.ai_bot_answer,
                    self.check_answer
                ],
            )
        )
        self.user_state = user_state

        self.add_dialog(TextPrompt(TextPrompt.__name__))

        self.initial_dialog_id = WaterfallDialog.__name__

        # Add the data connector
        self.data_connector = SQLliteConnector()
        # self.data_connector = DatabricksConnector()

        system = f"""
        You are a statistics expert that provides insight to my {self.data_connector.database_type} database table called '{self.data_connector.table_name}'.  Any SQL commands must only reference this table
        and be in the correct syntax for this type of database.  

        The column headings and associated desciptions are as follows:
        {self.data_connector.headers_description_list}.

        If you are asked conversational questions, then you must reply with json in the form {{"conv_resp":<conversational_response>}}.  If 
        you are asked for information or statistics about the data, you must reply with a SQL command in a json format with the form {{"sql_resp":<sql_command>}}.

        If you are unsure about the answer, please let me know.

        Todays date is {datetime.datetime.now().strftime('%Y-%m-%d')}.

        Do not use colons in the <conversational_response>.

        Do not mention the database name in the conversational responses.

        For any comparison queries, always include both specified entities in the filter.
        For example, when comparing lifeboat stations, ensure both stations are included in the SQL query.

        Only use the following data types for SQL commands:
        'STRING', 'INTEGER', 'DOUBLE', 'DATE', 'TIMESTAMP', 'DATETIME'.

        If asked for "incidents involving X", use the AIC column to filter on.

        {self.data_connector.additional_system_info}
        """

        self.system_prompt = {"role": "system", "content": system}

    async def update_user_conversations(self, step_context, user_id, role, content):
        # Retrieve current conversation
        user_conversations = await self.user_conversations_accessor.get(step_context, {})

        # Initialize if necessary
        if user_conversations is None:
            user_conversations = {}
        if user_id not in user_conversations:
            user_conversations[user_id] = []

        # Update the conversation
        user_conversations[user_id].append({
            'role': role,
            'content': content
        })

        # Save the updated conversation back to state
        await self.user_conversations_accessor.set(step_context, user_conversations)
        await self.user_state.save_changes(step_context)

    async def clean_strings(self, input_string, command_type='conv_resp'):
        # remove leading and trailing quotes
        clean_str = input_string.strip().lstrip('"').lstrip("'").rstrip('"').rstrip("'")
        # remove backslashes
        clean_str = clean_str.replace('\\', '')
        # check that the number of quotes is even
        doube_count = clean_str.count('"')
        single_count = clean_str.count("'")
        if doube_count % 2 == 1:
            clean_str = clean_str + '"'
        if single_count % 2 == 1:
            clean_str = clean_str + "'"
        # convert to json string format
        # json_clean_content = json.dumps({command_type: clean_str})
        json_clean_content = json.dumps(clean_str)

        return json_clean_content

    async def clean_sql(self, sql_query):
        # if lifeboat station in the Where make sure it is nested in % marks
        # if 'WHERE' in sql_query and 'Lifeboat_Station' in sql_query:
        #     # make sure there is a % around the value
        #     sql_query = re.sub(r"Lifeboat_Station\s*ILIKE\s*'([^']*)'", r'Lifeboat_Station ILIKE \'%\1%\'', sql_query).replace('%%','%').replace('\\','')
        # replace LIKE with ILIKE
        # if 'LIKE' in sql_query and 'ILIKE' not in sql_query:
        #     sql_query = sql_query.replace('LIKE', 'ILIKE')
        # check if there is an AVG function in the response
        # if 'AVG' in sql_query and not 'TRY_CAST' in sql_query:
        #     # get the term within the AVG command
        #     avg_term = re.search(r'AVG\((.*?)\)', sql_query).group(1)
        #     # replace term with a null if statement
        #     avg_term_formatted = f'TRY_CAST({avg_term} AS DOUBLE)'
        #     # replace the avg_term with the avg_term_formatted
        #     sql_query = sql_query.replace(avg_term, avg_term_formatted)

        return sql_query

    async def nl_to_sql(self, plain_query, user_id, step_context):
        # self.prompt.append({"role": "user", "content": plain_query})

        user_conversations = await self.user_conversations_accessor.get(step_context, {})

        regex_pattern = r'\{"(sql_resp|conv_resp)"\s*:\s*"[^"]*"\}'

        attempt = 0

        while attempt < 3:
            response = openai.ChatCompletion.create(
                # model="gpt-3.5-turbo",
                model="gpt-4o-mini",
                seed=42,
                messages=user_conversations[user_id],
                temperature=0,
                response_format={"type": "json_object"},
                max_tokens=256)
            try:
                # check if the response has the correct format with either a sql or conversational response
                # check if the response matches the regex pattern
                resp_message = response['choices'][0]['message']['content']
                patterncheck = re.search(regex_pattern, resp_message)
                command_type = "sql_resp" if 'SELECT' in resp_message and "sql_resp" in resp_message else "conv_resp"
                if patterncheck is None:
                    if len(resp_message.split(':')) == 1:   # one single string with no colon
                        content = await self.clean_strings(resp_message, command_type)
                        print('Formatting a single string with no colon')
                        content_formatted = f'{{"{command_type}":{content}}}'
                        # if the command type is sql clean it
                        if command_type == 'sql_resp':
                            content_formatted = await self.clean_sql(content_formatted)
                        resp_generated = json.loads(content_formatted)
                        break

                    elif len(resp_message.split(':')) == 2:  # one single string with a colon
                        # check out if the content is non-conformative - we can for the key
                        content = await self.clean_strings(resp_message.split(':')[-1].strip('}'), command_type)
                        print('Formatting a non-conforming string with a colon')
                        content_formatted = f'{{"{command_type}":{content}}}'
                        # if the command type is sql clean it
                        if command_type == 'sql_resp':
                            content_formatted = await self.clean_sql(content_formatted)
                        resp_generated = json.loads(content_formatted)
                        break

                    else:   # if it's more than 2 elements on the split or anything else we repeat
                        try:  # try joining the last elements
                            print('joining the last elements')
                            elements = resp_message.split(':')
                            content = await self.clean_strings(':'.join(elements[1:]).strip('}'), command_type)
                            content_formatted = f'{{"{command_type}":{content}}}'
                            resp_generated = json.loads(content_formatted)
                            break
                        except:
                            print('Cant join the last elsements so going to re-run the command and try again.')

                        print('Formatting a non-conforming string with more than 2 elements.')
                        self_healing_conv = user_conversations[user_id][-1]['content'] + f' Remember to give your anwer in format that conforms with this regex: {regex_pattern}'
                        await self.update_user_conversations(step_context, user_id, 'assistant', str(self_healing_conv))

                        attempt += 1
                        continue

                else:
                    # check that it is not a conv with a nested sql command
                    if len(patterncheck.regs) == 2 and not patterncheck.regs[0][0]==0:  # this is sql nested in a conv
                        print('Nested conforming string')
                        # extract out the SQL command and run and return sql conv
                        response['choices'][0]['message']['content'] = resp_message[patterncheck.regs[0][0]:patterncheck.regs[0][1]]

                    # this is either sql or conv that is formatted correctly
                    # clean the sql query
                    if command_type == 'sql_resp':
                        # pull out the regex match to make sure that the only bit being sent
                        content_formatted = resp_message[patterncheck.regs[0][0]:patterncheck.regs[0][1]]
                        response['choices'][0]['message']['content'] = await self.clean_sql(content_formatted)

                    resp_generated = json.loads(response['choices'][0]['message']['content'])
                    break
            except json.JSONDecodeError:
                attempt += 1
                continue

        return resp_generated

    async def request_data(self, plain_query, user_id, step_context):
        class Response:
            def __init__(response, data, sql):
                response.data = data
                response.explaination = explaination

        resp_generated = await self.nl_to_sql(plain_query, user_id, step_context)
        # check if the response is a conversational response
        if 'sql_resp' in resp_generated.keys():

            try:
                sql_response = self.data_connector.query_source_data(resp_generated['sql_resp'])
                sql_keys = sql_response[0].keys()
                # we need to check that no more than 10 rows are returned otherwise we will be using too many tokens
                if len(sql_response) > 10:
                    pass
                data = sql_response.__str__()

            except Exception as e:
                # if it errors - pass the SQL with the error back and try to fix it!
                print('Self healing!')
                exception_query = f'''The sql run was:{resp_generated['sql_resp']}, the following error was raised:{e}. Correct the SQL command and return in the format {{"sql_resp":corrected_sql_command}}'''
                await self.update_user_conversations(step_context, user_id, 'assistant', str(exception_query))

                resp_generated = await self.nl_to_sql(exception_query, user_id, step_context)
                # try and run again
                sql_response = self.data_connector.query_source_data(resp_generated['sql_resp'])
                data = sql_response.__str__()

            await self.update_user_conversations(step_context, user_id, 'assistant', str(resp_generated))

            # add the convert numbers to sentence conv
            # num2conv = f'The question was:{plain_query}, the answer is:{data}. Give the answer in a sentence with the format {{"conv_resp":answer_as_sentence}}'
            num2conv = f'The question was:{plain_query}.  You have run the command and have the result in a dictionary called data with the keys {sql_keys}. Give the answer in a sentence with the format {{"conv_resp":answer_as_sentence}} and use the data key names as placeholders for the results that can be filled using .format(data)'
            await self.update_user_conversations(step_context, user_id, 'assistant', str(num2conv))

            # get explanation of how you came to that answer
            explaination_query = await self.nl_to_sql(num2conv, user_id, step_context)
            # self.prompt.append({"role": "assistant", "content": str(explaination_query)})
            await self.update_user_conversations(step_context, user_id, 'assistant', str(explaination_query))

            # explaination = explaination_query['conv_resp'].format(sql_response=data)
            explaination = explaination_query['conv_resp'].format(**sql_response[0])
        else:
            explaination = resp_generated['conv_resp']
            data = None

        await self.update_user_conversations(step_context, user_id, 'assistant', str(explaination))

        return Response(data, resp_generated)

    async def ai_bot_question(
        self, step_context: WaterfallStepContext
    ) -> DialogTurnResult:
        # WaterfallStep always finishes with the end of the Waterfall or with another dialog;
        # here it is a Prompt Dialog. Running a prompt here means the next WaterfallStep will
        # be run when the users response is received.
        if step_context.options is None:
            return await step_context.prompt(
                TextPrompt.__name__,
                PromptOptions(
                    prompt=MessageFactory.text("What can I help you with today?"),
                ),
            )
        else:
            user_id = step_context._turn_context.activity.from_property.id

            await self.update_user_conversations(step_context.context, user_id, 'user', step_context.options['question'])
            return await step_context.next(step_context.options['question'])

    async def ai_bot_answer(
        self, step_context: WaterfallStepContext
    ) -> DialogTurnResult:
        self.question = step_context.result
        # WaterfallStep always finishes with the end of the Waterfall or with another dialog;
        # here it is a Prompt Dialog. Running a prompt here means the next WaterfallStep will
        # be run when the users response is received.
        if 'Rich Martin' in self.question:
            return await step_context.prompt(
                TextPrompt.__name__,
                PromptOptions(
                    prompt=MessageFactory.text("Rich is a mapping genius")
                )
            )
        else:
            user_id = step_context._turn_context.activity.from_property.id
            user_conversations = await self.user_conversations_accessor.get(step_context.context, {})
            if user_conversations is None:
                user_conversations = {}

            if user_id not in user_conversations:
                await self.update_user_conversations(step_context.context, user_id, 'system', self.system_prompt['content'])
                await self.update_user_conversations(step_context.context, user_id, 'user', self.question)

            user_conversations = await self.user_conversations_accessor.get(step_context.context, {})

            self.response = await self.request_data(self.question, user_id, step_context._turn_context)

            return await step_context.prompt(
                TextPrompt.__name__,
                PromptOptions(
                    prompt=MessageFactory.text(f"{self.response.explaination}")
                )
            )

    async def check_answer(self, step_context: WaterfallStepContext) -> DialogTurnResult:
        # if thanks in the reply then end the dialog if not restart the dialog
        step_context.values['question'] = step_context.result
        # if 'thank' in step_context.result:
        #     # send a url link for the user to click to lodge a ticket.
        #     await step_context.context.send_activity(
        #         MessageFactory.text("If you need this information validated please log a ticket here:"
        #                             "[Log a ticket](https://live.hornbill.com/rnli/catalog/service/com.hornbill.servicemanager/85/serviceRequest/)"
        #                             )
        #     )
        #     hornbill_req_params={'title': 'AI validation request',
        #                          'extra': json.dumps(self.agent.logs, indent=4),
        #                          'category': 'Data validation',
        #                          'owner': 'Data team',
        #                          'assignTo': 'Data Triage'}

        #     await step_context.context.send_activity(
        #         MessageFactory.text("You're welcome. If you need anything else"
        #                             ", just ask!")
        #     )
        #     return await step_context.end_dialog()
        # else:
        return await step_context.replace_dialog(self.initial_dialog_id,
                                                     options=step_context.values)

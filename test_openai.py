import os
from dotenv import load_dotenv
load_dotenv()

from openai import OpenAI
from openai.resources.beta.assistants.assistants import Assistants
from openai.resources.beta.threads.threads import Threads
from openai.resources.beta.threads.runs.runs import Runs
from openai.resources.beta.threads.messages.messages import Messages

client = OpenAI(api_key=os.environ.get('YOUR_OPENAI_KEY'))



def create_assistant(name:str, instructions:str, tools:str, model:str='gpt-4-1106-preview'):
    """
    Creates an OpenAI assistant using the API.

    Arguments:


    >>> Name: the name of the assistant.
    >>> Instructions: the instructions for the assistant.
    >>> Tools: The tools to give the assistant. e.g. code-interpreter, retrieval
    >>> Model: the model to use. default = gpt-4-1106-preview
    """
    assistant = client.beta.assistants.create(
        name=name,
        instructions=f"{instructions}",
        tools=[{"type": f"{tools}"}],
        model=model
    )
    id = assistant.id
    model = assistant.model
    instructions = assistant.instructions
    return assistant


def create_thread():
    thread = client.beta.threads.create()

    return thread.id


def create_message(thread, role:str, content:str):
    """
    Creates a message within a thread between user / assistant.

    Arguments:

    >>> thread_id: the ID of the thread
    >>> role: whether the message is from the user or the assistant
    >>> content: the content of the interaction
    """

    message = client.beta.threads.messages.create(
        thread_id=thread.id,
        role=role,
        content=content
    )


    return message

def run_thread(thread_id):
    run = client.beta.threads.runs.create(
    thread_id='thread_xogYNUjD5psm38lbxYZxGmeI',
    assistant_id='asst_mzbMbtc8p0jjUomVYPaGARPB',
    instructions="Please address the user as Jane Doe. The user has a premium account."
    )

    return run.id


def check_run(thread_id, run_id):
    run = client.beta.threads.runs.retrieve(
    thread_id=thread_id,
    run_id=run_id
    )

    return run

assistant_id='asst_mzbMbtc8p0jjUomVYPaGARPB'
thread_id= 'thread_xogYNUjD5psm38lbxYZxGmeI'
run_id = run_thread(thread_id)

thread_check = check_run(thread_id, run_id)

print(thread_check)
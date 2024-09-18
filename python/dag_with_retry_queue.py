import logging
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator # type: ignore
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define the custom macro function
def get_user_cd(dag_run):
    """
    Retrieve the configuration values from the DAG run context.

    Args:
        dag_run: The context of the current DAG run, containing configuration.

    Returns:
        dict: A dictionary containing 'id' and 'name' from the configuration.
    """
    id = dag_run.conf.get('id')
    name = dag_run.conf.get('name')
    print({"id": id, "name": name})
    return {"id": id, "name": name}

def check_required_keys(**kwargs):
    """
    Check if required keys are present in the DAG run configuration.

    Args:
        kwargs: The context dictionary, which includes 'dag_run'.

    Returns:
        str: The task ID to execute next, either 'task_1' or 'exit_task'.
    """
    conf = kwargs.get('dag_run').conf
    print(f"conf: {conf}")
    required_keys = ['id', 'name']  # List of required keys

    # Check if all required keys are present in the configuration
    if all(key in conf for key in required_keys):
        return 'task_1'  # Configuration is complete, proceed to task_1
    else:
        return 'exit_task'  # Configuration is missing, go to exit task

def update_task_status(task_id, status):
    """
    Call a public API to update the task status.

    Args:
        task_id (str): The ID of the task to update.
        status (str): The new status of the task.

    Raises:
        requests.exceptions.RequestException: If the API call fails.
    """
    # Example API endpoint for JSONPlaceholder
    api_url = 'https://jsonplaceholder.typicode.com/posts'
    
    # Define the data to send in the POST request
    data = {
        'task_id': task_id,
        'status': status,
    }

    # Make the API call
    try:
        response = requests.post(api_url, json=data)
        response.raise_for_status()  # Raise an error for bad responses
        logging.info(f"Successfully called API for task {task_id}: {response.json()}")
    except requests.exceptions.RequestException as e:
        logging.error(f"API call failed for task {task_id}: {e}")

def execute_with_retry(task_id, **kwargs):
    """
    Execute the task logic with a simulated retry mechanism.

    Args:
        task_id (str): The ID of the task to execute.

    Raises:
        Exception: If task execution fails, triggering the retry mechanism.
    """
    try:
        # Simulate task logic
        if task_id == 'task_2':
            # Simulate failure for task_2
            logging.error("Simulating failure for task_2")
            result = 1 / 0  # Simulating failure
        else:
            result = f"Result from {task_id}"  # Simulating success

        logging.info(f"Executing task {task_id}. Result: {result}")
        return result

    except Exception as e:
        logging.error(f"Task {task_id} failed: {e}")
        # Update status on failure
        raise  # Rethrow the exception to trigger the retry mechanism

def on_success_callback(context):
    """
    Callback function executed on task success.

    Args:
        context (dict): The context dictionary containing task instance information.
    """
    task_instance = context['task_instance']
    ti = context['ti']
    
    """
    try_number is set as 1 even before the task runs for the first time in airflow.
    Once the task starts running, the try_number is immediately incremented to 2.
    
    If the task fails, it will still show try_number=2, indicating the task will retry on the next attempt.
    On a retry, try_number would be 3, meaning it's now making its third overall attempt.
    """
    
    attempt_number = ti.try_number - 1  # Subtract 1 to get actual attempt count

    if attempt_number > 1:
        logging.info(f"Task {task_instance.task_id} succeeded after failure. Calling API. Attempt number: {attempt_number}")
        update_task_status(task_instance.task_id, "running")
    else:
        logging.info(f"Task {task_instance.task_id} succeeded without previous failure. Attempt number: {attempt_number}")

def on_failure_callback(context):
    """
    Callback function executed on task failure.

    Args:
        context (dict): The context dictionary containing task instance information.
    """
    ti = context['ti']
    task_instance = context['task_instance']
    update_task_status(task_instance.task_id, "failed")
    logging.info(f"Task {task_instance.task_id} failed")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,  # Set to 1 to allow for one retry
    'retry_delay': timedelta(minutes=1),  # Reduced delay for quicker testing
}

# Create the DAG
dag = DAG(
    'dag_with_retry_queue',
    default_args=default_args,
    description='A DAG with multiple tasks and API call on task failure and retry logic',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    catchup=False,
)

# Validate task
validate_task = BranchPythonOperator(
    task_id='validate_task',
    python_callable=check_required_keys,
    provide_context=True,
    dag=dag
)

# Define the tasks
task_1 = PythonOperator(
    task_id='task_1',
    python_callable=execute_with_retry,
    op_kwargs={'task_id': 'task_1'},
    dag=dag,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=execute_with_retry,
    op_kwargs={'task_id': 'task_2'},
    dag=dag,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
)

task_3 = PythonOperator(
    task_id='task_3',
    python_callable=execute_with_retry,
    op_kwargs={'task_id': 'task_3'},
    dag=dag,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
)

exit_task = DummyOperator(task_id='exit_task', dag=dag)

# Set up task dependencies
validate_task >> task_1
validate_task >> exit_task
task_1 >> task_2 >> task_3  # task_1 must succeed for task_2 to run, and task_2 must succeed for task_3 to run

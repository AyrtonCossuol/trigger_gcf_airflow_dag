from typing import Any

import composer2_airflow_rest_api

def trigger_dag_gcf(data, context=None):
  """
  Trigger a DAG and pass event data.

  Args:
    data: A dictionary containing the data for the event. Its format depends
    on the event.
    context: The context object for the event.

  For more information about the arguments, see:
  https://cloud.google.com/functions/docs/writing/background#function_parameters
  """

  # TODO(developer): replace with your values
  # Replace web_server_url with the Airflow web server address. To obtain this
  # URL, run the following command for your environment:
  # gcloud composer environments describe example-environment \
  #  --location=your-composer-region \
  #  --format="value(config.airflowUri)"
  web_server_url = (
      "https://EXAMPLE..........composer.googleusercontent.com"
  )
  # Replace with the ID of the DAG that you want to run.
  dag_id = 'move_file_buckets'

  composer2_airflow_rest_api.trigger_dag(web_server_url, dag_id, data)
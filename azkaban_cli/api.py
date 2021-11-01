# -*- coding: utf-8 -*-

"""
azkaban_cli.api

This module provides a set of requests for the Azkaban API
"""

import logging
import os

def upload_request(session: requests.Session, host: str, session_id: str, project: str, zip_path: str) -> requests.Response:
    """
    Upload request for the Azkaban API
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        project: Project name on Azkaban
        zip_path: Local path from zip that will be uploaded
    Raise:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns: 
        The response from the request made
    """

    zip_file = open(zip_path, 'rb')
    zip_name = os.path.basename(zip_path)

    response = session.post(
        host + '/manager',
        data={
            u'session.id': session_id,
            u'ajax': u'upload',
            u'project': project
        },
        files={
            u'file': (zip_name, zip_file, 'application/zip'),
        }
    )

    logging.debug("Response: \n%s", response.text)

    return response

def login_request(session: requests.Session, host: str, user: str, password: str) -> requests.Response:
    """
    Login request for the Azkaban API
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        user: The user name
        password: The user password
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Retunrs:
        The response from the request made
    """

    response = session.post(
        host,
        data={
            u'action': u'login',
            u'username': user,
            u'password': password
        }
    )

    logging.debug("Response: \n%s", response.text)

    return response

def schedule_request(
    session: requests.Session, host: str, session_id: str, project: str, flow: str, cron: str, **execution_options) -> requests.Response:
    """
    Schedule request for the Azkaban API
    Args:
        session: A session for creating the request
        session: requests.Session
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        project: Project name that contains the flow that will be scheduled on Azkaban
        flow: Flow name to be scheduled on Azkaban
        cron: Cron expression in quartz format used to schedule
        \*\*execution_options: Optional parameters to execution
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    data = {
        u'session.id': session_id,
        u'ajax': u'scheduleCronFlow',
        u'projectName': project,
        u'flow': flow,
        u'cronExpression': cron
    }
    data.update(execution_options)

    logging.debug("Request data: \n%s", data)

    response = session.post(
        host + '/schedule',
        data=data
    )

    logging.debug("Response: \n%s", response.text)

    return response

def fetch_flows_request(session: requests.Session, host: str, session_id: str, project: str) -> requests.Response:
    """
    Fetch flows of a project request for the Azkaban API
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        project: Project name whose flows will be fetched on Azkaban
    Raises:
        raises requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    response = session.get(
        host + '/manager',
        params={
            u'session.id': session_id,
            u'ajax': 'fetchprojectflows',
            u'project': project
        }
    )

    logging.debug("Response: \n%s", response.text)

    return response

def fetch_executions_of_a_flow_request(session: requests.Session, session_id: str, project: str, flow: str, start: int, length:int) -> requests.Response:
    """
    fetch executions of a flow on a given project
    Args:
        session: A session for creating the request
        session: requests.Session
        session_id: An id that the user should have when is logged in
        project: Project name whose flows will be fetched on Azkaban
        flow: Flow name whose schedule will be fetched on Azkaban
        start: The start index of the returned list (inclusive)
        length: The length of the returned list
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    response = session.get(
        host + '/manager',
        params={
            u'session.id': session_id,
            u'ajax':'fetchFlowExecutions',
            u'project': project,
            u'flow': flow,
            u'start': start,
            u'length': length,
        }
    )

    logging.debug("Response: \n%s", response.text)

    return response

def fetch_jobs_from_flow_request(session: requests.Session, host: str, session_id: str, project: str, flow: str) -> requests.Response:
    """
    Fetch jobs of a flow of a project request for the Azkaban API
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        project: Project name whose flow's jobs will be fetched on Azkaban
        flow: Flow id whose jobs will be fetched on Azkaban
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    response = session.get(
        host + '/manager',
        params={
            u'session.id': session_id,
            u'ajax': 'fetchflowgraph',
            u'project': project,
            u'flow': flow
        }
    )

    logging.debug("Response: \n%s", response.text)

    return response

def fetch_schedule_request(session: requests.Session, host: str, session_id: str, project_id: str, flow: str) -> requests.Response:
    """Fetch flow of a project request for the Azkaban API
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        project: Project name whose flow's jobs will be fetched on Azkaban
        project: Flow id whose jobs will be fetched on Azkaban
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    response = session.get(
        host + '/schedule',
        params={
            u'session.id': session_id,
            u'ajax': 'fetchSchedule',
            u'projectId': project_id,
            u'flowId': flow
        }
    )

    logging.debug("Response: \n%s", response.text)

    return response

def unschedule_request(session: requests.Session, host: str, session_id: str, schedule_id: str) -> requests.Response:
    """
    Unschedule request for the Azkaban API
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        schedule_id: Schedule id of the flow that will be unscheduled on Azkaban
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    data = {
        u'session.id': session_id,
        u'action': u'removeSched',
        u'scheduleId': schedule_id
    }

    logging.debug("Request data: \n%s", data)

    response = session.post(
        host + '/schedule',
        data=data
    )

    logging.debug("Response: \n%s", response.text)

    return response

def execute_request(session: requests.Session, host: str, session_id: str, project: str, flow: str, **execution_options) -> requests.Response:
    """
    Execute request for the Azkaban API
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        project: Project name that contains the flow that will be executed on Azkaban
        flow: Flow name to be executed on Azkaban
        \*\*execution_options: Optional parameters to execution
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    params = {
        u'session.id': session_id,
        u'ajax': 'executeFlow',
        u'project': project,
        u'flow': flow
    }

    params.update(execution_options)

    response = session.get(
        host + '/executor',
        params=params
    )

    logging.debug("Response: \n%s", response.text)

    return response

def cancel_request(session: requests.Session, host: str, session_id: str, exec_id: str) -> requests.Response:
    """
    Cancel an running flow for the Azkaban API
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        exec_id: Execution id to be canceled
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    response = session.get(
        host + '/executor',
        params={
            u'session.id': session_id,
            u'ajax': 'cancelFlow',
            u'execid': exec_id
        }
    )

    logging.debug("Response: \n%s", response.text)

    return response


def create_request(session: requests.Session, host: str, session_id: str, project: str, description: str) -> requests.Response:
    """
    Create a Project request for the Azkaban API
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        project: Project name to be created on Azkaban
        description: The description for the project
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    response = session.post(
        host + '/manager',
        data={
            u'session.id': session_id,
            u'action': u'create',
            u'name': project,
            u'description': description
        }
    )

    logging.debug("Response: \n%s", response.text)

    return response

def delete_request(session: requests.Session, host: str, session_id: str, project: str) -> requests.Response:
    """
    Delete a Project request for the Azkaban API
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        project: Project name to be deleted on Azkaban
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    response = session.get(
        host + '/manager',
        params={
            u'session.id': session_id,
            u'delete': 'true',
            u'project': project
        }
    )

    logging.debug("Response: \n%s", response.text)

    return response

def fetch_projects_request(session: requests.Session, host: str, session_id: str) -> requests.Response:
    """
    Fetch all projects request for the Azkaban API
     Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    response = session.get(
        host + '/index?all',
        params={
            u'session.id': session_id
        }
    )

    logging.debug("Response: \n%s", response.text)

    return response

def add_permission_request(session: requests.Session, host: str, session_id: str, project: str, group: str, permission_options: dict) -> requests.Response:
    """
    Add permission request for the Azkaban API
    Fetch all projects request for the Azkaban API
     Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        project: Project name that will receive group permissions on Azkaban
        group: Group name on Azkaban
        permission_options: The permissions options added to group in the project on Azkaban
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    response = __call_permission_api(session, host, session_id, 'addPermission', project, group, permission_options)

    logging.debug("Response: \n%s", response.text)

    return response

def remove_permission_request(session: requests.Session, host: str, session_id: str, project: str, group: str) -> requests.Response:
    """
    Remove permission request for the Azkaban API
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        project: Project name that will lose group permissions on Azkaban
        group: Group name on Azkaban
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    #to remove a group permission, we have to pass all permissions as False
    permission_options = {'admin': False, 'read': False, 'write': False, 'execute': False, 'schedule': False}

    response = __call_permission_api(session, host, session_id, 'changePermission', project, group, permission_options)

    logging.debug("Response: \n%s", response.text)

    return response

def change_permission_request(session: requests.Session, host: str, session_id: str, project: str, group: str, permission_options: dict) -> requests.Response:
    """
    Change permission request for the Azkaban API
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        project: Project name that will receive the newly updated group permissions on Azkaban
        group: Group name on Azkaban
        permission_options: The permissions options to replace old permission for the group in the project on Azkaban
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    response = __call_permission_api(session, host, session_id, 'changePermission', project, group, permission_options)

    logging.debug("Response: \n%s", response.text)

    return response


def fetch_sla_request(session: requests.Session, host: str, session_id: str, schedule_id: str) -> requests.Response:
    """
    Fetch flow of a SLA request for the Azkaban API
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        schedule_id: The id of the shchedule
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    response = session.get(
        host + '/schedule',
        params={
            u'session.id': session_id,
            u'ajax': 'slaInfo',
            u'scheduleId': schedule_id,
        }
    )

    logging.debug("Response: \n%s", response.text)

    return response

def __call_permission_api(
    session: requests.Response, host: str, session_id: str, operation: str, project: str, group: str, permission_options: str
    ) -> requests.Response:
    """
    This function is a utility to call permission API in Azkaban.
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        operation: The action to be executed in Azkaban, can be with values [addPermission OU changePermission]
        project: Project name that will receive the newly updated group permissions on Azkaban
        group: Group name on Azkaban
        permission_options: The permissions options to replace old permission for the group in the project on Azkaban
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made

    Sample request to Azkaban
    #https://azkaban.qa.globoi.com/manager?project=teste-permission-api-20190806&name=time-dmp&ajax=addPermission&permissions%5Badmin%5D=false&permissions%5Bread%5D=true&permissions%5Bwrite%5D=false&permissions%5Bexecute%5D=true&permissions%5Bschedule%5D=false&group=true
    """

    return session.get(
        host + '/manager',
        params = {
            u'session.id': session_id,
            u'ajax': operation,
            u'project': project,
            u'name': group,
            u'permissions[admin]': permission_options['admin'],
            u'permissions[write]': permission_options['write'],
            u'permissions[read]': permission_options['read'],
            u'permissions[execute]': permission_options['execute'],
            u'permissions[schedule]': permission_options['schedule'],
            u'group': True
        }
    )

def fetch_flow_execution_request(session: requests.Session, host: str, session_id: str, exec_id: str) -> requests.Response:
    """
    Fetch a flow execution request for the Azkaban API
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        exec_id: Execution id to be fetched
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    response = session.get(
        host + '/executor',
        params={
            u'session.id': session_id,
            u'ajax': 'fetchexecflow',
            u'execid': exec_id
        }
    )

    logging.debug("Response: \n%s", response.text)

    return response

def fetch_flow_execution_updates_request(
    session: requests.Session, host: str, session_id: str, exec_id: str, last_update_time: str = None
    ) -> requests.Response:
    """
    Fetch a flow execution updates request for the Azkaban API
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        exec_id: Execution id to be fetched
        last_update_time: The criteria to filter by last update time. Set the value to be -1 if all 
            job information are needed. Use -lt="value" to subscribe the default value, defaults to -1
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
        The response from the request made
    """

    response = session.get(
        host + '/executor',
        params={
            u'session.id': session_id,
            u'ajax': 'fetchexecflowupdate',
            u'execid': exec_id,
            u'lastUpdateTime': last_update_time
        }
    )

    logging.debug("Response: \n%s", response.text)

    return response

def fetch_execution_job_log_request(
    session: requests.Response, host: str, session_id: str, exec_id: str, jobid: str, offset: str, length: str
    ) -> requests.Response:
    """
    Fetches the correponding job logs.
    This method receives the execution id, jobid, offset and lenght, makes a fetch
    request to get the correponding job logs and evaluates the response.
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        exec_id: Execution id to be fetched
        jobid: The unique id for the job to be fetched
        offset: The offset for the log data
        length: The length of the log data. For example, if the offset set is 10 and the length is 1000,
            the returned log will starts from the 10th character and has a length of 1000 
            (less if the remaining log is less than 1000 long)
        
    Raises:
        FetchExecutionJobsLogError: when Azkaban api returns error in response
    Returns:
         The json response from the request
    """

    response = session.get(
        host + '/executor',
        params={
            u'session.id': session_id,
            u'ajax': 'fetchExecJobLogs',
            u'execid': exec_id,
            u'jobId': jobid,
            u'offset': offset,
            u'length': length
        }
    )

    logging.debug("Response: \n%s", response.text)

    return response

def resume_flow_execution(session: requests.Session, host: str, session_id: str , exec_id: str) -> requests.Response:
    """
    Resume a flow execution request for the Azkaban API
     Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        exec_id: Execution id to be fetched
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
         The response from the request made
    """
    response = session.get(
        host + '/executor',
        params={
            u'session.id': session_id,
            u'ajax': 'resumeFlow',
            u'execid': exec_id
        }
    )

    logging.debug("Response: \n%s", response.text)

    return response

def fetch_running_executions_of_a_flow_request(session: requests.Session, host: str, session_id: str, project: str, flow: str) -> requests.Response:
    """
    Fetch running executions of a flow
    Args:
        session: A session for creating the request
        host: Hostname where the request should go
        session_id: An id that the user should have when is logged in
        project: Project name that will receive the newly updated group permissions on Azkaban
        Flow: Flow id whose executions will be fetched on Azkaban
    Raises:
        requests.exceptions.ConnectionError: if cannot connect to host
    Returns:
         The response from the request made
    """

    response = session.get(
        host + '/executor',
        params={
            u'session.id': session_id,
            u'ajax': 'getRunning',
            u'project': project,
            u'flow': flow
        }
    )

    logging.debug("Response: \n%s", response.text)

    return response

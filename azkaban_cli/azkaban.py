# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
import os
from shutil import make_archive

import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning

import azkaban_cli.api as api
from azkaban_cli.exceptions import (
    NotLoggedOnError,
    SessionError,
    LoginError,
    UploadError,
    ScheduleError,
    FetchFlowsError,
    FetchJobsFromFlowError,
    FetchScheduleError,
    FetchSLAError,
    UnscheduleError,
    ExecuteError,
    CancelError,
    CreateError,
    AddPermissionError,
    RemovePermissionError,
    ChangePermissionError,
    FetchFlowExecutionError,
    FetchFlowExecutionUpdatesError,
    FetchExecutionsOfAFlowError,
    FetchExecutionJobsLogError,
    ResumeFlowExecutionError,
    FetchRunningExecutionsOfAFlowError
)


class Azkaban(object):
    def __init__(self):
        # Session ignoring SSL verify requests
        session = requests.Session()
        session.verify = False
        urllib3.disable_warnings(InsecureRequestWarning)

        self.__session = session
        self.__host = None
        self.__user = None
        self.__session_id = None

    def __validate_host(self, host: str) -> str:
        """ PRIVATE
        Receives a host and when the host ends with '/', will we return a host without the '/'.
        Args:
            host: azkaban host
        Returns:
            host without the '/'
        """
        valid_host = host

        while valid_host.endswith(u'/'):
            valid_host = valid_host[:-1]

        return valid_host

    def __check_if_logged(self):
        """ PRIVATE
        Checks if the instance created has a valid session.
        :raise: NotLoggedOnError when __session_id not exists.
        """
        if not self.__session_id:
            raise NotLoggedOnError()

    def __catch_login_html(self, response):
        """ PRIVATE
        Checks the content in the verification is in at least one line of the response.
        :raise: SessionError when content not in response lines.
        """
        if "  <script type=\"text/javascript\" src=\"/js/azkaban/view/login.js\"></script>" in \
                response.text.splitlines():
            raise SessionError(response.text)

    def __catch_response_status_error(self, exception, response_json):
        """ PRIVATE
        Verify error in response, catch response status.
        :raise: exception(error_msg), when error exists and status equals a error, with the 'error_msg'.
        """
        response_status = response_json.get('status')
        if response_status == u'error':
            error_msg = response_json[u'message']
            raise exception(error_msg)

    def __catch_response_error_msg(self, exception, response_json):
        """ PRIVATE
        Catches the error message when 'error' exists in the response keys.
        :raise: SessionError, when error_msg equals a 'sessions', or exception(error_msg).
        """
        if u'error' in response_json.keys():
            error_msg = response_json[u'error']
            if error_msg == "session":
                raise SessionError(error_msg)
            raise exception(error_msg)

    def __catch_empty_response(self, exception, response_json):
        """ PRIVATE
        Does not allow an empty response.
        :raise: exception
        """
        if response_json == {}:
            raise exception('Empty response')

    def __catch_login_text(self, response):
        """ PRIVATE
        Do not allow an empty login attempt.
        :raise: SessionError("Login error. Need username and password")
        """
        if response.text == "Login error. Need username and password":
            raise SessionError(response.text)

    def __catch_login(self, response):
        """ PRIVATE
        Private method to call login_text and login_html from response.
        """
        self.__catch_login_text(response)
        self.__catch_login_html(response)

    def __catch_response_error(self, response, exception, ignore_empty_responses=False):
        """ PRIVATE
        Try to get the answer json. If an error occurs, define response_json as an empty json, send it
        together with the input to the error functions.
        """
        self.__catch_login(response)

        # Some ajax api operations don`t have return body making response.json() raise a ValueError exception
        # The try block enable the __catch_empty_response raise the correct exception
        try:
            response_json = response.json()
        except Exception:
            response_json = {}

        self.__catch_response_error_msg(exception, response_json)
        self.__catch_response_status_error(exception, response_json)

        # Don't raise a exception if we know the request has a empty body
        if not ignore_empty_responses:
            self.__catch_empty_response(exception, response_json)

    def get_logged_session(self) -> dict:
        """
        Method for return the host and session id of the logged session saved on the class
        Returns:
            A dictionary containing host, user and session_id as keys
        """

        logged_session = {
            u'host': self.__host,
            u'user': self.__user,
            u'session_id': self.__session_id
        }

        return logged_session

    def set_logged_session(self, host: str, user: str, session_id: str):
        """
        Method for set host, user and session_id, attributes of the class
        Args:
            host: Azkaban hostname
            user: Azkaban username
            session_id: session.id received from a login request
        """

        self.__host = host
        self.__user = user
        self.__session_id = session_id

    def logout(self):
        """Logout command, intended to clear the host, user and session_id attributes from the instance"""

        self.set_logged_session(None, None, None)

    def login(self, host: str, user: str, password: str):
        """
        Login command, intended to make the request to Azkaban and treat the response properly

        This method validate the host, make the request to Azkaban, and evaluate the response. If host, user or
        password is wrong or could not connect to host, it returns false and do not change the host and session_id
        attribute from the class. If everything is fine, saves the new session_id and corresponding host as attributes
        in the class and returns True
        Args:
            host: Azkaban hostname
            user: Username to login
            password: Password from user
        Raises:
            LoginError: when Azkaban api returns error in response
        """

        valid_host = self.__validate_host(host)

        response = api.login_request(self.__session, valid_host, user, password)

        self.__catch_response_error(response, LoginError)

        response_json = response.json()
        self.set_logged_session(valid_host, user, response_json['session.id'])

        logging.info('Logged as %s' % (user))

    def upload(self, path: str, project: str=None, zip_name: str=None):
        """
        Upload command, intended to make the request to Azkaban and treat the response properly

        This method receives a path to a directory that contains all the files that should be in the Azkaban project,
        zip this path (as Azkaban expects it zipped), make the upload request to Azkaban, deletes the zip that was
        created and evaluate the response.

        If project name is not passed as argument, it will be assumed that the project name is the basename of the path
        passed. If zip name is not passed as argument, the project name will be used for the zip.

        If project or path is wrong or if there is no session_id, it returns false. If everything is fine, returns True.
        Args:
            path: path to be zipped and uploaded
            project: Project name on Azkaban, optional
            zip_name: Zip name that will be created and uploaded, optional
        Raises:
            UploadError: when Azkaban api returns error in response
        """

        self.__check_if_logged()

        if not project:
            # define project name as basename
            project = os.path.basename(os.path.abspath(path))

        if not zip_name:
            # define zip name as project name
            zip_name = project

        try:
            zip_path = make_archive(zip_name, 'zip', path)
        except FileNotFoundError as e:
            raise UploadError(str(e))

        try:
            response = api.upload_request(self.__session, self.__host, self.__session_id, project, zip_path)
        finally:
            os.remove(zip_path)

        self.__catch_response_error(response, UploadError)

        response_json = response.json()
        logging.info('Project %s updated to version %s' % (project, response_json[u'version']))

    def schedule(self, project: str, flow: str, cron: str, **execution_options: dict):
        """
        Schedule command, intended to make the request to Azkaban and treat the response properly.

        This method receives the project, the flow, the cron expression in quartz format and optional execution options,
        make the schedule request to schedule the flow with the cron specified and evaluate the response.

        If project, flow or cron is wrong or if there is no session_id, it returns false. If everything is fine, returns
        True.
        Args:
            project: Project name on Azkaban
            flow: flow name on Azkaban
            cron: Cron expression, in quartz format [Eg.: '0*/10*?**' -> Every 10 minutes]
        Raises:
            ScheduleError: when Azkaban api returns error in response
        """

        self.__check_if_logged()

        execution_options = {k: v for (k, v) in execution_options.items() if v}

        response = api.schedule_request(
            self.__session,
            self.__host,
            self.__session_id,
            project,
            flow,
            cron,
            **execution_options
        )

        self.__catch_response_error(response, ScheduleError)

        response_json = response.json()
        logging.info(response_json[u'message'])
        logging.info('scheduleId: %s' % (response_json[u'scheduleId']))

    def fetch_flows(self, project: str) -> json:
        """
        Fetch flows command, intended to make the request to Azkaban and treat the response properly.

        This method receives the project name, makes the fetch flows request to fetch the flows
        and evaluates the response.

        If project is wrong or there is no session_id, it returns false. If everything is fine, returns
        True.
        Args:
            project_: project name on Azkaban
        Raises:
            FetchJobsFromFlowError: when Azkaban api returns error in response
        Returns:
            the json response from the request
        """

        self.__check_if_logged()

        response = api.fetch_flows_request(
            self.__session,
            self.__host,
            self.__session_id,
            project
        )

        self.__catch_response_error(response, FetchFlowsError)

        response_json = response.json()
        logging.info('Project ID: %s' % (response_json[u'projectId']))
        return response_json

    def fetch_jobs_from_flow(self, project: str, flow: str):
        """
        Fetch jobs of a flow command, intended to make the request to Azkaban and return
        the response.

        This method receives the project name and flow id, makes the fetch jobs of a flow request
        to fetch the jobs of a flow and evaluates the response.
        Args:
            project_id: project id on Azkaban
            flow: flow name on Azkaban
        Raises:
            FetchJobsFromFlowError: when Azkaban api returns error in response
        Returns:
            the json response from the request
        """

        self.__check_if_logged()

        response = api.fetch_jobs_from_flow_request(
            self.__session,
            self.__host,
            self.__session_id,
            project,
            flow
        )

        self.__catch_response_error(response, FetchJobsFromFlowError)

        return response.json()

    def fetch_schedule(self, project_id: str, flow: str) -> json:
        """
        Fetch schedule command, intended to make the request to Azkaban and treat the response properly.

        This method receives the project id, flow name and optional execution options, makes the
        fetch schedule request to fetch the schedule of the flow and evaluates the response.

        If project_id or flow is wrong or there is no session_id, it returns false. If everything is fine, returns
        True.
        Args:
            project_id: project id on Azkaban
            flow: flow name on Azkaban
        Raises:
            FetchScheduleError: when Azkaban api returns error in response
        Returns:
            the json response from the request
        """

        self.__check_if_logged()

        response = api.fetch_schedule_request(
            self.__session,
            self.__host,
            self.__session_id,
            project_id,
            flow
        )

        self.__catch_response_error(response, FetchScheduleError)

        response_json = response.json()
        logging.info('Schedule ID: %s' % (response_json[u'schedule'][u'scheduleId']))
        return response_json

    def unschedule(self, schedule_id: str):
        """
        Unschedule command, intended to make the request to Azkaban and treat the response properly.

        This method receives the schedule id and optional execution options, makes the unschedule
        request to unschedule the flow and evaluates the response.

        If schedule_id is wrong or there is no session_id, it returns false. If everything is fine, returns
        True.
        Args:
            schedule_id: Schedule id on Azkaban
        Raises:
            UnscheduleError: when Azkaban api returns error in response
        """

        self.__check_if_logged()

        response = api.unschedule_request(
            self.__session,
            self.__host,
            self.__session_id,
            schedule_id
        )

        self.__catch_response_error(response, UnscheduleError)

        response_json = response.json()
        logging.info(response_json[u'message'])

    def execute(self, project: str, flow: str, **execution_options: dict):
        """
        Execute command, intended to make the request to Azkaban and treat the response properly.

        This method receives the project and the flow, make the execute request to execute the flow and evaluate the
        response.

        If project or flow is wrong or if there is no session_id, it returns false. If everything is fine, returns True.
        Args:
            project: Project name on Azkaban
            flow: Flow name on Azkaban
        Raises:
            ExecuteError: when Azkaban api returns error in response
        """

        self.__check_if_logged()

        execution_options = {k: v for (k, v) in execution_options.items() if v}

        response = api.execute_request(
            self.__session,
            self.__host,
            self.__session_id,
            project,
            flow,
            **execution_options
        )

        self.__catch_response_error(response, ExecuteError)

        response_json = response.json()
        logging.info('%s' % (response_json[u'message']))

    def cancel(self, execution_id: str):
        """
        Execute command, intended to make the request to Azkaban and treat the response properly.

        This method receives the flow execution id, make the cancel request to cancel the flow execution and
        evaluate the response.

        If the flow is not running, it will return an error message.
        Args:
            execution_id: Execution id on Azkaban
        Raises:
            CancelError: when Azkaban api returns error in response
        """

        self.__check_if_logged()

        response = api.cancel_request(
            self.__session,
            self.__host,
            self.__session_id,
            execution_id,
        )

        self.__catch_response_error(response, CancelError)

    def create(self, project: str, description: str):
        """
        Create command, intended to make the request to Azkaban and treat the response properly.

        This method receives the project name and the description, make the execute request to create the project and
        evaluate the response.
        Args:
            project: Project name on Azkaban
            description: Description for the project
        """

        self.__check_if_logged()

        response = api.create_request(
            self.__session,
            self.__host,
            self.__session_id,
            project,
            description
        )

        self.__catch_response_error(response, CreateError)

        logging.info('Project %s created successfully' % (project))

    def delete(self, project: str):
        """
        Delete command, intended to make the request to Azkaban and treat the response properly.

        This method receives the project name, make the execute request to delete the project and
        evaluate the response.
        Args:
            project: Project name on Azkaban
        """

        self.__check_if_logged()

        api.delete_request(
            self.__session,
            self.__host,
            self.__session_id,
            project
        )

        # The delete request does not return any message

    def fetch_projects(self) -> str:
        """
        Fetch all projects command, intended to make the request to Azkaban and treat the response properly.
        This method makes the fetch projects request to fetch all the projects and evaluates the response.
        Returns:
            Request response content in text
        """

        self.__check_if_logged()

        response = api.fetch_projects_request(
            self.__session,
            self.__host,
            self.__session_id
        )

        # The fetch projects request returns an html content, so we only catch login errors
        self.__catch_login(response)

        return response.text

    def add_permission(self, project: str, group: str, permission_options: dict):
        """
        Add permission command, intended to make the request to Azkaban and treat the response properly.

        This method receives the project name, the group name, and the permission options and execute
        request to add a group permission to the project and evaluate the response.
        Args:
            project: Project name on Azkaban
            group: Group name on Azkaban
            permission_options: The group permissions in the project on Azkaban
        """

        self.__check_if_logged()

        permission_options = self.__check_group_permissions(permission_options)

        response = api.add_permission_request(
            self.__session,
            self.__host,
            self.__session_id,
            project,
            group,
            permission_options
        )

        self.__catch_response_error(response, AddPermissionError, True)

        logging.info('Group [%s] add with permission [%s] in the Project [%s] successfully' % (group,  permission_options, project))

    def remove_permission(self, project: str, group: str):
        """
        Remove permission command, intended to make the request to Azkaban and treat the response properly.

        This method receives the project name and the group name and execute
        request to remove a group permission from the project and evaluate the response.
        Args:
            project: Project name on Azkaban
            group: Group name on Azkaban
        """

        self.__check_if_logged()

        response = api.remove_permission_request(
            self.__session,
            self.__host,
            self.__session_id,
            project,
            group
        )

        self.__catch_response_error(response, RemovePermissionError, True)

        logging.info('Group [%s] permission removed from the Project [%s] successfully' % (group, project))

    def change_permission(self, project: str, group: str, permission_options: dict):
        """
        Change permission command, intended to make the request to Azkaban and treat the response properly.

        This method receives the project name, the group name, and the permission options and execute
        request to change a existing group permission in a project and evaluate the response.
        Args:
            project: Project name on Azkaban
            group: Group name on Azkaban
            permission_options: The group permissions in the project on Azkaban
        """

        self.__check_if_logged()

        permission_options = self.__check_group_permissions(permission_options)

        response = api.change_permission_request(
            self.__session,
            self.__host,
            self.__session_id,
            project,
            group,
            permission_options
        )

        self.__catch_response_error(response, ChangePermissionError, True)

        logging.info('Group [%s] AAA received new permissions [%s] in the Project [%s] successfully' % (group, permission_options, project))

    def fetch_sla(self, schedule_id: str) -> json:
        """
        Fetch SLA command, intended to make the request to Azkaban and treat the response properly.
        Given a schedule id, this API call fetches the SLA.
        Args:
            schedule_id: Schedule ID on Azkaban (Find on fetch_schedule)
        Raises:
            FetchSLAError: when Azkaban api returns error in response
        Returns:
            Returns the json response from the request
        """

        self.__check_if_logged()

        response = api.fetch_sla_request(
            self.__session,
            self.__host,
            self.__session_id,
            schedule_id
        )

        self.__catch_response_error(response, FetchSLAError)

        return response.json()

    def __check_group_permissions(self, permission_options: dict) -> dict:
        """
        Check the group permissions for the project. Catch all permission from the dict and set as True, if the option
        dont exists in this dict, set False. If no permissions are found, just set the Read default to true, as in the
        web version of Azkaban.

        ADMIN.....: Allows the user to do anything with this project, as well as add permissions and delete the project.
        READ......: The user can view the job, the flows, the execution logs.
        WRITE.....: Project files can be uploaded, and the job files can be modified.
        EXECUTE...: The user is allowed to execute, pause, cancel jobs.
        SCHEDULE..: The user is allowed to add, modify and remove a flow from the schedule.

        Args:
           permission_options: The group permissions in the project on Azkaban
        Returns:
            Dictionary containing filled permissions
        """
        __options = ["admin", "write", "read", "execute", "schedule"]
        filled_permission_options = {
            option: permission_options[option] if option in permission_options else False for option in __options
        }

        have_declared_options = \
            filled_permission_options['admin'] and \
            filled_permission_options['read'] and \
            filled_permission_options['write'] and \
            filled_permission_options['execute'] and \
            filled_permission_options['schedule']

        if filled_permission_options['admin']:
            filled_permission_options = {option: True for option in filled_permission_options}

        elif not have_declared_options:
            filled_permission_options['read'] = True

        return filled_permission_options

    def fetch_flow_execution(self, execution_id: str) -> json:
        """
        Fetch a flow execution command, intended to make the request to Azkaban
        and treat the response properly.

        This method receives the execution id, makes the fetch a flow execution request
        to fetch the flow execution details and evaluates the response.
        Args:
            execution_id: Execution id on Azkaban
        Raises:
            FetchExecutionJobsLogError: when Azkaban api returns error in response
        Returns:
            Returns the json response from the request
        """

        self.__check_if_logged()

        response = api.fetch_flow_execution_request(
            self.__session,
            self.__host,
            self.__session_id,
            execution_id
        )

        self.__catch_response_error(response, FetchFlowExecutionError)

        return response.json()

    def fetch_flow_execution_updates(self, execution_id: str, last_update_time: str) -> json:
        """
        Fetch a flow execution updates command, intended to make the request to Azkaban
        and treat the response properly.

        This method receives the execution id and the last_update_time , makes the fetch a
        flow execution request to fetch the flow execution update details and evaluates the response.
        Args:
            execution_id: Execution id on Azkaban
            last_update_time: (optional) The criteria to filter by last update time.
                Set the value to be -1 if all job information are needed. Use -lt="value"
                to subscribe the default value, defaults to -1
        Raises:
            FetchExecutionJobsLogError: when Azkaban api returns error in response
        Returns:
            Returns `Response` object as json friendly
        """

        self.__check_if_logged()

        response = api.fetch_flow_execution_updates_request(
            self.__session,
            self.__host,
            self.__session_id,
            execution_id,
            last_update_time
        )

        self.__catch_response_error(response, FetchFlowExecutionUpdatesError)

        return response.json()

    def fetch_executions_of_a_flow(self, project: str, flow: str, start: int, length: int) -> json:
        """
        Fetch executions of a flow command, intended to make the request to Azkaban
        and treat the response properly.
        This method receives the project name, the flow id, the start index of the returned list and the length of the
        returned list, it makes the fetch and evaluates the response.
         Args:
            project: Project name on Azkaban
            flow: Flow id on Azkaban
            start: Start index of the returned list
            length: Length of the returned list
        Raises:
            FetchExecutionJobsLogError: when Azkaban api returns error in response
        Returns:
            Returns `Response` object as json friendly
        """

        self.__check_if_logged()

        response = api.fetch_executions_of_a_flow_request(
            self.__session,
            self.__session_id,
            project,
            flow,
            start,
            length
        )

        self.__catch_response_error(response, FetchExecutionsOfAFlowError)

        return response.json()

    def fetch_execution_job_log(self, execution_id: str, jobid: str, offset: str, length: str) -> json:
        """
        Fetches the correponding job logs.
        This method receives the execution id, jobid, offset and lenght, makes a fetch
        request to get the correponding job logs and evaluates the response.
        Args:
            exec_id: Execution id on Azkaban
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

        self.__check_if_logged()
        response = api.fetch_execution_job_log_request(
            self.__session,
            self.__host,
            self.__session_id,
            execution_id,
            jobid,
            offset,
            length
        )

        self.__catch_response_error(response, FetchExecutionJobsLogError)

        return response.json()

    def resume_flow_execution(self, execution_id: str) -> json:
        """
        Resume a flow execution for the Azkaban API
         Args:
            execution_id: Execution id to be resumed
        Raise:
            ResumeFlowExecutionError: when Azkaban api returns error in response
        Returns: 
            The response from the request made
        """
        self.__check_if_logged()

        response = api.resume_flow_execution(
            self.__session,
            self.__host,
            self.__session_id,
            execution_id
        )

        self.__catch_response_error(response, ResumeFlowExecutionError, ignore_empty_responses=True)

        return response.json()

    def fetch_running_executions_of_a_flow(self, project: str, flow: str) -> json:
        """
        Fetch running executions of a flow command, intended to make the request to Azkaban
        and treat the response properly.

        This method receives the project name and the flow id, making the fetch and evaluating the response.
        Args:
            project: Project name on Azkaban
            flow: Flow id on Azkaban
        Raise:
            FetchRunningExecutionsOfAFlowError: when Azkaban api returns error in response
        Returns: 
            Returns the json response from the request
        """

        self.__check_if_logged()

        response = api.fetch_running_executions_of_a_flow_request(
            self.__session,
            self.__host,
            self.__session_id,
            project,
            flow,
        )

        self.__catch_response_error(response, FetchRunningExecutionsOfAFlowError)

        return response.json()

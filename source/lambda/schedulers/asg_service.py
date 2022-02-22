######################################################################################################################
#  Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           #
#                                                                                                                    #
#  Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance        #
#  with the License. A copy of the License is located at                                                             #
#                                                                                                                    #
#      http://aws.amazon.com/asl/                                                                                    #
#                                                                                                                    #
#  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    #
#  and limitations under the License.                                                                                #
######################################################################################################################
import os
import time
from datetime import timedelta

import dateutil
import jmespath
from botocore.exceptions import ClientError
import re

import configuration
import schedulers
import time
from time import sleep
from boto_retry import get_client_with_retries
from configuration import SchedulerConfigBuilder
from configuration.instance_schedule import InstanceSchedule
from configuration.running_period import RunningPeriod

# instances are started in batches, larger bathes are more efficient but smaller batches allow more instances
# to start if we run into resource limits

#ALL FILE
#SPECIFIC_IMPLEMENTATION
START_BATCH_SIZE = 5
STOP_BATCH_SIZE = 50
UPDATE_ASG_CONFIG_SIZE = 20
ERR_STARTING_ASG_INSTANCES = "Error starting asg instances {}, ({})"
ERR_STOPPING_ASG_INSTANCES = "Error stopping asg instances {}, ({})"
INF_FETCHED_ASG_INSTANCES = "Number of fetched asg is {}"
INF_FETCHING_ASG_INSTANCES = "Fetching asg instances for account {} in region {}"
INF_ADD_KEYS = "Adding {} key(s) {} to asg instance(s) {}"
INFO_REMOVING_KEYS = "Removing {} key(s) {} from asg instance(s) {}"
WARN_STARTED_ASG_INSTANCES_TAGGING = "Error deleting or creating tags for started asg {} ({})"
WARN_STOPPED_ASG_INSTANCES_TAGGING = "Error deleting or creating tags for stopped asg {} ({})"
WARNING_ASG_INSTANCE_NOT_STARTING = "ASG instance {} is not started"
WARNING_ASG_INSTANCE_NOT_STOPPING = "ASG instance {} is not stopped"
DEBUG_SKIPPED_ASG_INSTANCE = "Skipping asg {} because it it not in a schedulable state ({})"
DEBUG_SELECTED_ASG_INSTANCE = "Selected asg {} in state ({})"


class AsgService:
    # """
    # Implements service start/stop functions for AutoScaling Group managed EC2 instances
    # """
    ASG_STATE_RUNNING = "running"
    ASG_STATE_TERMINATED = "stopped"

    ASG_SCHEDULABLE_STATES = {ASG_STATE_RUNNING, ASG_STATE_TERMINATED}
    ASG_STOPPED_STATES = {ASG_STATE_TERMINATED}
    ASG_STARTED_STATES = {ASG_STATE_RUNNING}

    def __init__(self):
        self.service_name = InstanceSchedule.ASG_SERVICE_NAME
        self.allow_resize = False
        self.schedules_with_hibernation = []
        self._ssm_maintenance_windows = None
        self._session = None
        self._logger = None
        self._instance_states = []

    def _init_scheduler(self, args):
        self._session = args.get(schedulers.PARAM_SESSION)
        self._context = args.get(schedulers.PARAM_CONTEXT)
        self._region = args.get(schedulers.PARAM_REGION)
        self._logger = args.get(schedulers.PARAM_LOGGER)
        self._account = args.get(schedulers.PARAM_ACCOUNT)
        self._tagname = args.get(schedulers.PARAM_TAG_NAME)

    @classmethod
    def instance_batches(cls, asg_instances, size):
        instance_buffer = []
        for asg in asg_instances:
            instance_buffer.append(asg)
            if len(instance_buffer) == size:
                yield instance_buffer
                instance_buffer = []
        if len(instance_buffer) > 0:
            yield instance_buffer

    # get instances and handle paging
    def get_schedulable_instances(self, kwargs):
        self._session = kwargs[schedulers.PARAM_SESSION]
        context = kwargs[schedulers.PARAM_CONTEXT]
        region = kwargs[schedulers.PARAM_REGION]
        account = kwargs[schedulers.PARAM_ACCOUNT]
        self._logger = kwargs[schedulers.PARAM_LOGGER]
        tagname = kwargs[schedulers.PARAM_CONFIG].tag_name
        config = kwargs[schedulers.PARAM_CONFIG]

        client = get_client_with_retries("autoscaling", ["describe_auto_scaling_groups"], context=context, session=self._session, region=region)

        def is_in_schedulable_state(asg):
            state = asg["state"]
            return state in AsgService.ASG_SCHEDULABLE_STATES

        jmes = "AutoScalingGroups[*].{AutoScalingGroupName:AutoScalingGroupName, DesiredCapacity:DesiredCapacity, MinSize:MinSize, InstanceNumber:length(Instances[?LifecycleState=='InService']), Tags:Tags}[]" + \
               " | [?Tags] | [?contains(Tags[*].Key, '{}')]".format(tagname)

        args = {}
        number_of_asgs = 0
        asg_instances = []
        done = False

        self._logger.info(INF_FETCHING_ASG_INSTANCES, account, region)

        while not done:

            asg_resp = client.describe_auto_scaling_groups_with_retries(**args)
            for asg in jmespath.search(jmes, asg_resp):
                asg = self._select_instance_data(asg=asg, tagname=tagname, config=config)
                #self._logger.info("Displaying asg instance data {}".format(asg))
                number_of_asgs += 1
                if is_in_schedulable_state(asg):
                    asg_instances.append(asg)
                    self._logger.debug(DEBUG_SELECTED_ASG_INSTANCE, asg[schedulers.INST_NAME], asg[schedulers.INST_STATE])
                else:
                    self._logger.warning(DEBUG_SKIPPED_ASG_INSTANCE, asg[schedulers.INST_NAME], asg[schedulers.INST_STATE])
            if "NextToken" in asg_resp:
                args["NextToken"] = asg_resp["NextToken"]
            else:
                done = True
        self._logger.info(INF_FETCHED_ASG_INSTANCES, number_of_asgs, len(asg_instances))
        return asg_instances

    # selects and builds a named tuple for the instance data
    def _select_instance_data(self, asg, tagname, config):

        def get_tags(asg):
            return {tag["Key"]: tag["Value"] for tag in asg["Tags"]} if "Tags" in asg else {}

        tags = get_tags(asg)
        id = asg['AutoScalingGroupName']
        name = asg['AutoScalingGroupName']
        schedule_name = tags.get(tagname)
        desired_capacity  = asg['DesiredCapacity']
        min_size = asg['MinSize']
        instance_number = asg['InstanceNumber']
        instance_type = 'asg'
        maintenance_window_schedule = None
        if tags.get('aws:cloudformation:logical-id') == 'ECSAutoScalingGroup':
            asg_is_ecs = 'true'
        else:
            asg_is_ecs = 'false'

        if instance_number == 0:
            state = AsgService.ASG_STATE_TERMINATED
        else:
            state = AsgService.ASG_STATE_RUNNING
        is_running = self.ASG_STATE_RUNNING == state
        is_terminated = state == self.ASG_STATE_TERMINATED

        asg_data = {
            schedulers.INST_ID: id,
            schedulers.INST_NAME: name,
            schedulers.INST_ASG_DESIRED_CAPACITY: desired_capacity,
            schedulers.INST_ASG_MIN_SIZE: min_size,
            schedulers.INST_SCHEDULE: schedule_name,
            schedulers.INST_STATE: state,
            schedulers.INST_ALLOW_RESIZE: self.allow_resize,
            schedulers.INST_IS_RUNNING: is_running,
            schedulers.INST_IS_TERMINATED: is_terminated,
            schedulers.INST_CURRENT_STATE: InstanceSchedule.STATE_RUNNING if is_running else InstanceSchedule.STATE_STOPPED,
            schedulers.INST_MAINTENANCE_WINDOW: maintenance_window_schedule,
            schedulers.INST_INSTANCE_TYPE: instance_type,
            schedulers.INST_ASG_IS_ECS: asg_is_ecs,
            schedulers.INST_TAGS: tags
        }
        self._logger.info("Displaying asg data {}".format(asg_data))
        return asg_data

    #######################
    ## A refactoring pour prendre en compte les differents statut de l'Asg
    #######################
    def get_asg_status(self, client, asg_ids):
        status_resp = client.describe_auto_scaling_groups_with_retries(AutoScalingGroupNames=asg_ids)
        jmes = "AutoScalingGroups[*].{AutoScalingGroupName:AutoScalingGroupName, InstanceNumber:length(Instances[?LifecycleState=='InService'])}"
        result = jmespath.search(jmes, status_resp)
        status = []
        for i in result:
            if i['InstanceNumber'] == 0:
                state = 'stopped'
            else:
                state = 'running'
            status.append({"AutoScalingGroupName":i['AutoScalingGroupName'], "State":state})
        return status

    def get_asg_termination_lifecycle_hook_name(self, client, asg):
        resp = client.describe_lifecycle_hooks(AutoScalingGroupName=asg)
        jmes = "LifecycleHooks[?contains(LifecycleHookName, 'Termination')].{LifecycleHookName:LifecycleHookName}"
        result = jmespath.search(jmes, resp)
        return result

    def get_asg_termination_lifecycle_hook_timeout(self, asg):
        methods = ["get_parameter"]
        ssm = get_client_with_retries("ssm", methods=methods, context=self._context, session=self._session, region=self._region)
        try:
            result = ssm.get_parameter(Name='/ecs/' + asg + '/lifecyclehook/heatbeat/timeout', WithDecryption=True)['Parameter']['Value']
        except ClientError as e:
            self._logger.error("Error whiling getting the {} lifecyclehook heatbeat timeout : {}".format(asg, e.response['Error']['Message']))
            return 300
        return result

    # noinspection PyMethodMayBeStatic
    def stop_instances(self, kwargs):

        # Optimization in the future
        def is_in_stopped_state(state):
            return state in AsgService.ASG_STOPPED_STATES

        self._init_scheduler(kwargs)
        stopped_asg_instances = kwargs[schedulers.PARAM_STOPPED_INSTANCES]
        stop_tags = kwargs[schedulers.PARAM_CONFIG].stopped_tags
        if stop_tags is None:
            stop_tags = []
        stop_tags_keys = [t["Key"] for t in stop_tags]
        stop_tags_values = [t["Value"] for t in stop_tags]
        start_tags_keys = [{"Key": t["Key"]} for t in kwargs[schedulers.PARAM_CONFIG].started_tags if
                           t["Key"] not in stop_tags_keys]

        methods = ["update_auto_scaling_group", "create_or_update_tags", "delete_tags", "describe_auto_scaling_groups", "put_lifecycle_hook"]
        client = get_client_with_retries("autoscaling", methods=methods, context=self._context, session=self._session, region=self._region)

        for asg_instance in list(self.instance_batches(stopped_asg_instances, STOP_BATCH_SIZE)):
            asg_ids = []
            asg_ecs_ids = []
            for i in asg_instance:
                if i.asg_is_ecs == 'true':
                    asg_ecs_ids.append(i.id)
                else:
                    asg_ids.append(i.id)
            self._logger.info("asg_ids : {}".format(asg_ids))
            self._logger.info("asg_ecs_ids : {}".format(asg_ecs_ids))

            asg_instances_ecs_stopped = []
            asg_instances_stopped = []
            if len(asg_ecs_ids) != 0:
                for asg in asg_ecs_ids:
                    try:
                        LifecycleHookName = self.get_asg_termination_lifecycle_hook_name(client, asg)[0]['LifecycleHookName']
                        HeartbeatTimeout = self.get_asg_termination_lifecycle_hook_timeout(asg)
                        self._logger.info("Displaying the LifecycleHook HeartbeatTimeout : {}".format(HeartbeatTimeout))
                        client.put_lifecycle_hook(AutoScalingGroupName=asg, LifecycleHookName=LifecycleHookName, HeartbeatTimeout=int(HeartbeatTimeout))
                    except ClientError as e:
                        self._logger.error("Error whiling updating the termination lifecycle hook heartbeat timeout : {}".format(e.response['Error']['Message']))
                    try:
                        stop_resp = client.update_auto_scaling_group(AutoScalingGroupName=asg, DesiredCapacity=0, MinSize=0)
                        asg_instances_ecs_stopped = [i["AutoScalingGroupName"] for i in self.get_asg_status(client, asg_ecs_ids)]
                    except Exception as ex:
                        self._logger.error(ERR_STOPPING_ASG_INSTANCES, ",".join(asg), str(ex))
            else:
                self._logger.info("No affected ECS found...")

            if len(asg_ids) != 0:
                for asg in asg_ids:
                    try:
                        stop_resp = client.update_auto_scaling_group(AutoScalingGroupName=asg, DesiredCapacity=0, MinSize=0)
                        asg_instances_stopped = [i["AutoScalingGroupName"] for i in self.get_asg_status(client, asg_ids)]
                    except Exception as ex:
                        self._logger.error(ERR_STOPPING_ASG_INSTANCES, ",".join(asg), str(ex))
            else:
                self._logger.info("No affected ASG found...")

            ###################
            # Status to verify
            ###################
            get_status_count = 0
            if len(asg_instances_ecs_stopped) < len(asg_ecs_ids):
                time.sleep(5)
                asg_instances_ecs_stopped = [i["AutoScalingGroupName"] for i in self.get_asg_status(client, asg_ecs_ids)]
                if len(asg_instances_ecs_stopped) == len(asg_instance):
                    break

                get_status_count += 1
                if get_status_count > 3:
                    for i in asg_instance:
                        if i not in asg_instances_ecs_stopped:
                            self._logger.warning(WARNING_ASG_INSTANCE_NOT_STOPPING, i)
                    break

            if len(asg_instances_stopped) < len(asg_ids):
                time.sleep(5)
                asg_instances_stopped = [i["AutoScalingGroupName"] for i in self.get_asg_status(client, asg_ids)]
                if len(asg_instances_stopped) == len(asg_instance):
                    break

                get_status_count += 1
                if get_status_count > 3:
                    for i in asg_instance:
                        if i not in asg_instances_stopped:
                            self._logger.warning(WARNING_ASG_INSTANCE_NOT_STOPPING, i)
                    break

            if len(asg_instances_ecs_stopped) > 0:
                try:
                    if start_tags_keys is not None and len(start_tags_keys):
                        self._logger.info(INFO_REMOVING_KEYS, "start",
                                          ",".join(["\"{}\"".format(k["Key"]) for k in start_tags_keys]),
                                          ",".join(asg_instances_ecs_stopped))
                        for asg in asg_instances_ecs_stopped:
                            for k in start_tags_keys:
                                client.delete_tags_with_retries(Tags=[{'ResourceId': asg, 'ResourceType': "auto-scaling-group", 'Key': k["Key"]}])
                    if len(stop_tags) > 0:
                        self._logger.info(INF_ADD_KEYS, "stop", str(stop_tags), ",".join(asg_instances_ecs_stopped))
                        for asg in asg_instances_ecs_stopped:
                            for k in stop_tags:
                                client.create_or_update_tags_with_retries(Tags=[{'ResourceId': asg, 'ResourceType': "auto-scaling-group", 'Key': k["Key"], 'Value': k["Value"], 'PropagateAtLaunch': False}])
                except Exception as ex:
                    self._logger.warning(WARN_STOPPED_ASG_INSTANCES_TAGGING, ','.join(asg_instances_ecs_stopped), str(ex))

                for i in asg_instances_ecs_stopped:
                    yield i, InstanceSchedule.STATE_STOPPED

            if len(asg_instances_stopped) > 0:
                try:
                    if start_tags_keys is not None and len(start_tags_keys):
                        self._logger.info(INFO_REMOVING_KEYS, "start",
                                          ",".join(["\"{}\"".format(k["Key"]) for k in start_tags_keys]),
                                          ",".join(asg_instances_stopped))
                        for asg in asg_instances_stopped:
                            for k in start_tags_keys:
                                client.delete_tags_with_retries(Tags=[{'ResourceId': asg, 'ResourceType': "auto-scaling-group", 'Key': k["Key"]}])
                    if len(stop_tags) > 0:
                        self._logger.info(INF_ADD_KEYS, "stop", str(stop_tags), ",".join(asg_instances_stopped))
                        for asg in asg_instances_stopped:
                            for k in stop_tags:
                                client.create_or_update_tags_with_retries(Tags=[{'ResourceId': asg, 'ResourceType': "auto-scaling-group", 'Key': k["Key"], 'Value': k["Value"], 'PropagateAtLaunch': False}])
                except Exception as ex:
                    self._logger.warning(WARN_STOPPED_ASG_INSTANCES_TAGGING, ','.join(asg_instances_stopped), str(ex))

                for i in asg_instances_stopped:
                    yield i, InstanceSchedule.STATE_STOPPED

    # noinspection PyMethodMayBeStatic
    def start_instances(self, kwargs):

        def is_in_started_state(state):
            return state in AsgService.ASG_STARTED_STATES

        self._init_scheduler(kwargs)
        asg_instances_to_start = kwargs[schedulers.PARAM_STARTED_INSTANCES]
        start_tags = kwargs[schedulers.PARAM_CONFIG].started_tags
        asg_conf = kwargs[schedulers.PARAM_ASG_CONF]
        if start_tags is None:
            start_tags = []
        start_tags_keys = [t["Key"] for t in start_tags]
        start_tags_values = [t["Value"] for t in start_tags]
        stop_tags_keys = [{"Key": t["Key"]} for t in kwargs[schedulers.PARAM_CONFIG].stopped_tags if
                          t["Key"] not in start_tags_keys]

        client = get_client_with_retries("autoscaling", ["update_auto_scaling_group", "describe_auto_scaling_groups", "create_or_update_tags", "delete_tags", "put_lifecycle_hook"],
                                         context=self._context, session=self._session, region=self._region)

        for asg_instance in self.instance_batches(asg_instances_to_start, START_BATCH_SIZE):

            asg_ids = []
            asg_ecs_ids = []
            for i in list(asg_instance):
                if i.asg_is_ecs == 'true':
                    asg_ecs_ids.append(i.id)
                else:
                    asg_ids.append(i.id)

            asg_instances_ecs_started = []
            asg_instances_started = []
            if len(asg_ecs_ids) != 0:
                for asg in asg_ecs_ids:
                    try:
                        LifecycleHookName = self.get_asg_termination_lifecycle_hook_name(client, asg)[0]['LifecycleHookName']
                        client.put_lifecycle_hook(AutoScalingGroupName=asg, LifecycleHookName=LifecycleHookName, HeartbeatTimeout=3600)
                    except ClientError as e:
                        self._logger.error("Error whiling updating the termination lifecycle hook heartbeat timeout : {}".format(e.response['Error']['Message']))
                    try:
                        for key, value in asg_conf.items():
                            if key == asg:
                                start_resp = client.update_auto_scaling_group(AutoScalingGroupName=asg, DesiredCapacity=value['desired_capacity'], MinSize=value['min_size'])
                                asg_instances_ecs_started = [i["AutoScalingGroupName"] for i in self.get_asg_status(client, asg_ecs_ids)]
                    except Exception as ex:
                        self._logger.error(ERR_STARTING_ASG_INSTANCES, ",".join(asg), str(ex))
            else:
                self._logger.info("No affected ECS found...")

            if len(asg_ids) != 0:
                for asg in asg_ids:
                    try:
                        for key, value in asg_conf.items():
                            if key == asg:
                                start_resp = client.update_auto_scaling_group(AutoScalingGroupName=asg, DesiredCapacity=value['desired_capacity'], MinSize=value['min_size'])
                                asg_instances_started = [i["AutoScalingGroupName"] for i in self.get_asg_status(client, asg_ids)]
                    except Exception as ex:
                        self._logger.error(ERR_STARTING_ASG_INSTANCES, ",".join(asg), str(ex))
            else:
                self._logger.info("No affected ASG found...")

            get_status_count = 0
            if len(asg_instances_ecs_started) < len(asg_ecs_ids):
                time.sleep(5)
                if len(asg_instances_ecs_started) == len(asg_ecs_ids):
                    break

                get_status_count += 1
                if get_status_count > 3:
                    for i in asg_ecs_ids:
                        if i not in asg_instances_ecs_started:
                            self._logger.warning(WARNING_ASG_INSTANCE_NOT_STARTING, i)
                    break

            if len(asg_instances_started) < len(asg_ids):
                time.sleep(5)
                if len(asg_instances_started) == len(asg_ids):
                    break

                get_status_count += 1
                if get_status_count > 3:
                    for i in asg_ids:
                        if i not in asg_instances_started:
                            self._logger.warning(WARNING_ASG_INSTANCE_NOT_STARTING, i)
                    break

            if len(asg_instances_ecs_started) > 0:
                try:
                    if stop_tags_keys is not None and len(stop_tags_keys) > 0:
                        self._logger.info(INFO_REMOVING_KEYS, "stop",
                                          ",".join(["\"{}\"".format(k["Key"]) for k in stop_tags_keys]),
                                          ",".join(asg_instances_ecs_started))
                        for asg in asg_instances_ecs_started:
                            for k in stop_tags_keys:
                                client.delete_tags_with_retries(Tags=[{'ResourceId': asg, 'ResourceType': "auto-scaling-group", 'Key': k["Key"]}])
                    if len(start_tags) > 0:
                        self._logger.info(INF_ADD_KEYS, "start", str(start_tags), ",".join(asg_instances_ecs_started))
                        for asg in asg_instances_ecs_started:
                            for k in start_tags:
                                client.create_or_update_tags_with_retries(Tags=[{'ResourceId': asg, 'ResourceType': "auto-scaling-group", 'Key': k["Key"], 'Value': k["Value"], 'PropagateAtLaunch': False}])
                except Exception as ex:
                    self._logger.warning(WARN_STARTED_ASG_INSTANCES_TAGGING, ','.join(asg_instances_ecs_started), str(ex))

                for i in asg_instances_ecs_started:
                    yield i, InstanceSchedule.STATE_RUNNING

            if len(asg_instances_started) > 0:
                try:
                    if stop_tags_keys is not None and len(stop_tags_keys) > 0:
                        self._logger.info(INFO_REMOVING_KEYS, "stop",
                                          ",".join(["\"{}\"".format(k["Key"]) for k in stop_tags_keys]),
                                          ",".join(asg_instances_started))
                        for asg in asg_instances_started:
                            for k in stop_tags_keys:
                                client.delete_tags_with_retries(Tags=[{'ResourceId': asg, 'ResourceType': "auto-scaling-group", 'Key': k["Key"]}])
                    if len(start_tags) > 0:
                        self._logger.info(INF_ADD_KEYS, "start", str(start_tags), ",".join(asg_instances_started))
                        for asg in asg_instances_started:
                            for k in start_tags:
                                client.create_or_update_tags_with_retries(Tags=[{'ResourceId': asg, 'ResourceType': "auto-scaling-group", 'Key': k["Key"], 'Value': k["Value"], 'PropagateAtLaunch': False}])
                except Exception as ex:
                    self._logger.warning(WARN_STARTED_ASG_INSTANCES_TAGGING, ','.join(asg_instances_started), str(ex))

                for i in asg_instances_started:
                    yield i, InstanceSchedule.STATE_RUNNING

import boto3
import logging

from datetime import timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.utils.dates import days_ago

# from settings import SUBNET_ID, MASTER_SG_ID, METRICS_ETL_ARTIFACTS_DIST, METRICS_ETL_ARTIFACTS_BOOTSTRAP, METRICS_ETL_ARTIFACTS_SETTINGS, METRICS_ETL_ARTIFACTS_MAIN, METRICS_ETL_LOGS, ENV, REGION
ETL_ARTIFACTS_DIST = ''
ETL_ARTIFACTS_SETTINGS = ''
ETL_ARTIFACTS_MAIN = ''
ETL_ARTIFACTS_BOOTSTRAP = ''

EMR_INSTANCE_GROUPS = [
    {
        'Name': "Master Instance Group",
        'EbsConfiguration': {
            'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'SizeInGB': 32,
                                    'VolumeType': 'gp2'
                                },
                                'VolumesPerInstance': 2
                            }
            ]
        },
        'InstanceRole': 'MASTER',
        'InstanceType': 'c5.xlarge',
        'InstanceCount': 1,
    },
    {
        'Name': "Core Instance Group",
        'EbsConfiguration': {
            'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'SizeInGB': 32,
                                    'VolumeType': 'gp2'
                                },
                                'VolumesPerInstance': 2
                            }
            ]
        },
        'InstanceRole': 'CORE',
        'InstanceType': 'c5.xlarge',
        'InstanceCount': 2,
    }
]

EMR_BOOTSTRAP_ACTIONS = [
    {
        'Name': 'Install deps',
        'ScriptBootstrapAction': {
                'Path': ETL_ARTIFACTS_BOOTSTRAP
        }
    },
]

JOB_FLOW_OVERRIDES = {
    'Name': 'PiCalc',
    'ReleaseLabel': 'emr-5.29.0',
    'Instances': {
        'InstanceGroups': EMR_INSTANCE_GROUPS,
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'BootstrapActions': EMR_BOOTSTRAP_ACTIONS,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}


SPARK_STEPS = [
    {
        'Name': 'lda_topics_classifier',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Properties': [],
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--py-files',
                ETL_ARTIFACTS_DIST,
                '--master',
                'yarn',
                '--files',
                ETL_ARTIFACTS_SETTINGS,
                '--packages',
                'com.amazonaws:aws-java-sdk:1.11.900,org.apache.hadoop:hadoop-aws:3.2.0',
                ETL_ARTIFACTS_MAIN,
                # f'--k={k_number}',
                # f'--iteration={max_iter}',
                # f'--seed={random_seed}'
            ]
        }
    }
]


DEFAULT_ARGS = {
    'owner': 'zdjohn',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='emr_job',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(1),
    tags=['lda'],
) as dag:

    s3_sensor = S3KeySensor(
        bucket_key={{vars.base_bucket}}, bucket_name={{vars.source_file}})

    cluster_creator = EmrCreateJobFlowOperator(
        task_id='spin_up_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
    )

    etl_step_add = EmrAddStepsOperator(
        task_id='etl_step_add',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='spin_up_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
    )

    emr_step_listener = EmrStepSensor(
        task_id='emr_step_listener',
        job_flow_id="{{ task_instance.xcom_pull('spin_up_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='etl_step_add', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='spin_up_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
    )

    s3_sensor >> cluster_creator >> etl_step_add >> emr_step_listener >> cluster_remover

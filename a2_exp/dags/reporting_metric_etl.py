from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.models.dag import dag
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.utils.dates import days_ago

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


def _metrics_job_steps(job, source, target):
    return [
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
                    f'--job={job}',
                    f'--sourceDir={source}',
                    f'--targetDir={target}'
                ]
            }
        }
    ]


DEFAULT_ARGS = {
    'owner': 'zdjohn',
    'depends_on_past': False,
    "start_date": datetime(2021, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}

METRIC_ETL_DAG_ID = 'metrics_etl_emr'


@dag(dag_id=METRIC_ETL_DAG_ID, default_args=DEFAULT_ARGS, tags=['lda'])
def metrics_etl_emr():
    jobs_list = ['1', '2', '3']

    cluster_creator = EmrCreateJobFlowOperator(
        task_id='spin_up_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
    )

    cluster_id = cluster_creator.output

    steps_added = {}

    for job_name in jobs_list:
        # TODO: finish source target by time and job
        source = f'{job_name}'
        target = f'{job_name}'
        etl_step_add = EmrAddStepsOperator(
            task_id=f'etl_{job_name}_step_add',
            job_flow_id=cluster_id,
            aws_conn_id='aws_default',
            steps=_metrics_job_steps(job_name, source, target),
        )
        steps_added[job_name] = etl_step_add

    step_success_sensors = []
    for job_name in steps_added:
        emr_step_listener = EmrStepSensor(
            task_id=f'emr_{job_name}_step_listener',
            job_flow_id=cluster_id,
            step_id=steps_added[job_name].output[0],
            aws_conn_id='aws_default',
        )
        steps_added[job_name] >> emr_step_listener
        step_success_sensors.append(emr_step_listener)

    cluster_terminator = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id=cluster_id,
        aws_conn_id='aws_default',
    )

    cluster_creator >> list(steps_added.values())
    step_success_sensors >> cluster_terminator


dag = metrics_etl_emr()

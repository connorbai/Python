import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket', 'parent_folder'])
job.init(args['JOB_NAME'], args)
BUCKET_NAME = args['bucket'] ## lly-cn-ibu-cmds-qa-private
FOLDER = args['parent_folder'] ## = cmds-glue/cmds-glue-process-ods-td-to-postgresql
print("Upload file S3 path is: {}, glue Job: {} Start ...".format(FOLDER, args['JOB_NAME']))


def truncate_and_save(sdf, table):
    glue = boto3.client('glue')
    connection = glue.get_connection(Name="conn-cmds-qa-rds-postgre-writer")
    pg_url = connection['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
    pg_user = connection['Connection']['ConnectionProperties']['USERNAME']
    pg_password = connection['Connection']['ConnectionProperties']['PASSWORD']

    df = sdf.toDF()
    print('Table: {}, count: {}'.format(table, df.count()))
    df.write.format("jdbc").mode('overwrite') \
        .option("url", pg_url) \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .option("dbtable", table) \
        .option("truncate", "true") \
        .save()

try:
    ############################# algnmnt #############################
    algnmnt_frame = glueContext.create_dynamic_frame.from_options(
        format_options={
            "jsonPath": "$[*]",
            "multiLine": "false",
            # "useGlueParquetWriter": True,
            # "BlockSize": "268435456",
            # "PageSize": "1048576",
        },
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [f"s3://{BUCKET_NAME}/{FOLDER}/ADS_CUST_ALGNMNT.json"],
            "recurse": True,
            'groupFiles': 'inPartition',
            'groupSize': '10485760'
        },
        transformation_ctx="algnmnt_frame"
    )

    # Script generated for node Change Schema # id dlt_indctr src_external_id
    algnmnt_frame_1 = ApplyMapping.apply(
        frame=algnmnt_frame,
        mappings=[
            ("algnmnt_id", "string", "algnmnt_id", "string"),
            ("algnmnt_country_cd", "string", "algnmnt_country_cd", "string"),
            ("cust_id", "string", "cust_id", "string"),
            ("cust_algnmnt_strt_dt", "string", "cust_algnmnt_strt_dt", "timestamp"),
            ("cust_algnmnt_end_dt", "string", "cust_algnmnt_end_dt", "timestamp"),
            ("cust_typ_cd", "string", "cust_typ_cd", "string"),
            ("cust_algnmnt_typ_cd", "string", "cust_algnmnt_typ_cd", "string"),
            ("src_algnmnt_typ_cd", "string", "src_algnmnt_typ_cd", "string"),
            ("crm_sync_flg", "string", "crm_sync_flg", "string"),
            ("ods_ld_dt", "string", "ods_ld_dt", "timestamp"),
            ("ods_lst_updt_dt", "string", "ods_lst_updt_dt", "timestamp"),
            ("src_sys_cd", "string", "src_sys_cd", "string"),
            ("ods_ld_usr", "string", "ods_ld_usr", "string"),
            ("ods_lst_updt_usr", "string", "ods_lst_udpt_usr", "string")
        ],
        transformation_ctx="algnmnt_frame_1"
    )
    truncate_and_save(algnmnt_frame_1, 'cmd_owner.i_ads_cust_algnmnt')


    ############################# algnmnt tier #############################
    tier_frame = glueContext.create_dynamic_frame.from_options(
        format_options={
            "jsonPath": "$[*]",
            "multiLine": "false",
            # "useGlueParquetWriter": True,
            # "BlockSize": "268435456",
            # "PageSize": "1048576",
        },
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [f"s3://{BUCKET_NAME}/{FOLDER}/ADS_CUST_ALGNMNT_TIER.json"],
            "recurse": True,
            'groupFiles': 'inPartition',
            'groupSize': '10485760'
        },
        transformation_ctx="tier_frame"
    )
    # id
    tier_frame1 = ApplyMapping.apply(
        frame=tier_frame,
        mappings=[
            ("actl_tier", "string", "actl_tier", "string"),
            ("algnmnt_id", "string", "algnmnt_id", "string"),
            ("algnmnt_country_cd", "string", "algnmnt_country_cd", "string"),
            ("cust_id", "string", "cust_id", "string"),
            ("cust_tier_end_dt", "string", "cust_tier_end_dt", "timestamp"),
            ("cust_tier_strt_dt", "string", "cust_tier_strt_dt", "timestamp"),
            ("cust_typ_cd", "string", "cust_typ_cd", "string"),
            ("recmnd_tier", "string", "recmnd_tier", "string"),
            ("cust_algnmnt_typ_cd", "string", "cust_algnmnt_typ_cd", "string"),
            ("src_algnmnt_typ_cd", "string", "src_algnmnt_typ_cd", "string"),
            ("ods_ld_dt", "string", "ods_ld_dt", "timestamp"),
            ("ods_lst_updt_dt", "string", "ods_lst_updt_dt", "timestamp"),
            ("src_sys_cd", "string", "src_sys_cd", "string"),
            ("ods_ld_usr", "string", "ods_ld_usr", "string"),
            ("ods_lst_updt_usr", "string", "ods_lst_udpt_usr", "string")
        ],
        transformation_ctx="tier_frame1"
    )
    truncate_and_save(tier_frame1, 'cmd_owner.i_ads_cust_algnmnt_tier_vw')
    job.commit()


    ############################# run lambda #############################
    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(FunctionName='cmds-qa-s3-odsTds', LogType='None', InvocationType='Event')
    print(response)
    print('Invoke lambda done.')
except Exception as e:
    print(e)
    raise e
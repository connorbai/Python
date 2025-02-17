import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.functions import col

try:
    ## @params: [JOB_NAME]
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket', 'parent_folder'])
    # args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    BucketName = args['bucket']
    # BucketName = "lly-cn-ibu-cmds-ods-qa-private"
    print("BucketName is: {}".format(BucketName))
    folder = args['parent_folder']
    # folder = "cmds-glue/input/algnmnt/2021-07-07"
    print("Upload file S3 path is: {}".format(folder))

    process_bucket = 'lly-cn-ibu-cmds-qa-private'
    process_folder = 'cmds-glue/cmds-glue-process-ods-td-to-postgresql/'

    # Parameter which should be changed and checked before deployment
    glue_catalog_database_name = "cmds-glue-qa-catalog-database"
    table_i_ads_cust_algnmnt = "cmd_owner.i_ads_cust_algnmnt"
    table_i_ads_cust_algnmnt_tier_vw = "cmd_owner.i_ads_cust_algnmnt_tier_vw"

    glue_postgre_table_i_ads_cust_algnmnt = "cmds_qa_cmdsqa_cmd_owner_i_ads_cust_algnmnt"
    glue_postgre_table_i_ads_cust_algnmnt_tier_vw = "cmds_qa_cmdsqa_cmd_owner_i_ads_cust_algnmnt_tier_vw"

    csv_file_name_i_ads_cust_algnmnt = "i_ads_cust_algnmnt.csv"
    csv_file_name_i_ads_cust_algnmnt_tier_vw = "i_ads_cust_algnmnt_tier_vw.csv"

    upload_csv_path_i_ads_cust_algnmnt = folder + "/" + csv_file_name_i_ads_cust_algnmnt
    upload_csv_path_i_ads_cust_algnmnt_tier_vw = folder + "/" + csv_file_name_i_ads_cust_algnmnt_tier_vw

    glue_s3_table_i_ads_cust_algnmnt_csv = "cmds_qa_i_ads_cust_algnmnt_csv"
    glue_s3_table_i_ads_cust_algnmnt_tier_vw_csv = "cmds_qa_i_ads_cust_algnmnt_tier_vw_csv"

    Lambda_function_name = 'cmds-qa-s3-odsTds'

    ## Delete file in process bucket
    import boto3

    client = boto3.client('s3')
    response = client.delete_objects(
        Bucket=process_bucket,
        Delete={
            'Objects': [
                {
                    'Key': process_folder + csv_file_name_i_ads_cust_algnmnt
                },
                {
                    'Key': process_folder + csv_file_name_i_ads_cust_algnmnt_tier_vw
                },

            ],
            'Quiet': True
        }
    )
    print('Delete files in process folder done.')
    print(response)

    ## Start copy source file from upload folder to process folder
    i_ads_cust_algnmnt_copy_source = {'Bucket': BucketName, 'Key': upload_csv_path_i_ads_cust_algnmnt}
    i_ads_cust_algnmnt_tier_vw_source = {'Bucket': BucketName, 'Key': upload_csv_path_i_ads_cust_algnmnt_tier_vw}
    print('Upload file {} path is: {}'.format(csv_file_name_i_ads_cust_algnmnt, i_ads_cust_algnmnt_copy_source))
    print('Upload file {} path is: {}'.format(csv_file_name_i_ads_cust_algnmnt_tier_vw,
                                              i_ads_cust_algnmnt_tier_vw_source))

    client.copy(CopySource=i_ads_cust_algnmnt_copy_source, Bucket=process_bucket,
                Key=process_folder + csv_file_name_i_ads_cust_algnmnt)
    print("File: {} moving to process folder done.".format(csv_file_name_i_ads_cust_algnmnt))

    client.copy(CopySource=i_ads_cust_algnmnt_tier_vw_source, Bucket=process_bucket,
                Key=process_folder + csv_file_name_i_ads_cust_algnmnt_tier_vw)
    print("File: {} moving to process folder done.".format(csv_file_name_i_ads_cust_algnmnt_tier_vw))

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    print("cmds glue Job: {} Start ...".format(args['JOB_NAME']))

    ## @type: DataSource
    ## @args: [database = glue_catalog_database_name, table_name = glue_s3_table_i_ads_cust_algnmnt_csv, transformation_ctx = "datasource10"]
    ## @return: datasource10
    ## @inputs: []
    datasource10 = glueContext.create_dynamic_frame.from_catalog(database=glue_catalog_database_name,
                                                                 table_name=glue_s3_table_i_ads_cust_algnmnt_csv,
                                                                 transformation_ctx="datasource10")
    ## @type: ApplyMapping
    ## @args: [mapping = [("algnmnt_id", "string", "algnmnt_id", "string"), ("cust_id", "string", "cust_id", "string"), ("cust_algnmnt_strt_dt", "string", "cust_algnmnt_strt_dt", "timestamp"), ("cust_algnmnt_end_dt", "string", "cust_algnmnt_end_dt", "timestamp"), ("cust_typ_cd", "string", "cust_typ_cd", "string"), ("src_external_id", "long", "src_external_id", "string"), ("cust_algnmnt_typ_cd", "string", "cust_algnmnt_typ_cd", "string"), ("crm_sync_flg", "string", "crm_sync_flg", "string"), ("ods_ld_dt", "string", "ods_ld_dt", "timestamp"), ("ods_lst_updt_dt", "string", "ods_lst_updt_dt", "timestamp"), ("src_sys_cd", "string", "src_sys_cd", "string"), ("ods_ld_usr", "string", "ods_ld_usr", "string"), ("ods_lst_updt_usr", "string", "ods_lst_udpt_usr", "string"), ("algnmnt_country_cd", "string", "algnmnt_country_cd", "string"), ("src_algnmnt_typ_cd", "string", "src_algnmnt_typ_cd", "string"), ("dlt_indctr", "long", "dlt_indctr", "string")], transformation_ctx = "applymapping11"]
    ## @return: applymapping11
    ## @inputs: [frame = datasource10]
    applymapping11 = ApplyMapping.apply(frame=datasource10, mappings=[("algnmnt_id", "string", "algnmnt_id", "string"),
                                                                      ("cust_id", "string", "cust_id", "string"), (
                                                                      "cust_algnmnt_strt_dt", "timestamp",
                                                                      "cust_algnmnt_strt_dt", "timestamp"), (
                                                                      "cust_algnmnt_end_dt", "timestamp",
                                                                      "cust_algnmnt_end_dt", "timestamp"), (
                                                                      "cust_typ_cd", "string", "cust_typ_cd", "string"),
                                                                      ("src_external_id", "long", "src_external_id",
                                                                       "string"), ("cust_algnmnt_typ_cd", "string",
                                                                                   "cust_algnmnt_typ_cd", "string"), (
                                                                      "crm_sync_flg", "string", "crm_sync_flg",
                                                                      "string"), ("ods_ld_dt", "timestamp", "ods_ld_dt",
                                                                                  "timestamp"), (
                                                                      "ods_lst_updt_dt", "timestamp", "ods_lst_updt_dt",
                                                                      "timestamp"),
                                                                      ("src_sys_cd", "string", "src_sys_cd", "string"),
                                                                      ("ods_ld_usr", "string", "ods_ld_usr", "string"),
                                                                      ("ods_lst_updt_usr", "string", "ods_lst_udpt_usr",
                                                                       "string"), ("algnmnt_country_cd", "string",
                                                                                   "algnmnt_country_cd", "string"), (
                                                                      "src_algnmnt_typ_cd", "string",
                                                                      "src_algnmnt_typ_cd", "string"),
                                                                      ("dlt_indctr", "long", "dlt_indctr", "string")],
                                        transformation_ctx="applymapping11")
    ## @type: SelectFields
    ## @args: [paths = ["algnmnt_id", "dlt_indctr", "src_external_id", "cust_typ_cd", "ods_ld_usr", "src_algnmnt_typ_cd", "ods_lst_udpt_usr", "cust_algnmnt_end_dt", "algnmnt_country_cd", "cust_algnmnt_strt_dt", "cust_algnmnt_typ_cd", "ods_lst_updt_dt", "src_sys_cd", "id", "crm_sync_flg", "cust_id", "ods_ld_dt"], transformation_ctx = "selectfields12"]
    ## @return: selectfields12
    ## @inputs: [frame = applymapping11]
    selectfields12 = SelectFields.apply(frame=applymapping11,
                                        paths=["algnmnt_id", "dlt_indctr", "src_external_id", "cust_typ_cd",
                                               "ods_ld_usr", "src_algnmnt_typ_cd", "ods_lst_udpt_usr",
                                               "cust_algnmnt_end_dt", "algnmnt_country_cd", "cust_algnmnt_strt_dt",
                                               "cust_algnmnt_typ_cd", "ods_lst_updt_dt", "src_sys_cd", "id",
                                               "crm_sync_flg", "cust_id", "ods_ld_dt"],
                                        transformation_ctx="selectfields12")
    ## @type: ResolveChoice
    ## @args: [choice = "MATCH_CATALOG", database = glue_catalog_database_name, table_name = "cmds_qa_cmdsqa_cmd_owner_i_ads_cust_algnmnt", transformation_ctx = "resolvechoice13"]
    ## @return: resolvechoice13
    ## @inputs: [frame = selectfields12]
    resolvechoice13 = ResolveChoice.apply(frame=selectfields12, choice="MATCH_CATALOG",
                                          database=glue_catalog_database_name,
                                          table_name="cmds_qa_cmdsqa_cmd_owner_i_ads_cust_algnmnt",
                                          transformation_ctx="resolvechoice13")
    ## @type: ResolveChoice
    ## @args: [choice = "make_cols", transformation_ctx = "resolvechoice14"]
    ## @return: resolvechoice14
    ## @inputs: [frame = resolvechoice13]
    resolvechoice14 = ResolveChoice.apply(frame=resolvechoice13, choice="make_cols",
                                          transformation_ctx="resolvechoice14")

    print("All rows is {}".format(resolvechoice14.count()))

    i_ads_cust_algnmnt_Loading = DropNullFields.apply(frame=resolvechoice14,
                                                      transformation_ctx="i_ads_cust_algnmnt_Loading")
    print("After drop null fields the rows is {}".format(i_ads_cust_algnmnt_Loading.count()))

    i_ads_cust_algnmnt_df = i_ads_cust_algnmnt_Loading.toDF()
    i_ads_cust_algnmnt_df.printSchema()
    # i_ads_cust_algnmnt_df.show()

    i_ads_cust_algnmnt_temp_df = i_ads_cust_algnmnt_df.withColumn("cust_algnmnt_strt_dt",
                                                                  F.unix_timestamp("cust_algnmnt_strt_dt",
                                                                                   'MM/dd/yyyy HH:mm:ss').cast(
                                                                      "timestamp")) \
        .withColumn("cust_algnmnt_end_dt",
                    F.unix_timestamp("cust_algnmnt_end_dt", 'MM/dd/yyyy HH:mm:ss').cast("timestamp")) \
        .withColumn("ods_ld_dt", F.unix_timestamp("ods_ld_dt", 'MM/dd/yyyy HH:mm:ss').cast("timestamp")) \
        .withColumn("ods_lst_updt_dt", F.unix_timestamp("ods_lst_updt_dt", 'MM/dd/yyyy HH:mm:ss').cast("timestamp"))

    i_ads_cust_algnmnt_temp_df.printSchema()
    # i_ads_cust_algnmnt_temp_df.show()

    conn = glueContext.extract_jdbc_conf('conn-cmds-qa-rds-postgre-writer')
    # print("conn:",conn)
    url = conn['url']
    # print("url",url)
    USERNAME = conn['user']
    # print("username:",USERNAME)
    PASSWORD = conn['password']
    DATABASE = "cmdsqa"
    # print("database:",DATABASE)
    URL = url + "/" + DATABASE + "?stringtype=unspecified"
    # print(URL)
    DRIVER = "org.postgresql.Driver"

    i_ads_cust_algnmnt_temp_df.write \
        .format("jdbc") \
        .option("driver", DRIVER) \
        .option("url", URL) \
        .option("dbtable", table_i_ads_cust_algnmnt) \
        .option("user", USERNAME) \
        .option("password", PASSWORD) \
        .option("truncate", "true") \
        .option("ssl", "true") \
        .option("sslfactory", "org.postgresql.ssl.NonValidatingFactory") \
        .option("sslmode", "require") \
        .mode("overwrite") \
        .save()

    print('Load all the data into table {} done.'.format(table_i_ads_cust_algnmnt))

    datasource0 = glueContext.create_dynamic_frame.from_catalog(database=glue_catalog_database_name,
                                                                table_name=glue_s3_table_i_ads_cust_algnmnt_tier_vw_csv,
                                                                transformation_ctx="datasource0")
    applymapping1 = ApplyMapping.apply(frame=datasource0, mappings=[("actl_tier", "long", "actl_tier", "string"),
                                                                    ("algnmnt_id", "string", "algnmnt_id", "string"), (
                                                                    "algnmnt_country_cd", "string",
                                                                    "algnmnt_country_cd", "string"),
                                                                    ("cust_id", "string", "cust_id", "string"), (
                                                                    "cust_tier_end_dt", "timestamp", "cust_tier_end_dt",
                                                                    "timestamp"), ("cust_tier_strt_dt", "timestamp",
                                                                                   "cust_tier_strt_dt", "timestamp"),
                                                                    ("cust_typ_cd", "string", "cust_typ_cd", "string"),
                                                                    ("recmnd_tier", "long", "recmnd_tier", "string"), (
                                                                    "cust_algnmnt_typ_cd", "string",
                                                                    "cust_algnmnt_typ_cd", "string"), (
                                                                    "src_algnmnt_typ_cd", "string",
                                                                    "src_algnmnt_typ_cd", "string"), (
                                                                    "ods_ld_dt", "timestamp", "ods_ld_dt", "timestamp"),
                                                                    ("ods_lst_updt_dt", "timestamp", "ods_lst_updt_dt",
                                                                     "timestamp"),
                                                                    ("src_sys_cd", "string", "src_sys_cd", "string"),
                                                                    ("ods_ld_usr", "string", "ods_ld_usr", "string"), (
                                                                    "ods_lst_updt_usr", "string", "ods_lst_udpt_usr",
                                                                    "string")], transformation_ctx="applymapping1")
    selectfields2 = SelectFields.apply(frame=applymapping1,
                                       paths=["algnmnt_id", "recmnd_tier", "cust_typ_cd", "ods_ld_usr",
                                              "src_algnmnt_typ_cd", "cust_tier_end_dt", "ods_lst_udpt_usr",
                                              "cust_tier_strt_dt", "algnmnt_country_cd", "actl_tier",
                                              "cust_algnmnt_typ_cd", "ods_lst_updt_dt", "src_sys_cd", "id", "cust_id",
                                              "ods_ld_dt"], transformation_ctx="selectfields2")
    resolvechoice3 = ResolveChoice.apply(frame=selectfields2, choice="MATCH_CATALOG",
                                         database=glue_catalog_database_name,
                                         table_name=glue_postgre_table_i_ads_cust_algnmnt_tier_vw,
                                         transformation_ctx="resolvechoice3")
    resolvechoice4 = ResolveChoice.apply(frame=resolvechoice3, choice="make_cols", transformation_ctx="resolvechoice4")

    print("All rows is {}".format(resolvechoice4.count()))

    i_ads_cust_algnmnt_view_Loading = DropNullFields.apply(frame=resolvechoice4,
                                                           transformation_ctx="i_ads_cust_algnmnt_view_Loading")
    print("After drop null fields the rows is {}".format(i_ads_cust_algnmnt_view_Loading.count()))

    i_ads_cust_algnmnt_view_df = i_ads_cust_algnmnt_view_Loading.toDF()
    i_ads_cust_algnmnt_view_df.printSchema()
    # i_ads_cust_algnmnt_view_df.show()

    i_ads_cust_algnmnt_view_temp_df = i_ads_cust_algnmnt_view_df.withColumn("cust_tier_end_dt",
                                                                            F.unix_timestamp("cust_tier_end_dt",
                                                                                             'MM/dd/yyyy HH:mm:ss').cast(
                                                                                "timestamp")) \
        .withColumn("cust_tier_strt_dt", F.unix_timestamp("cust_tier_strt_dt", 'MM/dd/yyyy HH:mm:ss').cast("timestamp")) \
        .withColumn("ods_ld_dt", F.unix_timestamp("ods_ld_dt", 'MM/dd/yyyy HH:mm:ss').cast("timestamp")) \
        .withColumn("ods_lst_updt_dt", F.unix_timestamp("ods_lst_updt_dt", 'MM/dd/yyyy HH:mm:ss').cast("timestamp"))

    i_ads_cust_algnmnt_view_temp_df.printSchema()
    # i_ads_cust_algnmnt_view_temp_df.show()

    conn = glueContext.extract_jdbc_conf('conn-cmds-qa-rds-postgre-writer')
    # print("conn:",conn)
    url = conn['url']
    # print("url",url)
    USERNAME = conn['user']
    # print("username:",USERNAME)
    PASSWORD = conn['password']
    DATABASE = "cmdsqa"
    # print("database:",DATABASE)
    URL = url + "/" + DATABASE + "?stringtype=unspecified"
    # print(URL)
    DRIVER = "org.postgresql.Driver"

    i_ads_cust_algnmnt_view_temp_df.write \
        .format("jdbc") \
        .option("driver", DRIVER) \
        .option("url", URL) \
        .option("dbtable", table_i_ads_cust_algnmnt_tier_vw) \
        .option("user", USERNAME) \
        .option("password", PASSWORD) \
        .option("truncate", "true") \
        .option("ssl", "true") \
        .option("sslmode", "require") \
        .mode("overwrite") \
        .mode("overwrite") \
        .save()

    print('Load all the data into table {} done.'.format(table_i_ads_cust_algnmnt_tier_vw))

    # run lambda
    # Please attach inline policy(invoke lambda) to IAM role(AWSGlueServiceRoleDevDefault)
    import boto3

    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke( \
        FunctionName=Lambda_function_name, \
        LogType='None', \
        InvocationType='Event'
        # MaximumEventAgeInSeconds=60
    )
    print(response)
    print('Invoke lambda done.')

    job.commit()

except Exception as e:
    print(e)
    raise e
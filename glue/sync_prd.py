import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.functions import col



try:
    # Parameter which should be changed and checked before deployment
    glue_catalog_database_name = "cmds-glue-qa-catalog-database"

    ## @params: [JOB_NAME]
    #args = getResolvedOptions(sys.argv, ['JOB_NAME','bucket','parent_folder'])
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])

    #wrcBucketName = args['bucket']
    BucketName = "lly-cn-ibu-cmds-qa-private"
    print(BucketName)




    #CSV表结构的表名
    source_csv = "temp_20211027_m_hco_csv"
    #最终的数据库目标表名
    target_table = "cmd_owner.m_hco_bk_20211027"

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    ## @type: DataSource
    ## @args: [database = "temp_liye", table_name = "temp_20211027_m_hco_csv", transformation_ctx = "datasource0"]
    ## @return: datasource0
    ## @inputs: []
    datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "temp_liye", table_name = "temp_20211027_m_hco_csv", transformation_ctx = "datasource0")
    ## @type: ApplyMapping
    ## @args: [mapping = [("hco_id", "long", "hco_id", "int"), ("vn_entity_id", "long", "vn_entity_id", "string"), ("hco_name", "string", "hco_name", "string"), ("hco_desc", "string", "hco_desc", "string"), ("hco_type_name", "string", "hco_type_name", "string"), ("cnty_name", "string", "cnty_name", "string"), ("adrs_line_1", "string", "adrs_line_1", "string"), ("adrs_line_2", "string", "adrs_line_2", "string"), ("stts_ind", "long", "stts_ind", "int"), ("sub_clsfctn_name", "string", "sub_clsfctn_name", "string"), ("clsfctn_name", "string", "clsfctn_name", "string"), ("cnty_cd", "long", "cnty_cd", "string"), ("cnt_lcnsd_asst_dctr", "string", "cnt_lcnsd_asst_dctr", "int"), ("createduser", "string", "createduser", "string"), ("createddate", "string", "createddate", "timestamp"), ("modifieduser", "string", "modifieduser", "string"), ("modifieddate", "string", "modifieddate", "timestamp"), ("isdeleted", "boolean", "isdeleted", "boolean"), ("versionnumber", "string", "versionnumber", "int"), ("hco_stts_name", "string", "hco_stts_name", "string"), ("merged_to", "long", "merged_to", "string"), ("city_name", "string", "city_name", "string"), ("frmt_adrs", "string", "frmt_adrs", "string"), ("rcrd_state_name", "string", "rcrd_state_name", "string"), ("hco_englsh_name", "string", "hco_englsh_name", "string"), ("hco_cd", "string", "hco_cd", "string"), ("hco_type_cd", "string", "hco_type_cd", "string"), ("cnt_bed", "string", "cnt_bed", "int"), ("city_cd", "long", "city_cd", "string"), ("phone_1", "string", "phone_1", "string"), ("phone_2", "string", "phone_2", "string"), ("url", "string", "url", "string"), ("pstl", "long", "pstl", "string"), ("hco_stts_cd", "string", "hco_stts_cd", "string"), ("merged_date", "string", "merged_date", "timestamp"), ("star_hco_id", "string", "star_hco_id", "int"), ("adrs_status", "string", "adrs_status", "string"), ("sub_clsfctn_cd", "string", "sub_clsfctn_cd", "string"), ("clsfctn_cd", "string", "clsfctn_cd", "string"), ("parent_hco_v_id", "string", "parent_hco_v_id", "string"), ("rcrd_state_cd", "string", "rcrd_state_cd", "string"), ("prvnc_cd", "string", "prvnc_cd", "string"), ("prvnc_name", "string", "prvnc_name", "string")], transformation_ctx = "applymapping1"]
    ## @return: applymapping1
    ## @inputs: [frame = datasource0]
    applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("hco_id", "long", "hco_id", "int"), ("vn_entity_id", "long", "vn_entity_id", "string"), ("hco_name", "string", "hco_name", "string"), ("hco_desc", "string", "hco_desc", "string"), ("hco_type_name", "string", "hco_type_name", "string"), ("cnty_name", "string", "cnty_name", "string"), ("adrs_line_1", "string", "adrs_line_1", "string"), ("adrs_line_2", "string", "adrs_line_2", "string"), ("stts_ind", "long", "stts_ind", "int"), ("sub_clsfctn_name", "string", "sub_clsfctn_name", "string"), ("clsfctn_name", "string", "clsfctn_name", "string"), ("cnty_cd", "long", "cnty_cd", "string"), ("cnt_lcnsd_asst_dctr", "string", "cnt_lcnsd_asst_dctr", "int"), ("createduser", "string", "createduser", "string"), ("createddate", "timestamp", "createddate", "timestamp"), ("modifieduser", "string", "modifieduser", "string"), ("modifieddate", "timestamp", "modifieddate", "timestamp"), ("isdeleted", "boolean", "isdeleted", "boolean"), ("versionnumber", "string", "versionnumber", "int"), ("hco_stts_name", "string", "hco_stts_name", "string"), ("merged_to", "long", "merged_to", "string"), ("city_name", "string", "city_name", "string"), ("frmt_adrs", "string", "frmt_adrs", "string"), ("rcrd_state_name", "string", "rcrd_state_name", "string"), ("hco_englsh_name", "string", "hco_englsh_name", "string"), ("hco_cd", "string", "hco_cd", "string"), ("hco_type_cd", "string", "hco_type_cd", "string"), ("cnt_bed", "string", "cnt_bed", "int"), ("city_cd", "long", "city_cd", "string"), ("phone_1", "string", "phone_1", "string"), ("phone_2", "string", "phone_2", "string"), ("url", "string", "url", "string"), ("pstl", "long", "pstl", "string"), ("hco_stts_cd", "string", "hco_stts_cd", "string"), ("merged_date", "timestamp", "merged_date", "timestamp"), ("star_hco_id", "string", "star_hco_id", "int"), ("adrs_status", "string", "adrs_status", "string"), ("sub_clsfctn_cd", "string", "sub_clsfctn_cd", "string"), ("clsfctn_cd", "string", "clsfctn_cd", "string"), ("parent_hco_v_id", "string", "parent_hco_v_id", "string"), ("rcrd_state_cd", "string", "rcrd_state_cd", "string"), ("prvnc_cd", "string", "prvnc_cd", "string"), ("prvnc_name", "string", "prvnc_name", "string")], transformation_ctx = "applymapping1")
    ## @type: SelectFields
    ## @args: [paths = ["hco_stts_name", "parent_hco_v_id", "createddate", "sub_clsfctn_name", "cnty_name", "modifieddate", "cnty_cd", "hco_desc", "hco_name", "adrs_line_2", "isdeleted", "adrs_line_1", "hco_type_name", "pstl", "sub_clsfctn_cd", "prvnc_name", "clsfctn_cd", "modifieduser", "city_name", "clsfctn_name", "prvnc_cd", "rcrd_state_name", "frmt_adrs", "adrs_status", "phone_2", "phone_1", "rcrd_state_cd", "city_cd", "cnt_bed", "createduser", "hco_type_cd", "versionnumber", "stts_ind", "cnt_lcnsd_asst_dctr", "merged_date", "url", "merged_to", "hco_cd", "vn_entity_id", "star_hco_id", "hco_stts_cd", "hco_id", "hco_englsh_name"], transformation_ctx = "selectfields2"]
    ## @return: selectfields2
    ## @inputs: [frame = applymapping1]
    selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["hco_stts_name", "parent_hco_v_id", "createddate", "sub_clsfctn_name", "cnty_name", "modifieddate", "cnty_cd", "hco_desc", "hco_name", "adrs_line_2", "isdeleted", "adrs_line_1", "hco_type_name", "pstl", "sub_clsfctn_cd", "prvnc_name", "clsfctn_cd", "modifieduser", "city_name", "clsfctn_name", "prvnc_cd", "rcrd_state_name", "frmt_adrs", "adrs_status", "phone_2", "phone_1", "rcrd_state_cd", "city_cd", "cnt_bed", "createduser", "hco_type_cd", "versionnumber", "stts_ind", "cnt_lcnsd_asst_dctr", "merged_date", "url", "merged_to", "hco_cd", "vn_entity_id", "star_hco_id", "hco_stts_cd", "hco_id", "hco_englsh_name"], transformation_ctx = "selectfields2")
    ## @type: ResolveChoice
    ## @args: [choice = "MATCH_CATALOG", database = "temp_liye", table_name = "temp_20211027_cmdsqa_cmd_owner_m_hco_bk_20211027", transformation_ctx = "resolvechoice3"]
    ## @return: resolvechoice3
    ## @inputs: [frame = selectfields2]
    resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "temp_liye", table_name = "temp_20211027_cmdsqa_cmd_owner_m_hco_bk_20211027", transformation_ctx = "resolvechoice3")
    ## @type: ResolveChoice
    ## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
    ## @return: resolvechoice4
    ## @inputs: [frame = resolvechoice3]
    resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")


    #从此处结束替换成Glue job自动生成的脚本，即CSV每一列跟目标表每一列的对应关系
    print("----------m_hco_bk_20211027——datasource0 count: {}".format(datasource0.count()))
    print("----------m_hco_bk_20211027 count: {}".format(resolvechoice4.count()))

    #去空值
    Loading1= DropNullFields.apply(frame = resolvechoice4, transformation_ctx = "Loading1")
    m_hco_bk_20211027_df = Loading1.toDF()
    #print(m_hco_bk_20211027_df.show())
    m_hco_bk_20211027_df_01 = m_hco_bk_20211027_df.withColumn("createddate", F.unix_timestamp("createddate", 'MM/dd/yyyy HH:mm:ss').cast("timestamp"))
    m_hco_bk_20211027_df_02 = m_hco_bk_20211027_df_01.withColumn("modifieddate", F.unix_timestamp("modifieddate", 'MM/dd/yyyy HH:mm:ss').cast("timestamp"))
    m_hco_bk_20211027_df_03 = m_hco_bk_20211027_df_02.withColumn("merged_date", F.unix_timestamp("merged_date", 'MM/dd/yyyy HH:mm:ss').cast("timestamp"))





    conn = glueContext.extract_jdbc_conf('conn-cmds-qa-rds-postgre-writer')
    #print("----------conn:",conn)
    url = conn['url']
    #print("----------url",url)
    USERNAME = conn['user']
    #print("----------username:",USERNAME)
    PASSWORD = conn['password']
    DATABASE = "cmdsqa"
    #print("----------database:",DATABASE)
    URL = url+"/"+DATABASE
    #print(URL)
    DRIVER = "org.postgresql.Driver"

    m_hco_bk_20211027_df_03.write \
    .format("jdbc") \
    .option("driver",DRIVER) \
    .option("url", URL) \
    .option("dbtable", target_table) \
    .option("user", USERNAME) \
    .option("password", PASSWORD) \
    .option("truncate","true") \
    .option("ssl","true") \
    .mode("overwrite") \
    .save()

    print('--------------Load all the data into table {} done.'.format(target_table))

    job.commit()

except Exception as e:
    print(e)
    raise e
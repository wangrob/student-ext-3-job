import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node planned_grad_semester
planned_grad_semester_node1678601040833 = glueContext.create_dynamic_frame.from_catalog(
    database="db_raw_sisdemo",
    table_name="semester",
    transformation_ctx="planned_grad_semester_node1678601040833",
)

# Script generated for node admit_semester
admit_semester_node1678600551137 = glueContext.create_dynamic_frame.from_catalog(
    database="db_raw_sisdemo",
    table_name="semester",
    transformation_ctx="admit_semester_node1678600551137",
)

# Script generated for node department
department_node1678598196623 = glueContext.create_dynamic_frame.from_catalog(
    database="db_raw_sisdemo",
    table_name="department",
    transformation_ctx="department_node1678598196623",
)

# Script generated for node ed_level
ed_level_node1678601473176 = glueContext.create_dynamic_frame.from_catalog(
    database="db_raw_sisdemo",
    table_name="ed_level",
    transformation_ctx="ed_level_node1678601473176",
)

# Script generated for node student
student_node1678597661486 = glueContext.create_dynamic_frame.from_catalog(
    database="db_raw_sisdemo",
    table_name="student",
    transformation_ctx="student_node1678597661486",
)

# Script generated for node school
school_node1678600430723 = glueContext.create_dynamic_frame.from_catalog(
    database="db_raw_sisdemo",
    table_name="school",
    transformation_ctx="school_node1678600430723",
)

# Script generated for node RENAME_planned_grad_semester
RENAME_planned_grad_semester_node1678601126339 = ApplyMapping.apply(
    frame=planned_grad_semester_node1678601040833,
    mappings=[
        ("semester_id", "int", "planned_grad_semester_id", "int"),
        ("start_date", "date", "final_semester_start_date", "date"),
        ("end_date", "date", "final_semester_end_date", "date"),
        ("term_name", "string", "final_semester_term_name", "string"),
        ("semester_year", "short", "final_semester_semester_year", "short"),
        ("school_year_name", "string", "final_semester_school_year_name", "string"),
    ],
    transformation_ctx="RENAME_planned_grad_semester_node1678601126339",
)

# Script generated for node RENAME_admit_semester
RENAME_admit_semester_node1678600641187 = ApplyMapping.apply(
    frame=admit_semester_node1678600551137,
    mappings=[
        ("semester_id", "int", "admit_semester_id", "int"),
        ("start_date", "date", "first_semester_start_date", "date"),
        ("end_date", "date", "first_semester_end_date", "date"),
        ("term_name", "string", "first_semester_term_name", "string"),
        ("semester_year", "short", "first_semester_semester_year", "short"),
        ("school_year_name", "string", "first_semester_school_year_name", "string"),
    ],
    transformation_ctx="RENAME_admit_semester_node1678600641187",
)

# Script generated for node RENAME_ed_level
RENAME_ed_level_node1678601515914 = ApplyMapping.apply(
    frame=ed_level_node1678601473176,
    mappings=[
        ("ed_level_id", "int", "parent_ed_level_id", "int"),
        ("ed_level_code", "string", "parent_ed_level_code", "string"),
        ("ed_level_desc", "string", "parent_ed_level_desc", "string"),
    ],
    transformation_ctx="RENAME_ed_level_node1678601515914",
)

# Script generated for node RENAME_department_id
RENAME_department_id_node1678597819862 = ApplyMapping.apply(
    frame=student_node1678597661486,
    mappings=[
        ("student_id", "int", "student_id", "int"),
        ("first_name", "string", "first_name", "string"),
        ("last_name", "string", "last_name", "string"),
        ("gender", "string", "gender", "string"),
        ("birth_date", "date", "birth_date", "date"),
        ("email_address", "string", "email_address", "string"),
        ("admitted", "short", "admitted", "short"),
        ("enrolled", "short", "enrolled", "short"),
        ("parent_alum", "short", "parent_alum", "short"),
        ("parent_highest_ed", "short", "parent_highest_ed", "short"),
        ("first_gen_hed_student", "short", "first_gen_hed_student", "short"),
        ("high_school_gpa", "double", "high_school_gpa", "double"),
        ("was_hs_athlete_ind", "short", "was_hs_athlete_ind", "short"),
        ("home_state_name", "string", "home_state_name", "string"),
        ("admit_type", "string", "admit_type", "string"),
        ("private_hs_indicator", "short", "private_hs_indicator", "short"),
        ("multiple_majors_indicator", "short", "multiple_majors_indicator", "short"),
        ("secondary_class_percentile", "short", "secondary_class_percentile", "short"),
        ("department_id", "long", "fk_department_id", "long"),
        ("admit_semester_id", "long", "fk_admit_semester_id", "long"),
        ("first_year_gpa", "double", "first_year_gpa", "double"),
        ("cumulative_gpa", "double", "cumulative_gpa", "double"),
        ("enroll_status", "string", "enroll_status", "string"),
        ("planned_grad_semester_id", "long", "fk_planned_grad_semester_id", "long"),
    ],
    transformation_ctx="RENAME_department_id_node1678597819862",
)

# Script generated for node JOIN_department
JOIN_department_node1678600203048 = Join.apply(
    frame1=RENAME_department_id_node1678597819862,
    frame2=department_node1678598196623,
    keys1=["fk_department_id"],
    keys2=["department_id"],
    transformation_ctx="JOIN_department_node1678600203048",
)

# Script generated for node RENAME_school_id
RENAME_school_id_node1678600317561 = ApplyMapping.apply(
    frame=JOIN_department_node1678600203048,
    mappings=[
        ("student_id", "int", "student_id", "int"),
        ("first_name", "string", "first_name", "string"),
        ("last_name", "string", "last_name", "string"),
        ("gender", "string", "gender", "string"),
        ("birth_date", "date", "birth_date", "date"),
        ("email_address", "string", "email_address", "string"),
        ("admitted", "short", "admitted", "short"),
        ("enrolled", "short", "enrolled", "short"),
        ("parent_alum", "short", "parent_alum", "short"),
        ("parent_highest_ed", "short", "parent_highest_ed", "short"),
        ("first_gen_hed_student", "short", "first_gen_hed_student", "short"),
        ("high_school_gpa", "double", "high_school_gpa", "double"),
        ("was_hs_athlete_ind", "short", "was_hs_athlete_ind", "short"),
        ("home_state_name", "string", "home_state_name", "string"),
        ("admit_type", "string", "admit_type", "string"),
        ("private_hs_indicator", "short", "private_hs_indicator", "short"),
        ("multiple_majors_indicator", "short", "multiple_majors_indicator", "short"),
        ("secondary_class_percentile", "short", "secondary_class_percentile", "short"),
        ("fk_department_id", "long", "fk_department_id", "long"),
        ("fk_admit_semester_id", "long", "fk_admit_semester_id", "long"),
        ("first_year_gpa", "double", "first_year_gpa", "double"),
        ("cumulative_gpa", "double", "cumulative_gpa", "double"),
        ("enroll_status", "string", "enroll_status", "string"),
        ("fk_planned_grad_semester_id", "long", "fk_planned_grad_semester_id", "long"),
        ("department_id", "int", "department_id", "int"),
        ("department_name", "string", "department_name", "string"),
        ("department_code", "string", "department_code", "string"),
        ("school_id", "long", "fk_school_id", "long"),
    ],
    transformation_ctx="RENAME_school_id_node1678600317561",
)

# Script generated for node JOIN_school
JOIN_school_node1678600407024 = Join.apply(
    frame1=RENAME_school_id_node1678600317561,
    frame2=school_node1678600430723,
    keys1=["fk_school_id"],
    keys2=["school_id"],
    transformation_ctx="JOIN_school_node1678600407024",
)

# Script generated for node JOIN_RENAME_admit_semester
JOIN_RENAME_admit_semester_node1678600849263 = Join.apply(
    frame1=JOIN_school_node1678600407024,
    frame2=RENAME_admit_semester_node1678600641187,
    keys1=["fk_admit_semester_id"],
    keys2=["admit_semester_id"],
    transformation_ctx="JOIN_RENAME_admit_semester_node1678600849263",
)

# Script generated for node JOIN_RENAME_planned_grad_semester
JOIN_RENAME_planned_grad_semester_node1678601231318 = Join.apply(
    frame1=JOIN_RENAME_admit_semester_node1678600849263,
    frame2=RENAME_planned_grad_semester_node1678601126339,
    keys1=["fk_planned_grad_semester_id"],
    keys2=["planned_grad_semester_id"],
    transformation_ctx="JOIN_RENAME_planned_grad_semester_node1678601231318",
)

# Script generated for node JOIN_RENAME_ed_level
JOIN_RENAME_ed_level_node1678601663318 = Join.apply(
    frame1=JOIN_RENAME_planned_grad_semester_node1678601231318,
    frame2=RENAME_ed_level_node1678601515914,
    keys1=["parent_highest_ed"],
    keys2=["parent_ed_level_id"],
    transformation_ctx="JOIN_RENAME_ed_level_node1678601663318",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1678601836019 = glueContext.getSink(
    path="s3://dataedu-curated-0ab7cc53f435/sisdemo/student_ext_3/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["first_semester_school_year_name", "first_semester_term_name"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AWSGlueDataCatalog_node1678601836019",
)
AWSGlueDataCatalog_node1678601836019.setCatalogInfo(
    catalogDatabase="db_cur_sisdemo", catalogTableName="student_ext_3"
)
AWSGlueDataCatalog_node1678601836019.setFormat("glueparquet")
AWSGlueDataCatalog_node1678601836019.writeFrame(JOIN_RENAME_ed_level_node1678601663318)
job.commit()

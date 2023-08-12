#  Question :
#  QUESTION
# 	~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 		courses:-
# 		COURSE_ID,COURSE,FEE
# 		100,Physics,1500
# 		101,Geography,1000
# 		102,Math,2000
# 		103,Biology,1400
# 		104,Chemistry,1700
# 		105,English,800
# 		~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 		students:-
# 		ID,STUDENT_NAME,AGE,COURSES
# 		1,Alok,18,Geography|Biology
# 		2,Prakash,16,Math|English|Geography
# 		3,Anna,16,Biology
# 		3,ANNA,16,Biology
# 		1,Alok,18,Geography
# 		~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# 		## List down all of the courses available along with their student count,
# 		Course with max total-revenue(fees-collection) should be on the top followed by rest.
# 		If for 2 courses , total-revenue match then order the course on STUDENT_COUNT most on top. 

# 		Expected o/p pattern-1:-

# 		COURSE,STUDENT_COUNT,COURSE_REVENUE_TOTAL
# 		Biology,2,2800
# 		Geography,2,2000
# 		Math,1,2000
# 		English,1,800
# 		Chemistry,0,0
# 		Physics,0,0

# ~~~~~~~~~~~~~

import findspark
findspark.init()
from pyspark.sql import SparkSession

from pyspark.sql.functions import explode, split,col,lower

spark=SparkSession.builder.master("local").appName("CourseSplit").getOrCreate()

# Sample data
data = [
    (1, "Alok", 18, "Geography|Biology"),
    (2, "Prakash", 16, "Math|English|Geography"),
    (3, 'Anna', 16, 'Biology'),
    (3, 'ANNA', 16, 'Biology'),
    (1, 'Alok', 18, 'Geography')
]

# Create a DataFrame
columns = ["ID", "STUDENT_NAME", "AGE", "COURSES"]
stud = spark.createDataFrame(data, columns)

stud.show(truncate=False)

stud_exploded = stud.selectExpr("ID", "STUDENT_NAME", "AGE", "explode(split(COURSES, '\\\\|')) as COURSE")
stud_exploded.show(truncate=False)
stud_transformed = stud_exploded.withColumn("STUDENT_NAME", lower(col("STUDENT_NAME")))
stud_transformed.show(truncate=False)

stud_unique=stud_transformed.distinct().orderBy("ID")
stud_unique.show()


stud_unique.createOrReplaceTempView("student")
spark.sql("select * from student ").show()


course_data=[
        (100, 'Physics', 1500),
    (101, 'Geography', 1000),
    (102, 'Math', 2000),
    (103, 'Biology', 1400),
    (104, 'Chemistry', 1700),
    (105, 'English', 800)
    
]
course_columns=["COURSE_ID","COURSE","FEE"]

course_df=spark.createDataFrame(course_data,course_columns)


course_df.createOrReplaceTempView("course")
spark.sql("select * from student ").show()
spark.sql("select * from course").show()

 spark.sql("""
 
 select * from (
    SELECT c.COURSE,
    coalesce(count(s.ID),0) as STUDENT_COUNT ,
    case when count(s.ID)>0 then  sum(c.FEE) else 0 end as COURSE_REVENUE_TOTAL
    --sum(c.FEE)  as COURSE_REVENUE_TOTAL
    
   FROM student s
   right JOIN course c ON s.COURSE = c.COURSE
   group by c.COURSE
   ) order by COURSE_REVENUE_TOTAL desc ,STUDENT_COUNT desc
""").show()
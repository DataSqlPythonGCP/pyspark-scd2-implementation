#-----------------------------------------------------
# This script contains basic DQ check using pyspark
# DQ : Accuracy, Completeness, Consistency & Uniqueness
#------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, array_contains

spark = SparkSession.builder.appName("DataQualityFramework").getOrCreate()

dq_rules_df = spark.read.csv("/Users/ad/Downloads/DQRules.csv",header=True,inferSchema=True)
print("\n The rule matrix is as follows.\n")
dq_rules_df.show(5, truncate=False)

dq_sample_df = spark.read.csv("/Users/ad/Downloads/dq_sample_dataset.csv",header=True,inferSchema=True)
print("\n Sample customer data\n.")
dq_sample_df.show(5, truncate=False)

class DQFramework:
    def __init__(self, df, rules_df):
        self.df = df
        self.rules_df = rules_df
        self.results = []

    def run_check(self):
        for rule in self.rules_df.collect():
            rule_type = rule["rule_type"]

            if rule_type == 'completeness':
                self.check_completeness(rule)
            elif rule_type == 'accuracy':
                self.check_accuracy(rule)
            elif rule_type == 'consistency':
                self.check_consistency(rule)
            elif rule_type == 'uniqueness':
                self.check_uniqueness(rule)

        return self.results

    def check_completeness(self, rule):
        col_name = rule["column_name"]
        failed_df = self.df.filter(col(col_name).isNull())
        self.results.append({
            "rule_id" : rule["rule_id"],
            "column_name" : col_name,
            "rule_type" : rule["rule_type"],
            "failed_count" : failed_df.count(),
            "comments" : "NULL values present in column."
        })

    def check_accuracy(self, rule):
        col_name = rule["column_name"]
        value = rule['rule_value']
        if value.startswith('RANGE'):
            #min_val, max_val = [int(x) for x in value.split(':')[1:]]
            val = value.split(':') # Values split into [Range : 0 : 120] then we pick [0:120]
            min_val = int(val[1])
            max_val = int(val[2])
            failed_df = self.df.filter((col(col_name) < min_val) | (col(col_name) > max_val))
            comment = 'Age not within range.'

        elif value.startswith('REGEX'):
            reg_pattern = value.split(':')[1]
            failed_df = self.df.filter(col(col_name).rlike(reg_pattern))
            comment = 'Invalid email.'


        self.results.append({
            "rule_id" : rule["rule_id"],
            "column_name" : col_name,
            "rule_type" : rule["rule_type"],
            "failed_count" : failed_df.count(),
            "comments" : comment
        })
    def check_consistency(self, rule):
        cols = rule["column_name"].split(',')
        rule_value = rule['rule_value'].removeprefix("MAP:").strip()

        mapping = {}
        for pair in rule_value.split(';'):
            k,v = pair.split('=')
            mapping[k] = v.split(',')

        valid_condition = False

        for country, states in mapping.items():
            for state in states:
                condition = (col(cols[0])==country) & (col(cols[1])==state)
                valid_condition = valid_condition | condition

        failed_df = self.df.filter(~valid_condition)
        self.results.append({
            "rule_id": rule['rule_id'],
            "column_name" : cols[0],
            "rule_type" : rule['rule_type'],
            "failed_count": failed_df.count(),
            "comments" : "Country, State mismatch."
        })


    def check_uniqueness(self, rule):
        col_name = rule["column_name"]
        count_df = self.df.groupby(col_name).count().filter(col(col_name) > 1)
        failed_df = count_df.count()
        self.results.append({
            "rule_id" : rule["rule_id"],
            "column_name" : col_name,
            "rule_type" : rule["rule_type"],
            "failed_count" : failed_df,
            "comments" : "NULL values present in column."
        })




dqf = DQFramework(dq_sample_df, dq_rules_df)
check_results = spark.createDataFrame(dqf.run_check())
print("\n Below are the DQ metrics on the customer table.\n")
check_results.show(5, truncate=False)




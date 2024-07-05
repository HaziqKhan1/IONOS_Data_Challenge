import sqlite3
import pandas as pd

# Connect to SQLite database
conn = sqlite3.connect('titanic.db')
cursor = conn.cursor()

# Define a function to execute a SQL query and return a DataFrame
def query_to_df(query):
    return pd.read_sql_query(query, conn)

# 1. Survival Analysis by Gender
gender_query = """
SELECT Sex, ROUND(AVG(Survived),2) AS SurvivalRate
FROM titanic
GROUP BY Sex;
"""
gender_survival = query_to_df(gender_query)
print("Survival Rate by Gender:")
print(gender_survival)



# 2. Survival Analysis by Passenger Class
class_query = """
SELECT Pclass, ROUND(AVG(Survived),2) AS SurvivalRate
FROM titanic
GROUP BY Pclass;
"""
class_survival = query_to_df(class_query)
print("\nSurvival Rate by Passenger Class:")
print(class_survival)

# 3. Survival Analysis by Age Group
age_group_query = """
SELECT 
    CASE 
        WHEN Age < 12 THEN 'Child'
        WHEN Age >= 12 AND Age < 18 THEN 'Teenager'
        WHEN Age >= 18 AND Age < 35 THEN 'Adult'
        WHEN Age >= 35 AND Age < 60 THEN 'Middle Aged'
        ELSE 'Senior'
    END AS AgeGroup,
    ROUND(AVG(Survived),2) AS SurvivalRate
FROM titanic
GROUP BY AgeGroup;
"""
age_group_survival = query_to_df(age_group_query)
print("\nSurvival Rate by Age Group:")
print(age_group_survival)

# Close the connection
conn.close()
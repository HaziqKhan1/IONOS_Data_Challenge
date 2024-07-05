import pandas as pd
import sqlite3
import re
from sklearn.preprocessing import MinMaxScaler
import logging
import schedule
import time

# Setup logging
logging.basicConfig(filename='etl_log.log', level=logging.INFO,
format='%(asctime)s:%(levelname)s:%(message)s')

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess and clean the data.""" 
    # Create a new column 'Age_without_nan' by copying the 'Age' column
    df['Age_without_nan'] = df['Age']
    # Calculate the mean of the 'Age' column
    mean_age = df['Age'].mean().round(2)
    # Fill the NaN values in 'Age_without_nan' with the rounded mean
    df['Age_without_nan'] = df['Age_without_nan'].fillna(mean_age)
    df['Embarked'] = df['Embarked'].fillna(df['Embarked'].mode()[0])
    df['Cabin'] = df['Cabin'].fillna(df['Cabin'].mode()[0])
    logging.info("Data preprocessing completed")
    return df

def extract_title(name: str) -> str:
    """Extract the title from a name string, prioritizing certain titles."""
    title_search = re.search(r'\b(Mrs|Mr|Miss|Master|Dr|Rev|Don|Mlle|Col|Capt|Countess|Jonkheer|Mme|Ms|Major)\b', name)
    if title_search:
        return title_search.group(0)
    return None

def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """Perform feature engineering on the data."""
    df['Title'] = df['Name'].apply(extract_title)
    logging.info("Feature engineering completed")
    """Normalize numerical columns and create new columns."""
    scaler = MinMaxScaler()
    # For Age_norm the column of age without Nan is used
    df['Age_norm'] = scaler.fit_transform(df[['Age_without_nan']]).round(2) 
    df['Fare_norm'] = scaler.fit_transform(df[['Fare']]).round(2)
    df['SibSp_norm'] = scaler.fit_transform(df[['SibSp']]).round(2)
    df['Parch_norm'] = scaler.fit_transform(df[['Parch']]).round(2)
    logging.info("Data normalization completed")
    return df



def sync_data(df: pd.DataFrame, db_name: str) -> None:
    """Sync the transformed data into a SQLite database."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Check if the 'titanic' table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='titanic';"
                   )
    table_exists = cursor.fetchone()
    
    if table_exists:
        # Read existing data
        existing_df = pd.read_sql('SELECT * FROM titanic', conn)
        
        # Ensure columns match
        existing_df = existing_df[df.columns]
        
        # Find new records
        new_df = df[~df['PassengerId'].isin(existing_df['PassengerId'])]
        
        # Find updated records
        merged_df = df.merge(existing_df, on='PassengerId', suffixes=('', '_old'), how='left', indicator=True)
        updated_df = merged_df[merged_df['_merge'] == 'both']
        
        for col in df.columns:
            if col != 'PassengerId':
                updated_df = updated_df[updated_df[col] != updated_df[f'{col}_old']]
        
        updated_df = updated_df[df.columns]
        
        # Find records to delete
        deleted_df = existing_df[~existing_df['PassengerId'].isin(df['PassengerId'])]

        # Sync data
        if not deleted_df.empty:
            cursor.executemany("DELETE FROM titanic WHERE PassengerId=?", [(i,) for i in deleted_df['PassengerId']])
            logging.info(f"Deleted {len(deleted_df)} records")

        if not updated_df.empty:
            for index, row in updated_df.iterrows():
                cursor.execute("""
                    UPDATE titanic
                    SET Survived=?, Pclass=?, Name=?, Sex=?, Age=?, SibSp=?, Parch=?, Ticket=?, Fare=?, Cabin=?, Embarked=?, Title=?
                    WHERE PassengerId=?
                """, (row['Survived'], row['Pclass'], row['Name'], row['Sex'], row['Age'], row['SibSp'], row['Parch'], row['Ticket'], row['Fare'], row['Cabin'], row['Embarked'], row['Title'], row['PassengerId']))
            logging.info(f"Updated {len(updated_df)} records")

        if not new_df.empty:
            new_df.to_sql('titanic', conn, if_exists='append', index=False)
            logging.info(f"Appended {len(new_df)} new records")
    else:
        # Table does not exist, create it and load all data
        df.to_sql('titanic', conn, if_exists='replace', index=False)
        logging.info("Database and table created, and all data loaded")

    conn.commit()
    conn.close()
    
    logging.info("Data syncing completed")

def main() -> None:
    """Main function to run the ETL pipeline."""
    url = 'https://github.com/datasciencedojo/datasets/raw/master/titanic.csv'
    try:
        df = pd.read_csv(url)
        
        # Uncomment the block below to test the process incase of updates
        ############### TEST #################
        """ 
        # Initialize an empty list to collect data
        data_to_append = []
        # Add a new row with PassengerId 895
        new_data = {
            'PassengerId': 895,
            'Survived': 1,
            'Pclass': 3,
            'Name': 'Mr.New Passenger',
            'Sex': 'male',
            'Age':  30.0,
            'SibSp': 0,
            'Parch': 0,
            'Ticket': 'PC 12345',
            'Fare': 50.0,
            'Cabin': 'C123',
            'Embarked': 'S'
        }
        # Append new_data to the list
        data_to_append.append(new_data)
        # Convert the list of dictionaries to a DataFrame
        new_df = pd.DataFrame(data_to_append)
        # Concatenate new_df with the original DataFrame df
        df = pd.concat([df, new_df], ignore_index=True)
          """
        ############### TEST #######################
        
        df = preprocess_data(df)
        df = feature_engineering(df)
        sync_data(df, 'titanic.db')
        logging.info("ETL pipeline completed successfully!")
    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")

# Schedule the ETL process to run daily at 1:00 AM. 
schedule.every().day.at("01:00").do(main)

if __name__ == "__main__":
    logging.info("Scheduler started...")
    while True:
        schedule.run_pending()
        time.sleep(10)  # Check every ten seconds

import pandas as pd
from google.cloud import bigquery
table_id = "your-project-id.dataset_name.table_name"

def extract(file_path):
    df = pd.read_csv(file_path)
    return df

def transform(df):
    # Example cleaning
    df = df.dropna()  # remove nulls
    
    # Convert data types
    df['date'] = pd.to_datetime(df['date'])
    
    # Example feature
    df['year'] = df['date'].dt.year
    
    return df

def load(df, table_id):
    client = bigquery.Client()

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"  # overwrite table
        )
    )

    job.result()
    print("Loaded to BigQuery")
    
def run_pipeline():
    df = extract("marketing_sales_dataset.csv")
    df = transform(df)
    df.to_csv("output.csv", index=False)
    load(df, "your-project-id.dataset.table")

if __name__ == "__main__":
    run_pipeline()
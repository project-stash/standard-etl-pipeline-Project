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
    
    # Drop rows where SPECIFIC column is null ex
    df.dropna(subset=["column_name"], inplace=True)

    # Fill nulls with a value ex
    df["column"].fillna(0, inplace=True)
    df["column"].fillna("Unknown", inplace=True)

    # Fill with mean / median / mode
    df["age"].fillna(df["age"].mean(), inplace=True)
    df["age"].fillna(df["age"].median(), inplace=True)
    df["city"].fillna(df["city"].mode()[0], inplace=True)
    # Example feature
    df['year'] = df['date'].dt.year
    
    # Check duplicates
    df.duplicated().sum()

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Remove duplicates based on specific columns
    df.drop_duplicates(subset=["email", "name"], inplace=True)
    
    # See distribution /Outlier ex
    df["salary"].describe()

    # Remove outliers using IQR
    Q1 = df["salary"].quantile(0.25)
    Q3 = df["salary"].quantile(0.75)
    IQR = Q3 - Q1

    df = df[(df["salary"] >= Q1 - 1.5 * IQR) & 
        (df["salary"] <= Q3 + 1.5 * IQR)]
    # Keep only valid ages
    df = df[df["age"] > 0]
    df = df[df["age"] < 120]

    # Keep rows matching a condition
    df = df[df["status"].isin(["active", "inactive"])]

    # Drop rows with invalid emails (basic check)
    df = df[df["email"].str.contains("@", na=False)]
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
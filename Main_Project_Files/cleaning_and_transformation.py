import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine,URL
import logging
from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.parquet as pq
import pandas.api.types as ptypes
from concurrent.futures import ProcessPoolExecutor, as_completed   # parallel fetching


load_dotenv()

logging.basicConfig(
    filename="../secondary_resourses/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def auto_impute(df):

    for col in df.columns:

        if ptypes.is_numeric_dtype(df[col]):
            df[col] = df[col].astype('float32')
            # automatically downcast to the smallest possible numeric dtype
            # example: int64 -> int32 / int16
            # example: float64 -> float32 no () like to_datetime() or .to_category() use.as_type()
            df[col] = pd.to_numeric(df[col], downcast='float')

            if   df[col].isna().any():
                df[col] = df[col].fillna(df[col].mean())

        elif ptypes.is_string_dtype(df[col])  :
            df[col] = df[col].astype('category')
            if df[col].isna().any():
                # check if column contains at least one missing value
                # .any() returns True if any NaN exists in the column
                df[col] = df[col].fillna('Unknown')


        elif ptypes.is_datetime64_any_dtype(df[col])  :
            df[col] = df[col].astype('datetime64[s]')
            if df[col].isna().any():
                df[col] = df[col].ffill().bfill()


    return df


def basic_cleaning(df,table_name,i):

    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    df = df.fillna(np.nan)

    logging.info(f'successfully done basic cleaning of group {i} of table {table_name}')

    return df


def create_connection(db,query):

    writer=None

    if db=='ecommerce_db':

        connection_url = URL.create(
            "mysql+pymysql",
            username=os.getenv("db_user"),
            password=os.getenv("db_pass"),
            # no manual encoding needed
            host=os.getenv("db_host"),
            port=3306,
            database=db
        )

    else:

        print('customersdb')

        connection_url = URL.create(
            "mysql+pymysql",
            username=os.getenv("CLOUD_DB_USER"),
            password=os.getenv("CLOUD_DB_PASS"),
            # no manual encoding needed
            host=os.getenv("CLOUD_DB_HOST"),
            port=4000,
            database='customers_db'
        )

    conn=create_engine(
        connection_url,
        pool_pre_ping=True,
        connect_args={"ssl":{"ssl_mode":"VERIFY_IDENTITY"}}
    )

    table_name=query.lower().split("from")[1].strip().split()[0]
    os.makedirs('../Fold', exist_ok=True)
    file_name=f"FOLD/{db}_{table_name}.parquet"

    try:

        print('\n-----------------------------------------------------------------------------------------\n')

        for i,chunk in enumerate(pd.read_sql(query,conn,chunksize=500000),start=1):

            chunk=basic_cleaning(chunk,table_name,i)

            table=pa.Table.from_pandas(chunk, preserve_index=False) # preserve_index=False avoids storing pandas index #smaller parquet files # faster read/write


            if writer is None:

                writer=pq.ParquetWriter(
                    file_name,
                    table.schema,
                    compression="snappy"
                )

            writer.write_table(table)

            print(f'data fetched from {db} group-{i} from {table_name} into {file_name} : {len(chunk)}')

        logging.info(f" connected with database {db} ")

    except Exception as error:

        print("ERROR:",error)

        logging.error(f" error connecting to database {db} - {error}")

    finally:

        if writer:
            writer.close()

        try:
            conn.close()
        except:
            pass

        logging.info(f" records fetched successfully from database {db} ")



# -------------------------------------------------------
# PARALLEL DATA FETCHING
# -------------------------------------------------------
# Instead of fetching tables one-by-one (sequential execution),
# we execute multiple database queries simultaneously using CPU cores.

def fetch_data(dbs):

    tasks=[]

    # Create task list containing (database, query) pairs
    for db in dbs.keys():

        for q in dbs[db]:

            tasks.append((db,q))


    # Determine number of parallel workers
    # Limiting to 8 even if CPU has more cores to avoid:
    # - DB overload
    # - disk IO bottlenecks
    # - excessive memory usage
    workers=min(8,len(tasks))


    # ProcessPoolExecutor launches multiple Python processes
    # Each process runs create_connection() independently
    with ProcessPoolExecutor(max_workers=workers) as executor:

        # Submit tasks to worker processes
        futures=[executor.submit(create_connection,db,q) for db,q in tasks]


        # Monitor task completion
        for f in as_completed(futures):

            try:
                f.result()
            except Exception as e:
                logging.error(f"parallel fetch error: {e}")





#
# def fetch_data(dbs):
#
#     for db in dbs.keys():
#
#         for q in dbs[db]:
#
#             create_connection(db,q)


def pre_fetch():

    logging.info('ready for pre fetch process')

    dbs={
        'customers_db':['select * from customers'],
        'ecommerce_db':[
            'select * from orders',
            'select * from order_items',
            'select * from products',
            'select * from payments'
        ]
    }

    fetch_data(dbs)


def EDA(dct):

    for df in dct.keys():

        print('\n-----------------------------------------------------------------------------------------------\n')

        print(f'{df} EDA')

        print(f"\nShape of {df} :",dct[df].shape)

        print(f"\nsize of {df} :",dct[df].size)

        print(f"\ntotal nulls in {df} :\n",dct[df].isnull().sum())

        print(f"\ninfo of {df} :\n")

        dct[df].info()

        print(f"\nstatistical values of {df} :\n",dct[df].describe(include='all'))

        print(f"\nno. of duplicate values in {df} :",dct[df].duplicated().sum())

        print("\nUnique values per column:\n",dct[df].nunique())

        print("\nMemory Usage (MB):",dct[df].memory_usage(deep=True).sum()/1024**2)

        print("\nColumn Types:\n",dct[df].dtypes.value_counts())

        print('\nPotential primary key')

        FLG=1

        for cols in dct[df].columns:

            if dct[df][cols].nunique()==len(dct[df]):

                print(cols)

                FLG=0

        if FLG:
            print('NONE')


def customer_cleaning_and_transformation(df):

    print(f"Rows before cleaning: {len(df)}")

    df.dropna(subset=['customer_id'],inplace=True)

    df.drop_duplicates(subset=['customer_id','email'],inplace=True)

    # ⚠️ String operations on category columns are slower / sometimes problematic.
    # ✅ Clean first → then convert to category
    df=df[df['email'].str.contains('@',na=False)]

    # df=df[df['name'].str.len()>0]

    df['name']=df['name'].str.strip()
    df['name']=df['name'].fillna('anonymous')

    df['email']=df['email'].str.strip().str.lower()

    df['city']=df['city'].str.strip().str.title()

    df['country']=df['country'].str.strip().str.title()

    df=df[df['email'].str.contains(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$',na=False)]

    # and data cleaning always before transformation or set index or dtype change
    # and email validation before indexing always
    df['signup_date']=pd.to_datetime(df['signup_date'],errors='coerce')

    df=auto_impute(df)

    # order matters first type change then set index
    df['customer_id']=df['customer_id'].astype('int32')

    assert df['customer_id'].is_unique # ensure customer_id is unique (primary key validation)

    # multi column indexing
    df.set_index(['customer_id','signup_date'],inplace=True)

    df.sort_index(inplace=True)

    print(df.memory_usage(deep=True))

    df.info()
    print(df.head(10))

    print(f"Rows after cleaning: {len(df)}")


def order_items_cleaning_and_transformation(df):

    print(f"Rows before cleaning: {len(df)}")

    print(df.info())

    print(df.head(100))

    df.dropna(subset=['order_item_id'],inplace=True)

    df.drop_duplicates(subset=['order_item_id'],inplace=True)

    df.reset_index(drop=True,inplace=True)

    df.sort_values('order_item_id',inplace=True)

    df=auto_impute(df)

    assert df['order_item_id'].is_unique # ensure order_item_id is unique (primary key validation)

    q1=df['quantity'].quantile(0.25)
    q3=df['quantity'].quantile(0.75)

    iqr=q3-q1

    upper=q3+1.5*iqr
    lower=q1-1.5*iqr

    df['quantity']=df['quantity'].clip(lower,upper=upper)

    print(df.head(100))


def orders_cleaning_and_transformation(df):

    print(f"rows before cleaning and transformation:{len(df)}")

    df.info()

    print(df.head(100))

    df.sort_values('order_id',inplace=True)

    df.dropna(subset=['order_id'],inplace=True)

    df.reset_index(drop=True,inplace=True)

    df.drop_duplicates('order_id',inplace=True)

    assert df['order_id'].is_unique # before set_index ensure order_id is unique (primary key validation)
    # if true continues normally else raises assertion error

    # replace exact text regex=False
    # replace pattern (multiple characters) regex=True
    df['order_status']=df['order_status'].str.replace('\r','',regex=False).str.strip().str.lower()

    df['order_status']=df['order_status'].fillna('pending')

    df.loc[df['order_status'].str.strip()=='','order_status']='pending'

    df=df[df['order_status'].isin(['cancelled','completed','returned','pending','return'])]

    df=auto_impute(df)

    df['order_date']=pd.to_datetime(df['order_date'],errors='coerce')

    df['order_year']=df['order_date'].dt.year
    df['order_month']=df['order_date'].dt.month # must be before setting index

    # some values may become NaT.
    df.dropna(subset=['order_date'],inplace=True)

    df.set_index(['order_id','order_date'],drop=True,inplace=True)

    print(df.head(100))

    print(f"rows after cleaning and transformation:{len(df)}")


def payments_cleaning_and_transformation(df):

    print(f"rows before cleaning and transformation:{len(df)}")

    df['payment_method']=df['payment_method'].str.title()

    df.drop_duplicates(subset=['payment_id'],inplace=True)

    df.dropna(subset=['payment_id'],inplace=True)

    assert df['payment_id'].is_unique

    print(df['payment_method'].unique())

    # FIXED: return boolean directly instead of 'yes'/'no'
    df['is_digital']=np.where(
        (df['payment_method']=='Upi')|(df['payment_method']=='Net Banking'),
        True,False
    )

    df['payment_status']=df['payment_status'].str.strip().str.replace('\r','',regex=False)

    df.dropna(subset=['order_id'],inplace=True)

    df['payment_status']=df['payment_status'].fillna('pending')

    df['amount']=df['amount'].fillna(df['amount'].mean())

    df.loc[df['payment_status'].str.strip()=='','payment_status']='pending'

    print(df.head(10))

    print(df.isnull().sum()) # or print(df.isna().sum())

    df=auto_impute(df)

    # outlier detection and handling
    q1=df['amount'].quantile(0.25)
    q3=df['amount'].quantile(0.75)

    iqr=q3-q1

    upper=q3+1.5*iqr
    lower=q1-1.5*iqr

    outlier=df[(df['amount']<lower)|(df['amount']>upper)]

    df2=df.query("amount<@lower or amount>@upper")

    print(df2.head(100))

    if len(outlier):
        df['amount']=np.clip(df['amount'],lower,upper)

    df['is_digital']=df['is_digital'].astype('bool')

    df.rename(columns={'is_digital':'is_digi'},inplace=True)

    df.reset_index(drop=True,inplace=True)

    print(f"rows after cleaning and transformation:{len(df)}")


def products_cleaning_and_transformation(df):

    print(f"rows before cleaning and transformation:{len(df)}")

    df.info()

    print(df.head(10))

    df.dropna(subset=['product_id'],inplace=True)

    df.drop_duplicates(subset=['product_id','product_name'],inplace=True)

    print(df['category'].unique()) # list all the unique values form the column

    df['category']=df['category'].str.strip().str.title()

    df['category']=df['category'].replace({'Books':'Book','book':'Book','electronics':'Electronics'})

    print(df['category'].unique()) # list all the unique values form the column

    print(f"rows after cleaning and transformation:{len(df)}")

    df['product_name']=df['product_name'].str.strip().str.title()

    df.loc[df['product_name']=='','product_name']='Unknown'

    df=auto_impute(df)

    df['total_price']=df.eval('price*stock_quantity')

    df.reset_index(drop=True,inplace=True)

    df.set_index('product_id',inplace=True,drop=True)

    print(df.head(10))

    print(f"rows after cleaning and transformation:{len(df)}")


def main():

    pre_fetch()

    print(os.path.exists('../customers_db_customers.parquet'))

    print("Current Working Directory:",os.getcwd())

    customer_cleaning_and_transformation(
        pd.read_parquet('../customers_db_customers.parquet')
    )

    logging.info('successfully done cleaning and transformation of table customers_db_customers')

    order_items_cleaning_and_transformation(
        pd.read_parquet('../ecommerce_db_order_items.parquet')
    )

    logging.info('successfully done cleaning and transformation of table ecommerce_db_order_items')

    orders_cleaning_and_transformation(
        pd.read_parquet('../ecommerce_db_orders.parquet')
    )

    logging.info('successfully done cleaning and transformation of table ecommerce_db_orders')

    payments_cleaning_and_transformation(
        pd.read_parquet('../ecommerce_db_payments.parquet')
    )

    products_cleaning_and_transformation(
        pd.read_parquet('../ecommerce_db_products.parquet')
    )


if __name__=="__main__":

    print("Script Started") # 👈 debug line

    main()
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib.parse

df1 = pd.read_csv('data_sets/Sales.csv', encoding='latin1')
df2 = pd.read_csv('data_sets/Customers.csv', na_values='', keep_default_na=False, encoding='latin1')
df3 = pd.read_csv('data_sets/Stores.csv')
df4 = pd.read_csv('data_sets/Products.csv')
df5 = pd.read_csv('data_sets/Exchange_Rates.csv')

encoded_password = urllib.parse.quote_plus("Push@1612")
connection = f"mysql+pymysql://root:{encoded_password}@localhost:3306/global_electronics"

engine = create_engine(connection)


def handle_missing_values(df):
    # Handling empty numeric values with mean()
    if 'Square Meters' in df.columns:
        df['Square Meters'] = df['Square Meters'].fillna(0)
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].mean())

    for column in df.columns:
        if 'Order Date' in df.columns and 'Delivery Date' in df.columns:
            # Update 'Delivery Date' to 'Order Date' where 'Delivery Date' is empty (NaN)
            df.loc[df['Delivery Date'].isna(), 'Delivery Date'] = df['Order Date']
        # Converting datetime for date columns
        if 'date' in column.lower() or 'day' in column.lower():
            df[column] = pd.to_datetime(df[column], errors='coerce')
        # Calculating and Adding age column if birthday is present in column
        if 'birthday' in column.lower():
            today = datetime.now()
            df['Age'] = today.year - df['Birthday'].dt.year - ((today.month < df['Birthday'].dt.month) |
                                                               ((today.month == df['Birthday'].dt.month) &
                                                                (today.day < df['Birthday'].dt.day)))

    # Removing $ in price columns and Changing price values to float
    price_columns_to_clean = ['Unit Cost USD', 'Unit Price USD']
    for column in price_columns_to_clean:
        if column in df.columns:
            df[column] = df[column].str.replace(r'[^\d.-]', '', regex=True)
            df[column] = df[column].apply(pd.to_numeric, downcast='float')

    # Handling empty categorical values with mode()
    categorical_columns = df.select_dtypes(include=['object']).columns
    df[categorical_columns] = df[categorical_columns].fillna(df[categorical_columns].mode().iloc[0])
    return df


def get_sql_type(pandas_dtype, column_name=None):
    """
    Convert pandas dtype to SQL type with consistent types for key columns
    """
    dtype_str = str(pandas_dtype)
    if 'int' in dtype_str:
        return 'BIGINT'
    elif column_name == 'Exchange':
        return 'DOUBLE'
    elif 'float' in dtype_str:
        return 'DECIMAL(10, 2)'
    elif 'datetime' in dtype_str:
        return 'DATETIME'
    else:
        return 'VARCHAR(255)'


def sanitize_column_name(column_name):
    """
    Sanitize column names for SQL use
    """
    return column_name.replace(" ", "").replace("-", "_")


def link_dataframes_to_sql(list_dfs, list_table_names, dict_keys, fact_index=0, sql_engine=None):
    """
    Link multiple DataFrames in SQL, loading dimension tables first and then the fact table.
    Handles cases where dimension tables contain records that don't exist in the fact table.

    Parameters:
    list_dfs: List of DataFrames
    list_table_names: List of table names for SQL
    dict_keys: Dictionary of keys for linking
    fact_index: Index of the fact table (default 0)
    sql_engine: SQLAlchemy engine
    """
    # Step 1: First drop the fact table since it contains the foreign key constraints
    with sql_engine.connect() as conn:
        try:
            conn.execute(text(f"DROP TABLE IF EXISTS {list_table_names[fact_index]}"))
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error dropping fact table: {str(e)}")

    # Step 2: Now process the dimension tables
    for i, df in enumerate(list_dfs):
        if i == fact_index:
            continue

        # Create a copy of the DataFrame to avoid modifying the original
        df = df.copy()

        # Replace spaces and dashes in column names with underscores
        df.columns = [sanitize_column_name(col) for col in df.columns]

        # Get the keys for this DataFrame
        link_keys = dict_keys.get(i)

        # Handle column renaming if needed
        if isinstance(link_keys, dict):
            df = df.rename(columns={v: sanitize_column_name(k) for k, v in link_keys.items()})
            link_keys = [sanitize_column_name(k) for k in link_keys.keys()]
        else:
            link_keys = [sanitize_column_name(k) for k in link_keys]

        # Create table columns with data types
        columns = []
        for column, dtype in df.dtypes.items():
            sql_type = get_sql_type(dtype, column)

            if column in link_keys:
                columns.append(f'`{column}` {sql_type} NOT NULL')
            else:
                columns.append(f'`{column}` {sql_type}')

        # Add primary key constraint for linking columns
        if isinstance(link_keys, list):
            pk_columns = ', '.join(f'`{col}`' for col in link_keys)
            pk_constraint = f'PRIMARY KEY ({pk_columns})'
            columns.append(pk_constraint)

        with sql_engine.connect() as conn:
            try:
                # Drop existing dimension table if it exists
                conn.execute(text(f"DROP TABLE IF EXISTS {list_table_names[i]}"))
                conn.commit()

                # Create dimension table
                create_table_sql = f"""
                CREATE TABLE {list_table_names[i]} (
                    {',\n                '.join(columns)}
                )
                """
                conn.execute(text(create_table_sql))
                conn.commit()

                # Insert dimension data
                df.to_sql(list_table_names[i], con=conn, if_exists='append', index=False)
                conn.commit()

            except Exception as e:
                conn.rollback()
                raise Exception(f"Error processing dimension table {list_table_names[i]}: {str(e)}")

    # Step 3: Handle the fact table
    fact_df = list_dfs[fact_index].copy()
    fact_df.columns = [sanitize_column_name(col) for col in fact_df.columns]

    # Collect all columns that will be foreign keys
    foreign_key_columns = set()
    for keys in dict_keys.values():
        if isinstance(keys, list):
            foreign_key_columns.update(map(sanitize_column_name, keys))
        else:
            foreign_key_columns.update(map(sanitize_column_name, keys.keys()))

    # Create fact table columns
    columns = []
    for column, dtype in fact_df.dtypes.items():
        sql_type = get_sql_type(dtype, column)

        if column in foreign_key_columns:
            columns.append(f'`{column}` {sql_type} NOT NULL')
        else:
            columns.append(f'`{column}` {sql_type}')

    with sql_engine.connect() as conn:
        try:
            # Create fact table
            create_fact_sql = f"""
            CREATE TABLE {list_table_names[fact_index]} (
                {',\n            '.join(columns)}
            )
            """
            conn.execute(text(create_fact_sql))
            conn.commit()

            # Insert fact data
            fact_df.to_sql(list_table_names[fact_index], con=conn, if_exists='append', index=False)
            conn.commit()

            # Create foreign key constraints
            for i, keys in dict_keys.items():
                if i == fact_index:
                    continue

                if isinstance(keys, dict):
                    fk_columns = list(keys.keys())
                else:
                    fk_columns = keys

                # Create indexes for foreign key columns
                for column in fk_columns:
                    index_sql = f"CREATE INDEX `idx_{sanitize_column_name(column)}` ON {list_table_names[fact_index]} (`{sanitize_column_name(column)}`)"
                    conn.execute(text(index_sql))
                    conn.commit()

                # Add foreign key constraints
                fk_columns_sql = ', '.join(f'`{sanitize_column_name(col)}`' for col in fk_columns)
                ref_columns_sql = ', '.join(f'`{sanitize_column_name(col)}`' for col in fk_columns)
                constraint_name = f'fk_{list_table_names[fact_index]}_{list_table_names[i]}'

                fk_sql = f"""
                ALTER TABLE {list_table_names[fact_index]}
                ADD CONSTRAINT `{constraint_name}`
                FOREIGN KEY ({fk_columns_sql})
                REFERENCES {list_table_names[i]}({ref_columns_sql})
                """
                conn.execute(text(fk_sql))
                conn.commit()

        except Exception as e:
            conn.rollback()
            raise Exception(f"Error processing fact table {list_table_names[fact_index]}: {str(e)}")


dfs = [df1, df2, df3, df4, df5]
cleaned_dfs = [handle_missing_values(df) for df in dfs]

keys = {
    1: ['CustomerKey'],
    2: ['StoreKey'],
    3: ['ProductKey'],
    4: {'Order Date': 'Date', 'Currency Code': 'Currency'}
}
table_names = ['fact_sales', 'dim_customers', 'dim_stores', 'dim_products', 'dim_exchange_rates']

link_dataframes_to_sql(cleaned_dfs, table_names, keys, fact_index=0, sql_engine=engine)




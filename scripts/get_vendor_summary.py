import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

logging.basicConfig(
    filename=r"C:\Projects\Vendor_Performance_Analysis-SQL-Python-Powerbi\logs\get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
    force=True
)

def create_vendor_summary(conn):
    '''
    This function will merge the different tables to get the overall vendor summary
    and add new columns in the resultant data
    '''
    vendor_sales_summary = pd.read_sql_query("""
    WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) as FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber  
    ),

    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Volume,
            pp.Price as ActualPrice,
            SUM(p.Quantity) as TotalPurchaseQuantity,
            SUM(p.Dollars) as TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),

    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesDollars) as TotalSalesDollars,
            SUM(SalesPrice) as TotalSalesPrice,
            SUM(SalesQuantity) as TotalSalesQuantity,
            SUM(ExciseTax) as TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )

    SELECT 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """, conn)
    
    return vendor_sales_summary

def clean_data(df):
    '''This function will clean the data'''
    #changing data type to float
    df['Volume'] = df['Volume'].astype('float')
    
    #filling missing values with 0
    df.fillna(0,inplace=True)

    #removing spave from categorial columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    #creating new columns for better analysis
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars'])*100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity'] 
    df['SalestoPurchaseRatio'] = df['TotalSalesDollars'] /df['TotalPurchaseDollars']


    return df

if __name__ == '__main__':
    #creating database connection
    # conn = sqlite3.connect('Inventory.db')
    conn = sqlite3.connect(r"C:\Projects\Vendor_Performance_Analysis-SQL-Python-Powerbi\Inventory.db")

    logging.info('Creating Vendor Summary Table.....')
    summary_df =create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('Cleaning Data.....')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting Data.....')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logging.info('Completed')
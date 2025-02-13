
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

# 创建数据库连接引擎
engine = create_engine('mysql+pymysql://root:root@cdh03:3306/dev')
shop_codes = [f'SHOP_{i:05d}' for i in range(1, 1000)]
years = list(range(2020, 2025))
weeks = list(range(1, 53))
product_nos = [f'PROD_{i:06d}' for i in range(1, 10000)]
product_names = [f'Product_{i}' for i in range(1, 10000)]
color_nos = [f'CLR_{i:03d}' for i in range(1, 100)]
color_names = [f'Color_{i}' for i in range(1, 100)]

data = []
for _ in range(50000):
    shop_code = np.random.choice(shop_codes)
    year_no = np.random.choice(years)
    week_no = np.random.choice(weeks)
    product_no = np.random.choice(product_nos)
    product_name = np.random.choice(product_names)
    color_no = np.random.choice(color_nos)
    color_name = np.random.choice(color_names)
    is_top = np.random.choice([True, False])
    row = [shop_code, year_no, week_no, product_no, product_name, color_no, color_name, is_top]
    data.append(row)

columns = ['shop_code', 'year_no', 'week_no', 'product_no', 'product_name', 'color_no', 'color_name', 'is_top']
df = pd.DataFrame(data, columns=columns)

# 将DataFrame数据写入数据库表
df.to_sql('sales', engine, if_exists='replace')
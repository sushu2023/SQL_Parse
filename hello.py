import sqlparse
import pandas as pd

# 示例 SQL
sql = """
SELECT t0.goods_id AS GOODS_ID,
       t0.goods_name AS GOODS_NAME,
       t0.stock_date AS STOCK_DATE,
       t0.company_code AS COMPANY_CODE,
       t0.goods_type AS GOODS_TYPE,
       t0.bz_size AS BZ_SIZE,
       t0.qty AS QTY,
       current_timestamp AS ETL_TIME
FROM
    (SELECT cast(d.sj_date AS date) stock_date,
            cast('1000401' AS varchar(20)) company_code,
            cast(d.goods_id AS varchar(20)) goods_id,
            cast(d.goods_name AS varchar(500)) goods_name,
            cast(d.goods_type AS varchar(20)) goods_type,
            cast(d.bz_size AS varchar(500)) bz_size,
            cast(d.new_kc AS decimal(28, 2)) qty
     FROM getkchistorybyday(to_char(current_date-1,'yyyy-mm-dd')) d) t0
"""

# 解析 SQL
parsed = sqlparse.parse(sql)[0]

# 提取 SELECT 列
columns = []
for token in parsed.tokens:
    if isinstance(token, sqlparse.sql.IdentifierList):
        for identifier in token.get_identifiers():
            # 手动解析字段和别名
            parts = str(identifier).split(" AS ")
            if len(parts) == 2:
                real_name = parts[0].strip()
                alias = parts[1].strip()
            else:
                real_name = str(identifier).strip()
                alias = None
            
            # 获取来源表或子查询
            source = "t0" if "." in real_name else None
            columns.append({
                "alias": alias,
                "real_name": real_name,
                "source": source
            })

# 导出到 Excel
df = pd.DataFrame(columns)
df.to_excel("sql_analysis.xlsx", index=False)

print("SQL 分析完成，结果已导出到 sql_analysis.xlsx")
import sqlglot
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
parsed = sqlglot.parse_one(sql)

# 提取 SELECT 列
columns = []
for select_expr in parsed.find_all(sqlglot.expressions.Select):
    for column in select_expr.expressions:
        alias = column.alias  # 获取别名
        real_name = column.this.sql() if hasattr(column, "this") else str(column)
        source = column.find_ancestor(sqlglot.expressions.Table).alias_or_name if column.find_ancestor(sqlglot.expressions.Table) else None
        processing = column.sql() if isinstance(column, sqlglot.expressions.Cast) else None
        columns.append({
            "alias": alias,
            "real_name": real_name,
            "source": source,
            "processing": processing
        })

# 导出到 Excel
df = pd.DataFrame(columns)
df.to_excel("sql_analysis_sqlglot.xlsx", index=False)

print("SQL 分析完成，结果已导出到 sql_analysis_sqlglot.xlsx")
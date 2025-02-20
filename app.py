import streamlit as st
import sqlparse
import pandas as pd
import io
import re

# ====================
# 解析 SQL 的核心逻辑
# ====================

def extract_columns(sql):
    """
    提取 SELECT 子句中的列及其别名。
    """
    parsed = sqlparse.parse(sql)[0]  # 解析 SQL
    columns = []
    
    for token in parsed.tokens:
        if isinstance(token, sqlparse.sql.IdentifierList):  # 处理多个列的情况
            for identifier in token.get_identifiers():
                columns.append(parse_identifier(identifier))
        elif isinstance(token, sqlparse.sql.Identifier):  # 单个列的情况
            columns.append(parse_identifier(token))
    
    return columns

def parse_identifier(identifier):
    """
    解析单个列，提取原始列名和别名。
    """
    original_name = str(identifier).split(" AS ")[0].strip()
    alias = identifier.get_alias() or original_name
    return {"original_name": original_name, "alias": alias}

def extract_tables(sql):
    """
    提取 FROM 子句中的表名，包括递归解析子查询。
    """
    tables = set()  # 使用集合避免重复
    parsed = sqlparse.parse(sql)[0]
    
    def _extract_from_clause(tokens):
        for token in tokens:
            if isinstance(token, sqlparse.sql.Identifier):  # 表名
                tables.add(str(token).strip())
            elif isinstance(token, sqlparse.sql.Parenthesis):  # 子查询
                subquery_sql = token.value.strip()[1:-1]  # 去掉括号
                _extract_from_clause(sqlparse.parse(subquery_sql)[0].tokens)
            elif isinstance(token, sqlparse.sql.IdentifierList):  # 多个表
                for identifier in token.get_identifiers():
                    tables.add(str(identifier).strip())
    
    for token in parsed.tokens:
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "FROM":
            next_token = parsed.token_next(parsed.token_index(token))[1]  # 获取 FROM 后的下一个 Token
            if isinstance(next_token, sqlparse.sql.TokenList):
                _extract_from_clause(next_token)
            else:
                tables.add(str(next_token).strip())
    
    return list(tables)

def analyze_column_logic(sql):
    """
    分析列的加工逻辑（如聚合、计算等）。
    """
    # 示例：简单匹配聚合函数
    aggregations = re.findall(r"(SUM|AVG|COUNT|MAX|MIN)\((.*?)\)", sql, re.IGNORECASE)
    return [{"function": func, "column": col} for func, col in aggregations]

def parse_sql(sql):
    """
    主解析函数，返回解析结果的 DataFrame。
    """
    columns = extract_columns(sql)
    tables = extract_tables(sql)
    logic = analyze_column_logic(sql)
    
    # 构建解析结果
    parsed_data = []
    for col in columns:
        parsed_data.append({
            "Original Column": col["original_name"],
            "Alias": col["alias"],
            "Source Table": ", ".join(tables),
            "Processing Logic": next((l["function"] + "(" + l["column"] + ")" for l in logic if l["column"] in col["original_name"]), "None")
        })
    
    return pd.DataFrame(parsed_data)

def to_excel(df):
    """
    将 DataFrame 转换为 Excel 文件的字节流。
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Parsed Results")
    output.seek(0)
    return output.read()

# ====================
# Streamlit 应用界面
# ====================

# 页面标题
st.title("SQL 解析工具")

# 用户输入 SQL
user_sql = st.text_area("请输入 SQL 查询语句", height=200)

# 提交按钮
if st.button("解析"):
    if user_sql.strip() == "":
        st.error("请输入有效的 SQL 查询！")
    else:
        # 调用解析函数
        try:
            parsed_result = parse_sql(user_sql)
            
            # 展示解析结果
            st.write("解析结果：")
            st.dataframe(parsed_result)
            
            # 提供 Excel 下载
            st.download_button(
                label="下载解析结果",
                data=to_excel(parsed_result),
                file_name="parsed_sql_results.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            st.error(f"解析失败：{str(e)}")
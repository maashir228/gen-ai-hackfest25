import streamlit as st
import requests
import re
import json
import os
import sys
import pandas as pd
from supabase import create_client, Client
from typing import Dict, List, Any, Union
from dotenv import load_dotenv

st.set_page_config(
    page_title="NL to SQL Database Assistant",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load environment variables
try:
    load_dotenv()
    print("Loaded environment from .env file")
except ImportError:
    print("python-dotenv package not installed. Loading directly from environment.")
except Exception as e:
    print(f"Could not load .env file: {str(e)}")

# Initialize session state variables
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'schema_data' not in st.session_state:
    st.session_state.schema_data = None
if 'connected' not in st.session_state:
    st.session_state.connected = False
if 'connection_error' not in st.session_state:
    st.session_state.connection_error = None

# Set up sidebar
st.sidebar.title("Database Connection")

# Get connection details from sidebar or environment variables
supabase_url = st.sidebar.text_input("Supabase URL", os.environ.get('SUPABASE_URL', ''))
supabase_key = st.sidebar.text_input("Supabase Key", os.environ.get('SUPABASE_KEY', ''), type="password")
gemini_api_key = st.sidebar.text_input("Gemini API Key", os.environ.get('GEMINI_API_KEY', ''), type="password")

# Connect button
if st.sidebar.button("Connect to Database"):
    try:
        with st.spinner("Connecting to Supabase..."):
            sb = create_client(supabase_url, supabase_key)
            test_query = "SELECT 1 as test"
            test_response = sb.rpc('run_sql', {'sql_query': test_query}).execute()
            
            if test_response.data:
                st.session_state.connected = True
                st.session_state.connection_error = None
                st.session_state.sb = sb
                st.session_state.GEMINI_API_URL = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={gemini_api_key}'
                st.sidebar.success("Successfully connected to Supabase!")
            else:
                st.session_state.connection_error = "Connected but received empty response from test query"
                st.sidebar.error(st.session_state.connection_error)
    except Exception as e:
        st.session_state.connection_error = f"Error connecting to Supabase: {str(e)}"
        st.sidebar.error(st.session_state.connection_error)
        st.session_state.connected = False

# Display connection status
if st.session_state.connected:
    st.sidebar.success("Connected to Supabase!")
elif st.session_state.connection_error:
    st.sidebar.error(f"Connection failed: {st.session_state.connection_error}")

# Main area
st.title("🤖 NL to SQL Database Assistant")
st.markdown("""
Ask questions about your database in natural language and get SQL queries and results back!
""")

# Define functions
def get_supabase_schema():
    if not st.session_state.connected:
        return {"error": "Not connected to database"}
        
    try:
        try:
            response = st.session_state.sb.rpc("get_table_schema", {}).execute()
            
            if response.data:
                schema = {}
                for entry in response.data:
                    table = entry["table_name"]
                    column = entry["column_name"]
                    if table not in schema:
                        schema[table] = []
                    schema[table].append(column)
                return schema
        except Exception as func_error:
            print(f"Direct RPC call failed: {str(func_error)}, falling back to SQL query.")
            
        schema_query = """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
        """
        response = st.session_state.sb.rpc('run_sql_query', {'sql_query': schema_query}).execute()
        
        if not response.data:
            response = st.session_state.sb.rpc('run_sql', {'sql_query': schema_query}).execute()
        
        if response.data:
            schema = {}
            for entry in response.data:
                table = entry["table_name"]
                column = entry["column_name"]
                if table not in schema:
                    schema[table] = []
                schema[table].append(column)
            return schema
        else:
            return {"error": "No data returned from schema query"}
    except Exception as e:
        return {"error": f"Exception occurred while fetching schema: {str(e)}"}

def format_schema_for_prompt(schema_data):
    schema_str = "Here is the database schema:\n"
    
    if isinstance(schema_data, dict) and "error" not in schema_data:
        for table, columns in schema_data.items():
            schema_str += f"\nTable `{table}` with columns: {', '.join(columns)}"
    elif isinstance(schema_data, list):
        table_dict = {}
        for row in schema_data:
            table = row['table_name']
            column = row['column_name']
            if table not in table_dict:
                table_dict[table] = []
            table_dict[table].append(column)
            
        for table, columns in table_dict.items():
            schema_str += f"\nTable `{table}` with columns: {', '.join(columns)}"
    
    return schema_str

def nl_to_sql_gemini(prompt: str):
    if not st.session_state.connected:
        return {"error": "Not connected to database"}
        
    headers = {
        "Content-Type": "application/json"
    }

    schema_data = st.session_state.schema_data
    schema_error = schema_data is None or (isinstance(schema_data, dict) and "error" in schema_data)
    
    if schema_error:
        schema_description = "Generate SQL using the common tables like 'employees', 'customers', 'orders', 'products', or 'refund_requests' with standard columns."
    else:
        schema_description = format_schema_for_prompt(schema_data)

    prompt_lower = prompt.lower()
    is_update = "update" in prompt_lower or "modify" in prompt_lower or "change" in prompt_lower or "set" in prompt_lower
    is_delete = "delete" in prompt_lower or "remove" in prompt_lower
    is_insert = "insert" in prompt_lower or "add" in prompt_lower or "create" in prompt_lower or "new" in prompt_lower
    is_select = "fetch" in prompt_lower or "show" in prompt_lower or "get" in prompt_lower or "select" in prompt_lower or "find" in prompt_lower or "list" in prompt_lower

    has_row_reference = re.search(r"(?:row|record|id)\s*(?:number)?\s*(\d+)", prompt_lower)
    row_id = has_row_reference.group(1) if has_row_reference else None

    operation_guidance = ""
    if is_select:
        operation_guidance = "\nFor the SELECT query:\n- Use PostgreSQL syntax\n- Use single quotes for strings\n- Use ILIKE for case-insensitive matching"
    elif is_insert:
        operation_guidance = "\nFor the INSERT query:\n- Include all relevant columns from the user's request\n- Do NOT include the id column (it's auto-generated)\n- Do NOT include the created_at column (it's auto-generated)\n- Use single quotes for strings"
    elif is_update:
        operation_guidance = "\nFor the UPDATE query:\n- Include all relevant columns to update from the user's request\n- Be sure to include a proper WHERE clause"
        if row_id:
            operation_guidance += f"\n- Use 'WHERE id = {row_id}' as the condition"
    elif is_delete:
        operation_guidance = "\nFor the DELETE query:\n- Include a proper WHERE clause to avoid deleting all records"
        if row_id:
            operation_guidance += f"\n- Use 'WHERE id = {row_id}' as the condition"

    table_hint = ""
    common_tables = ["employees", "customers", "orders", "products", "refund_requests"]
    for table in common_tables:
        if table in prompt_lower or table[:-1] in prompt_lower:
            if schema_error:
                table_hint = f"\nYou should use the '{table}' table for this query."
            break

    refined_prompt = (
        f"{schema_description}\n\n"
        f"Generate a PostgreSQL query for: {prompt}.{operation_guidance}{table_hint}\n\n"
        f"Return only the SQL query without any explanation, markdown formatting, or backticks."
    )

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": refined_prompt}
                ]
            }
        ]
    }

    try:
        response = requests.post(st.session_state.GEMINI_API_URL, json=payload, headers=headers)

        if response.status_code == 200:
            data = response.json()
            candidates = data.get('candidates', [])
            if candidates:
                text = candidates[0].get('content', {}).get('parts', [{}])[0].get('text', '')
                text = text.replace('```sql', '').replace('```', '').strip()
                
                sql_match = re.search(r'(SELECT|INSERT|UPDATE|DELETE).*', text, re.IGNORECASE | re.DOTALL)
                if sql_match:
                    return sql_match.group(0).strip()
                return text.strip()
            else:
                return {"error": "No candidates returned by Gemini."}
        else:
            return {"error": f"Gemini API error: {response.text}"}
    except Exception as e:
        return {"error": f"Error calling Gemini API: {str(e)}"}

def execute_sql_query(query: str):
    if not st.session_state.connected:
        return {"error": "Not connected to database"}
        
    try:
        query = query.strip().rstrip(';')
        
        if query.lower().startswith("insert into"):
            match = re.match(r"insert\s+into\s+(\w+)\s*\((.*?)\)\s*values\s*\((.*?)\)", query, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                columns = [col.strip() for col in match.group(2).split(',')]
                values = [val.strip() for val in match.group(3).split(',')]
                
                data = {}
                for i, column in enumerate(columns):
                    value = values[i]
                    if value.upper() == 'NULL':
                        data[column] = None
                    elif (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
                        data[column] = value[1:-1]
                    else:
                        try:
                            if '.' in value:
                                data[column] = float(value)
                            else:
                                data[column] = int(value)
                        except ValueError:
                            data[column] = value
                
                response = st.session_state.sb.table(table_name).insert(data).execute()
                
                if hasattr(response, 'error') and response.error:
                    return {"error": f"Insert failed: {response.error}"}
                    
                if hasattr(response, 'data'):
                    return response.data
                else:
                    return {"message": "Insert was executed successfully", "success": True}
        
        elif query.lower().startswith("update"):
            match = re.match(r"update\s+(\w+)\s+set\s+(.*?)\s+where\s+(.*)", query, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                set_clause = match.group(2)
                where_clause = match.group(3)
                
                set_items = set_clause.split(',')
                data = {}
                
                for item in set_items:
                    if '=' in item:
                        column, value = [part.strip() for part in item.split('=', 1)]
                        
                        if value.upper() == 'NULL':
                            data[column] = None
                        elif (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
                            data[column] = value[1:-1]
                        else:
                            try:
                                if '.' in value:
                                    data[column] = float(value)
                                else:
                                    data[column] = int(value)
                            except ValueError:
                                data[column] = value
                
                where_match = re.match(r"(\w+)\s*=\s*(\S+)", where_clause)
                if where_match:
                    where_column = where_match.group(1)
                    where_value = where_match.group(2).strip()
                    
                    if where_value.startswith("'") and where_value.endswith("'"):
                        where_value = where_value[1:-1]
                    elif where_value.isdigit():
                        where_value = int(where_value)
                    
                    response = st.session_state.sb.table(table_name).update(data).eq(where_column, where_value).execute()
                    
                    if hasattr(response, 'error') and response.error:
                        return {"error": f"Update failed: {response.error}"}
                        
                    if hasattr(response, 'data'):
                        return response.data
                    else:
                        return {"message": "Update was executed successfully", "success": True}
                        
        elif query.lower().startswith("delete from"):
            match = re.match(r"delete\s+from\s+(\w+)(?:\s+where\s+(.*))?", query, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                where_clause = match.group(2) if match.group(2) else None
                
                if not where_clause:
                    return {"error": "DELETE without WHERE clause is not allowed for safety. Please specify a WHERE condition."}
                
                where_match = re.match(r"(\w+)\s*=\s*(\S+)", where_clause)
                if where_match:
                    where_column = where_match.group(1)
                    where_value = where_match.group(2).strip()
                    
                    if where_value.startswith("'") and where_value.endswith("'"):
                        where_value = where_value[1:-1]
                    elif where_value.isdigit():
                        where_value = int(where_value)
                    
                    response = st.session_state.sb.table(table_name).delete().eq(where_column, where_value).execute()
                    
                    if hasattr(response, 'error') and response.error:
                        return {"error": f"Delete failed: {response.error}"}
                        
                    if hasattr(response, 'data'):
                        return {"message": "Delete was executed successfully", "success": True, "rows_affected": len(response.data) if response.data else 0}
                    else:
                        return {"message": "Delete was executed successfully", "success": True}
        
        try:
            response = st.session_state.sb.rpc("run_sql_query", {"sql_query": query}).execute()
        except Exception as e:
            try:
                response = st.session_state.sb.rpc('run_sql', {'sql_query': query}).execute()
            except Exception as e2:
                return {"error": f"Query failed: {str(e2)}"}
            
        if hasattr(response, 'error') and response.error:
            return {"error": f"Database error: {response.error}"}
            
        if hasattr(response, 'data'):
            if response.data:
                return response.data
            else:
                if any(op in query.lower() for op in ['insert', 'update', 'delete']):
                    operation = "insert" if "insert" in query.lower() else "update" if "update" in query.lower() else "delete"
                    return {"message": f"The {operation} operation was executed successfully", "success": True}
                return []
        else:
            return {"warning": "Query executed but returned no data attribute"}
            
    except Exception as e:
        error_message = str(e)
        return {"error": f"Query failed: {error_message}"}

def handle_database_query(user_input: str):
    if not st.session_state.connected:
        return {"error": "Not connected to database"}
        
    # Generate SQL from natural language
    sql_query = nl_to_sql_gemini(user_input)
    
    if isinstance(sql_query, dict) and "error" in sql_query:
        return sql_query, None
    
    # Execute SQL query
    result = execute_sql_query(sql_query)
    
    # Add to history
    st.session_state.query_history.append({
        "user_input": user_input,
        "sql_query": sql_query,
        "result": result
    })
    
    return result, sql_query

# Refresh schema button
if st.sidebar.button("Refresh Database Schema") and st.session_state.connected:
    with st.spinner("Fetching database schema..."):
        st.session_state.schema_data = get_supabase_schema()
        if "error" not in st.session_state.schema_data:
            st.sidebar.success("Schema refreshed successfully!")
        else:
            st.sidebar.error(f"Error fetching schema: {st.session_state.schema_data['error']}")

# Display schema if available
if st.session_state.connected and st.session_state.schema_data is None:
    with st.spinner("Fetching database schema..."):
        st.session_state.schema_data = get_supabase_schema()

# Schema expander
if st.session_state.schema_data and "error" not in st.session_state.schema_data:
    with st.sidebar.expander("Database Schema"):
        for table, columns in st.session_state.schema_data.items():
            st.sidebar.markdown(f"**{table}**")
            st.sidebar.text(", ".join(columns))

# User input
if st.session_state.connected:
    user_input = st.text_area("Ask a question about your database:", height=100, 
                              placeholder="Example: Show me all customers from New York", key="user_input")
    col1, col2 = st.columns([1, 5])
    with col1:
        submit_button = st.button("Submit", type="primary", use_container_width=True)
    with col2:
        clear_button = st.button("Clear Results", use_container_width=True)
        
    # Process the query on button click
    if submit_button and user_input:
        with st.spinner("Processing your request..."):
            result, sql_query = handle_database_query(user_input)
            
            # Display results
            st.subheader("Generated SQL:")
            st.code(sql_query, language="sql")
            
            st.subheader("Result:")
            if isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict):
                df = pd.DataFrame(result)
                st.dataframe(df, use_container_width=True)
            elif isinstance(result, dict):
                if "error" in result:
                    st.error(result["error"])
                elif "message" in result:
                    st.success(result["message"])
                    if "rows_affected" in result:
                        st.info(f"Rows affected: {result['rows_affected']}")
                elif "warning" in result:
                    st.warning(result["warning"])
                else:
                    st.json(result)
            elif isinstance(result, list) and len(result) == 0:
                st.info("Query executed successfully, but returned no data.")
            else:
                st.write(result)
    
    if clear_button:
        st.session_state.query_history = []
        st.experimental_rerun()
        
    # Show query history
    if st.session_state.query_history:
        with st.expander("Query History", expanded=False):
            for i, item in enumerate(reversed(st.session_state.query_history)):
                st.markdown(f"### Query {len(st.session_state.query_history) - i}")
                st.markdown(f"**Question:** {item['user_input']}")
                st.markdown("**SQL Query:**")
                st.code(item['sql_query'], language="sql")
                
                # Display results for this query
                st.markdown("**Result:**")
                if isinstance(item['result'], list) and len(item['result']) > 0 and isinstance(item['result'][0], dict):
                    df = pd.DataFrame(item['result'])
                    st.dataframe(df, use_container_width=True)
                elif isinstance(item['result'], dict):
                    if "error" in item['result']:
                        st.error(item['result']["error"])
                    elif "message" in item['result']:
                        st.success(item['result']["message"])
                    else:
                        st.json(item['result'])
                else:
                    st.write(item['result'])
                    
                st.markdown("---")
else:
    st.info("Please connect to your database using the sidebar options.")

# Footer
st.markdown("---")
st.caption("Made with ❤️ using Streamlit, Supabase, and Gemini")
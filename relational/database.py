import json
import os
import pandas as pd
from collections import defaultdict

class Database:
    def __init__(self, data_model):
        self.data_model = data_model
        self.tables = {}
        self.load_existing_tables()

    # CREATE THE TABLE
    def create_table(self, table_name, columns):
        # Remove unnecessary characters from column names
        columns = [col.strip('()') for col in columns]

        if table_name in self.tables:
            return f'Table {table_name} already exists.'

        self.tables[table_name] = {'columns': columns, 'data': []}
        self.save_to_file(table_name)
        return f'Table {table_name} created.'
    
    # INSERT DATA TO THE SPECIFIED TABLE
    def insert_data(self, table_name, values):
        if table_name not in self.tables:
            return f'Table {table_name} does not exist.'
        
        if len(values) != len(self.tables[table_name]['columns']):
            return 'Number of columns does not match.'

        self.tables[table_name]['data'].append(dict(zip(self.tables[table_name]['columns'], values)))
        return 'Data inserted successfully.'
    

    def batch_insert_data(self, table_name, data):
        if table_name not in self.tables:
            return f'Table {table_name} does not exist.'

        for values in data:
            if len(values) != len(self.tables[table_name]['columns']):
                return 'Number of columns does not match.'
            self.tables[table_name]['data'].append(dict(zip(self.tables[table_name]['columns'], values)))

        self.save_to_file(table_name)
        return 'Batch data inserted successfully.'
    
    # FETCH ALL RECORDS FROM THE SPECIFIED TABLE
    def select_data(self, table_name):
        if table_name not in self.tables:
            return f'Table {table_name} does not exist.'

        table_data = self.tables[table_name]
        if not table_data['data']:
            return f'Table {table_name} is empty.'

        formatted_output = []
        for row in table_data['data']:
            #formatted_output.append(f"ID: {row['id']}, Name: {row['name']}, RGB: {row['rgb']}")
            formatted_output.append(self.format_select_output(row))

        return "\n".join(formatted_output)
    
    # FETCH ALL RECORDS FROM THE SPECIFIED TABLE WITH THE CONDITION
    def select_data_with_condition(self, table_name, col_name, operator, value):
        if table_name not in self.tables:
            return f'Table {table_name} does not exist.'

        table_data = self.tables[table_name]
        if not table_data['data']:
            return f'Table {table_name} is empty.'

        formatted_output = []
        for row in table_data['data']:
            if col_name in row:
                col_value = row[col_name]

                if operator == '=' and col_value == value:
                    formatted_output.append(self.format_select_output(row))
                elif operator == '>' and isinstance(col_value, int) and col_value > value:
                    formatted_output.append(self.format_select_output(row))
                elif operator == '<' and isinstance(col_value, int) and col_value < value:
                    formatted_output.append(self.format_select_output(row))

        if not formatted_output:
            return f'No matching rows found for the condition.'
        else:
            return "\n".join(formatted_output)
    def select_specific_data_with_condition(self, table_name, col_to_find, col_name, operator, value):
        if table_name not in self.tables:
            return f'Table {table_name} does not exist.'

        if col_to_find not in self.tables[table_name]['columns']:
            return f'Column {col_to_find} does not exist in table {table_name}.'

        table_data = self.tables[table_name]
        if not table_data['data']:
            return f'Table {table_name} is empty.'

        formatted_output = []
        for row in table_data['data']:
            if col_name in row and col_to_find in row:
                col_value = row[col_name]

                try:
                    value_converted = self.convert_type_for_comparison(col_value, value)
                except ValueError:
                    return f"Value '{value}' is not valid for comparison with column '{col_name}'."

                if (operator == '=' and col_value == value_converted) or \
                   (operator == '>' and col_value > value_converted) or \
                   (operator == '<' and col_value < value_converted):
                    if row[col_to_find] not in formatted_output:
                        formatted_output.append(row[col_to_find])

        if not formatted_output:
            return f'No matching rows found for the condition.'
        else:
            return "\n".join(formatted_output)


    
    # DELETE ALL RECORDS FROM THE SPECIFIED TABLE
    def delete_all_records(self, table_name):
        if table_name not in self.tables:
            return f'Table {table_name} does not exist.'

        self.tables[table_name]['data'] = []
        self.save_to_file(table_name)
        return f'All records deleted from {table_name}.'

    # DELETE ALL RECORDS FROM THE SPECIFIED TABLE WITH CONDITION
    def delete_records_with_condition(self, table_name, col_name, operator, value):
        if table_name not in self.tables:
            return f'Table {table_name} does not exist.'

        new_data = []
        for row in self.tables[table_name]['data']:
            if not self.check_condition(row, col_name, operator, value):
                new_data.append(row)

        self.tables[table_name]['data'] = new_data
        self.save_to_file(table_name)
        return f'Records deleted from {table_name} based on the condition.'

    def update_records_with_condition(self, table_name, set_col_name, set_value, condition_col_name, condition_value):
        if table_name not in self.tables:
            return f'Table {table_name} does not exist.'

        for row in self.tables[table_name]['data']:
            if row.get(condition_col_name) == condition_value:
                row[set_col_name] = set_value

        self.save_to_file(table_name)
        return f'Records updated in {table_name}.'

    def join_tables(self, table1_name, table2_name, join_column1, join_column2, join_type='inner'):
        if table1_name not in self.tables or table2_name not in self.tables:
            return 'One or both tables do not exist.'

        table1 = self.tables[table1_name]
        table2 = self.tables[table2_name]

        # Check if join columns exist in their respective tables
        if join_column1 not in table1['columns']:
            return f"Join column '{join_column1}' does not exist in table '{table1_name}'."
        if join_column2 not in table2['columns']:
            return f"Join column '{join_column2}' does not exist in table '{table2_name}'."

        joined_data = []
        for row1 in table1['data']:
            for row2 in table2['data']:
                if row1[join_column1] == row2[join_column2]:
                    # Combine data from both rows
                    combined_row = {**row1, **row2}
                    joined_data.append(combined_row)

        if join_type == 'inner':
            # For inner join, return only the matched rows
            return joined_data
        # Other join types can be implemented similarly

        # Format the output
        return "\n".join(str(row) for row in joined_data)

    def aggregate_data(self, table_name, agg_column, agg_func):
        if table_name not in self.tables:
            return f"Table {table_name} does not exist."

        if agg_column not in self.tables[table_name]['columns']:
            return f"Column {agg_column} does not exist in table {table_name}."

        data = [row[agg_column] for row in self.tables[table_name]['data']]
        try:
            if agg_func == 'sum':
                return sum(data)
            elif agg_func == 'avg':
                return sum(data) / len(data)
            elif agg_func == 'count':
                return len(data)
            elif agg_func == 'min':
                return min(data)
            elif agg_func == 'max':
                return max(data)
            else:
                return 'Invalid aggregation function.'
        except TypeError:
            return 'Aggregation error: non-numeric data encountered.'
        
    def group_by(self, table_name, group_columns, agg_column, agg_func):
        if table_name not in self.tables:
            return f"Table {table_name} does not exist."

        for col in group_columns:
            if col not in self.tables[table_name]['columns']:
                return f"Column {col} does not exist in table {table_name}."

        grouped_data = defaultdict(list)
        for row in self.tables[table_name]['data']:
            key = tuple(row[col] for col in group_columns)
            grouped_data[key].append(row)

        # 如果提供了聚合函数和列名，应用聚合函数
        if agg_func and agg_column:
            result = {}
            for key, group in grouped_data.items():
                # 根据提供的聚合函数计算结果
                if agg_func.lower() == 'count':
                    result[key] = len(group)
                elif agg_func.lower() == 'sum':
                    result[key] = sum(row[agg_column] for row in group)
                elif agg_func.lower() == 'avg':
                    result[key] = sum(row[agg_column] for row in group) / len(group)
                # ... 可以添加其他聚合函数的处理
            return result
        else:
            # 如果没有提供聚合函数，只返回分组数据
            return grouped_data


    def aggregate_data_internal(self, data, agg_column, agg_func):
        try:
            if agg_func == 'sum':
                return sum(row[agg_column] for row in data)
            elif agg_func == 'avg':
                return sum(row[agg_column] for row in data) / len(data)
            elif agg_func == 'count':
                return len(data)
            elif agg_func == 'min':
                return min(row[agg_column] for row in data)
            elif agg_func == 'max':
                return max(row[agg_column] for row in data)
        except TypeError:
            return 'Aggregation error: non-numeric data encountered.'
    
    def order_by(self, table_name, sort_columns):
        if table_name not in self.tables:
            return f"Table {table_name} does not exist."

        # 检查所有排序列是否存在于表中
        for col, _ in sort_columns:
            if col not in self.tables[table_name]['columns']:
                return f"Column {col} does not exist in table {table_name}."

        # 使用多列排序
        self.tables[table_name]['data'].sort(key=lambda x: tuple(x[col] for col, _ in sort_columns), 
                                            reverse=any(asc == False for _, asc in sort_columns))
        return self.select_data(table_name)

    
    def save_to_file(self, table_name):
        filename = f'{table_name}.csv'  # Always use CSV extension
        df = pd.DataFrame(self.tables[table_name]['data'])
        df.to_csv(filename, index=False)  # Save data to CSV without row indices


    def update_file_data(self, table_name):
        self.save_to_file(table_name)  # Reuse save_to_file for updating

    def load_existing_tables(self):
    # Iterate over existing files and load tables
        for filename in os.listdir():
            if filename.endswith('.csv'):
                table_name = os.path.splitext(filename)[0]
                self.load_from_file(table_name)

    def load_from_file(self, table_name):
        filename = f'{table_name}.csv'
        try:
            df = pd.read_csv(filename)
            data = {'columns': list(df.columns), 'data': df.to_dict(orient='records')}
            self.tables[table_name] = data
        except FileNotFoundError:
            print(f"File {filename} not found.")
        except pd.errors.EmptyDataError:
            print(f"Error loading data from {filename}.")


    def execute_query(self, query):
        tokens = query.lower().split()
        table_name = None

        # 首先检查是否是退出命令
        if query.strip().lower() == 'exit':
            for table_name in self.tables:
                self.save_to_file(table_name)
            return 'Exiting...'

        # 检查特定类型的命令并提取表名
        if 'from' in tokens:
            from_index = tokens.index('from')
            table_name = tokens[from_index + 1]
        elif query.startswith('create table') or query.startswith('insert into'):
            table_name = query.split()[2]

        if query.startswith('create table'):
            tokens = query.split()
            table_name = tokens[2]
            columns = [col.strip(',') for col in tokens[3:]]
            result = self.create_table(table_name, columns)
            return result  # Return the result directly, which may be "Table created" or "Table already exists."
        
        elif query.startswith('insert into'):
            tokens = query.split(maxsplit=3)
            table_name = tokens[2]

            values_part = tokens[3].strip("()")  
            values = [val.strip(' " ') for val in values_part.split(',')]  # 根据逗号分割并移除引号和额外空格

            return self.insert_data(table_name, values)


        elif query.startswith('find all'):
            tokens = query.split()
            table_name = tokens[2]
            if(len(tokens) == 3):
                return self.select_data(table_name)
            elif len(tokens) > 3:
                # Find statement with conditions
                condition_start_index = tokens.index('where')
                condition_tokens = tokens[condition_start_index + 1:]

                # Ensure the condition has a valid structure
                if len(condition_tokens) == 3:
                    col_name = condition_tokens[0]
                    operator = condition_tokens[1]
                    value = condition_tokens[2].strip('\"')
                    try:
                        if col_name in self.tables[table_name]['columns']:
                            value = self.convert_type(self.tables[table_name], col_name, value)
                    except ValueError:
                        return 'Invalid value type.'

                    selected_data = self.select_data_with_condition(table_name, col_name, operator, value)
                    return selected_data
                else:
                    return 'Invalid condition format. Use: find all <table_name> where <col_name> <operator> "<value>"'

            
        elif query.startswith('delete all'):
            tokens = query.split()
            table_name = tokens[2]

            if len(tokens) == 3:
                return self.delete_all_records(table_name)
            
            elif len(tokens) > 3 and tokens[3] == 'where':
                # Delete with condition
                condition_start_index = tokens.index('where')
                condition_tokens = tokens[condition_start_index + 1:]

                # Ensure the condition has a valid structure
                if len(condition_tokens) == 3:
                    col_name = condition_tokens[0]
                    operator = condition_tokens[1]
                    value = condition_tokens[2].strip('\"')

                    # Perform the deletion based on the condition
                    return self.delete_records_with_condition(table_name, col_name, operator, value)

                else:
                    return 'Invalid condition format. Use: delete all <table_name> where <col_name> <operator> "<value>"'
            
            else:
                return 'Invalid delete command format. Use: delete all <table_name> [where <col_name> <operator> "<value>"]'
            
        elif query.startswith('find'):
            tokens = query.split()
            col_to_find = tokens[1]  
            table_name = tokens[3] 

            if len(tokens) > 4:
                condition_start_index = tokens.index('where')
                condition_tokens = tokens[condition_start_index + 1:]

                if len(condition_tokens) >= 3:
                    col_name = condition_tokens[0]
                    operator = condition_tokens[1]
                    value = " ".join(condition_tokens[2:]).strip('\"')
                    try:
                        if col_name in self.tables[table_name]['columns']:
                            value = self.convert_type(self.tables[table_name], col_name, value)
                    except ValueError:
                        return 'Invalid value type.'

                    selected_data = self.select_specific_data_with_condition(table_name, col_to_find, col_name, operator, value)
                    return selected_data
            else:
                return 'Invalid condition format. Use: find <column_name> from <table_name> where <col_name> <operator> "<value>"'
        
        elif query.startswith('delete from'):
            tokens = query.split()
            table_name = tokens[2]

            if 'where' in tokens:
                where_index = tokens.index('where')
                col_name = tokens[where_index + 1]
                operator = '=' 
                value = tokens[where_index + 3].strip('\"')

                return self.delete_records_with_condition(table_name, col_name, operator, value)
            else:
                return self.delete_all_records(table_name)

        elif query.startswith('update'):
            try:
                tokens = query.split()
                table_name = tokens[1]

                set_index = tokens.index('set')
                set_col_name = tokens[set_index + 1]
                set_value_str = query.split('set')[1].split('where')[0]
                set_value = set_value_str.split('=')[1].strip(' \'\"')

                where_index = tokens.index('where')
                condition_str = query.split('where')[1]
                condition_col_name = condition_str.split('=')[0].strip()
                condition_value = condition_str.split('=')[1].strip(' \'\"')

                return self.update_records_with_condition(table_name, set_col_name, set_value, condition_col_name, condition_value)
            except IndexError:
                return 'Error in parsing the update query.'

        elif query.startswith('join'):
            tokens = query.split()
            if len(tokens) < 6:  # Adjusted for two join columns
                return 'Invalid join query format.'

            table1_name = tokens[1]
            table2_name = tokens[2]
            join_column1 = tokens[4]  # Join column for the first table
            join_column2 = tokens[5]  # Join column for the second table
            join_type = 'inner'  # Default join type

            # Handle different types of joins if specified in the query
            if 'left' in query:
                join_type = 'left'
            elif 'right' in query:
                join_type = 'right'
            elif 'full' in query:
                join_type = 'full'

            return self.join_tables(table1_name, table2_name, join_column1, join_column2, join_type)
        
        
        elif 'select' in tokens and 'group by' in query.lower():
            select_parts = query.lower().split('select')[1].split('from')[0].strip().split(',')
            agg_func, agg_column = None, None
            if any(func in select_parts[0] for func in ['sum', 'avg', 'count', 'min', 'max']):
                agg_func_part = select_parts[0].split('(')
                agg_func = agg_func_part[0].strip()
                agg_column = agg_func_part[1].split(')')[0].strip()

            group_by_index = query.lower().find('group by')
            group_columns = query[group_by_index + 9:].split(',')

            return self.group_by(table_name, group_columns, agg_column, agg_func)

        # 处理ORDER BY查询
        elif 'order by' in query.lower():
            order_by_index = query.lower().find('order by')
            order_by_clause = query[order_by_index + 9:].strip()

            # 分割列名和排序方向
            order_columns = order_by_clause.split(',')
            sort_columns = []
            for col in order_columns:
                col_parts = col.strip().split()
                column_name = col_parts[0].strip()
                ascending = True  # 默认升序
                if len(col_parts) > 1 and col_parts[1].lower() == 'desc':
                    ascending = False
                sort_columns.append((column_name, ascending))

            return self.order_by(table_name, sort_columns)

        else:
            return 'Unsupported query.'
        
        
    def convert_type(self, table, col_name, value):
        if value.isdigit():
            return int(value)
        else:
            return value
    
    def convert_type_for_comparison(self, col_value, value):
        if isinstance(col_value, int):
            return int(value)
        elif isinstance(col_value, float):
            return float(value)
        else:
            return value
        
    def format_select_output(self, row):
        return ", ".join(f"{key}: {value}" for key, value in row.items())
    
    def check_condition(self, row, col_name, operator, value):
        if col_name in row:
            col_value = row[col_name]

            if operator == '=' and col_value == value:
                return True
            elif operator == '>' and col_value > value:
                return True
            elif operator == '<' and col_value < value:
                return True

        return False

def insert_chunk_to_database(db, table_name, chunk):
    for index, row in chunk.iterrows():
        db.insert_data(table_name, list(row))

def main():
    db = Database(data_model='csv')
    table_name = 'movies'
    db.create_table(table_name, ['budget', 'genres', 'id', 'keywords', 'original_language', 
                                'original_title', 'overview', 'popularity', 'production_companies', 
                                'production_countries', 'release_date', 'revenue', 'runtime', 
                                'spoken_languages', 'status', 'title', 'vote_average', 'vote_count', 
                                'movie_id', 'cast', 'crew'])
    if not db.tables.get(table_name):  # 如果表不存在或为空
        db.create_table(table_name, [...])  # 列定义

        movies_df = pd.read_csv('tmdb_movie_metadata_cleaned.csv')
        movies_data = movies_df.values.tolist()

        db.batch_insert_data(table_name, movies_data)

if __name__ == "__main__":
    main()

    db = Database(data_model='csv')
    while True:
        user_input = input('MyDB > ')
        result = db.execute_query(user_input)

        if result == 'Exiting...':
            break

        print(result)
import random
import string

# 修改随机数据生成函数，使 t_name 的长度随机
def generate_random_data_variable_length(num_entries):
    data = []
    for _ in range(num_entries):
        id = random.randint(1, 100)  # 假设ID范围在1到100之间
        # 生成随机长度（5到20）的随机字符串
        t_name_length = random.randint(0, 20)
        t_name = ''.join(random.choices(string.ascii_uppercase + string.digits, k=t_name_length))
        col1 = random.randint(1, 100)  # 假设col1的值在1到100之间
        col2 = random.randint(1, 100)  # 假设col2的值在1到100之间
        data.append((id, t_name, col1, col2))
    return data

# 生成80条数据
random_data_variable_length = generate_random_data_variable_length(80)

# 格式化为SQL INSERT语句
insert_statements_variable_length = [
    f"INSERT INTO update_table_1 VALUES ({id}, '{t_name}', {col1}, {col2});"
    for id, t_name, col1, col2 in random_data_variable_length
]

insert_statements_variable_length



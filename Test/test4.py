import base64
from datetime import datetime, timedelta

# base64_str = 'LS0tLS1CRUdJTi'
# a = base64.b64decode(base64_str)
# print(str(a, 'utf-8'))
# print(datetime.now().strftime('%Y-%m-%d'))

with open('done.rdy', 'w') as file:
    file.write('done')
print('done.rdy 文件已成功创建！')
lst = ['ADS_CUST_ALGNMNT_TIER_VW_20250206.json', 'ADS_CUST_ALGNMNT_VW_20250206.json', 'call_objective_20250206.json','ecdp_data_20250206.json']
a1 = filter(lambda x: x.startswith('ADS_CUST_ALGNMNT_VW_'), lst)
print([x for x in a1])
# (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
pass








if __name__ == '__main__':
    pass

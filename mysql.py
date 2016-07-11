# 导入MySQL驱动:
#import mysql.connector
import pymysql.cursors






conn = pymysql.connect(host='rdscov8y5jpiumh0x51v0.mysql.rds.aliyuncs.com', user='transadmin', password='moon9876', database='transdb')
cursor = conn.cursor()



"""

>>> cursor.execute('create table user (id varchar(20) primary key, name varchar(20))')
# 插入一行记录，注意MySQL的占位符是%s:
>>> cursor.execute('insert into user (id, name) values (%s, %s)', ['1', 'Michael'])
>>> cursor.rowcount
1
# 提交事务:
>>> conn.commit()
>>> cursor.close()
# 运行查询:
>>> cursor = conn.cursor()
>>> cursor.execute('select * from user where id = %s', ('1',))
>>> values = cursor.fetchall()
>>> values
[('1', 'Michael')]
# 关闭Cursor和Connection:
>>> cursor.close()
True
>>> conn.close()

"""
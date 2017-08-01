#!/usr/bin/env python
#-*- coding:utf-8 -*-


from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy import MetaData
import sys

reload(sys)
sys.setdefaultencoding('utf8')


class DBTool(object):
    """检测MySQL 主从状态是否符合要求"""

    conn = None
    
    def __init__(self):
        pass

    def create_engine_str(self, username='root', password='root',
                          host='127.0.0.1', port=3306, database='',
                          charset='utf8'):
        """通过给的链接创建一个数据库链接串"""
        engine_str = ('mysql+mysqldb://{username}:{password}@{host}:{port}/{database}'
                      '?charset={charset}'.format(username = username,
                                                  password = password,
                                                  host = host,
                                                  port = port,
                                                  database = database,
                                                  charset = charset,))
        return engine_str

    def create_conn(self, username='root', password='root',
                       host='127.0.0.1', port=3306, database='',
                       charset='utf8'):
        """通过给的链接创建一个数据库Session"""
        self.engine_str = self.create_engine_str(username = username,
                                                  password = password,
                                                  host = host,
                                                  port = port,
                                                  database = database,
                                                  charset = charset)
        self.engine = create_engine(self.engine_str)
        # 创建DBSession类型:
        self.conn = self.engine.connect()
        return self.conn

    def execute(self, conn=None, sql=''):
        """执行SQL语句"""

        if conn:
            self.conn = conn

        rs = self.conn.execute(sql)
        return rs

    def fetch_all(self, sql='', extends={}):
        """获取所有的数据"""
        rs = self.execute(sql = sql)

        return self.to_dict(rs.keys(), rs.fetchall(), extends=extends)

    def to_dict(self, keys, rows, extends={}):
        """将给定的数据变成字典数组并返回
        Args
            keys: 和row对于的column name
            rows: 多行数据
        Return
            list
        Raise: None
        """
        datas = []
        for row in rows:
            data = dict(zip(keys, row))
            # 添加额外的字段值
            for key, value in extends.iteritems():
                data[key] = value

            datas.append(data)

        return datas

    def bind_table(self, table_name):
        """绑定一个需要执行DML 的表"""
        # 绑定表结构
        self.metadata = MetaData(self.conn)
        self.table = Table(table_name, self.metadata, autoload=True)

    def tosql(self, rows=[], table_name=''):
        """将数据保存到数据库指定的表中
        Args
            rows: list保存了多条dict
            table: 需要保存在哪个表中
        """
        self.bind_table(table_name)

        self.conn.execute(
            self.table.insert(),
            rows
        )
        


def main():
    pangu_conf = {
        'username': 'db_metric_user',
        'password': 'GcK6EeF7za2541c3XEt8',
        'host': 'xg-dal-c4-27',
        'port': 9863,
        'database': 'pangu-db_metric_group',
    }
    
    db_tool = DBTool()
    db_tool.create_conn(**pangu_conf)

    instance_sql = '''
        SELECT
            project,
            role,
            host,
            port
        FROM dashboard q
        WHERE q.role IN (
            'master_drc',
            'drc_master',
            'master',
            'master_master'
        );
    '''
 
    extends = {'name': 'HH', 'age': 29}
    data = db_tool.fetch_all(sql = instance_sql,
                             extends = extends)   
    print data

if __name__ == '__main__':
    main()

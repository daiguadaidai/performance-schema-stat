#!/usr/bin/env python
#-*- coding:utf-8 -*-

from db_tool import DBTool
import sys

reload(sys)
sys.setdefaultencoding('utf8')


class CollectionData(object):
    """检测MySQL 主从状态是否符合要求"""

    def __init__(self):
        pass

    def init_from_conn(self, host='127.0.0.1', port=3306, username='root',
                             password='root', database='', charset='utf8'):
        """创建读取数据数据源链接"""
        self.from_conf = {
            'username': username,
            'password': password,
            'host': host,
            'port': port,
            'database': database,
            'charset': charset,
        }

        self.from_db_tool = DBTool()
        self.from_db_tool.create_conn(**self.from_conf)

        return self.from_db_tool

    def init_to_conn(self, host='127.0.0.1', port=3306, username='root',
                           password='root', database='', charset='utf8'):
        """创建读取数据数据源链接"""
        self.to_conf = {
            'username': username,
            'password': password,
            'host': host,
            'port': port,
            'database': database,
            'charset': charset,
        }

        self.to_db_tool = DBTool()
        self.to_db_tool.create_conn(**self.to_conf)

        return self.from_db_tool

    def from2sql(self, sql = '', table='', extends={}):
        """通过给定的sql查询数据，并且将数据插入MySQL中
        Args
            sql: 需要查询的SQL
            table: 需要将数据插入到哪个表中
            extends: 指定某些字段的默认值
                extends = {'name': 'HH',
                            'age' : 27}
        """

        rows = self.from_db_tool.fetch_all(sql = sql, extends=extends)
        self.to_db_tool.tosql(rows=rows, table_name=table)


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

    source_extends = {'username': 'ken',
                       'password': 'guoguofei@dba123',
                       'database': 'performance_schema'}
    rows = db_tool.fetch_all(sql = instance_sql, extends = source_extends)

    for row in [rows[0]]:
        from_conf = {
            'username': row['username'],
            'password': row['password'],
            'host': row['host'],
            'port': row['port'],
            'database': row['database'],
        }

        to_conf = {
            'username': 'ken',
            'password': 'guoguofei@dba123',
            'host': '10.200.150.50',
            'port': 3306,
            'database': 'db_stat',
        }

        stat_sql = '''
            SELECT
                NULL AS id,
                CURRENT_TIMESTAMP() AS collection_time,
                object_type,
                object_schema,
                object_name,
                count_read,
                count_write,
                count_fetch,
                count_insert, 
                count_update, 
                count_delete
            FROM performance_schema.table_io_waits_summary_by_table
            WHERE OBJECT_SCHEMA NOT IN(
                'information_schema',
                'performance_schema',
                'mysql',
                'dbmonitor',
                'test',
                'sys',
                'drc_server_meta'
            );
        '''

        collection = CollectionData()
        collection.init_from_conn(**from_conf)
        collection.init_to_conn(**to_conf)

        collection.from2sql(sql=stat_sql, table='table_dml_stat', extends=row)

if __name__ == '__main__':
    main()

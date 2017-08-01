#!/usr/bin/env python
#-*- coding:utf-8 -*-

from db_tool import DBTool
from collection_data import CollectionData
from multiprocessing import Pool
import sys
import argparse
import time

reload(sys)
sys.setdefaultencoding('utf8')


def parse_args():
    """解析命令行传入参数"""
    usage = """
Example: python collection.py --interval=300 --thread=1
    """

    # 创建解析对象并传入描述
    parser = argparse.ArgumentParser(description = usage, 
                            formatter_class = argparse.RawTextHelpFormatter)

    # 添加 差集统计信息的间隔时间 参数
    parser.add_argument('--interval', dest='interval', required = True,
                        action='store', default='300', metavar='second',
                        help='collction stastics info interval time(s), default 300')

    # 添加 差集统计信息的间隔时间 参数
    parser.add_argument('--threads', dest='threads', required = True,
                        action='store', default='1', metavar='num',
                        help='paralle collection info thread num, default 1')

    args = parser.parse_args()

    return args

def get_instances():
    """获取所有需要采集的数据库实例数据源"""
    pangu_conf = {
        'username': 'root',
        'password': 'root',
        'host': '127.0.0.1',
        'port': 3306,
        'database': 'xxx',
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

    source_extends = {'username': 'root',
                      'password': 'root',
                      'database': 'performance_schema'}
    rows = db_tool.fetch_all(sql = instance_sql, extends = source_extends)

    return rows

def from2sql(from_conf, to_conf, sql, extends={}):
    """获取数据并且将数据插入数据库中"""

    collection = CollectionData()
    collection.init_from_conn(**from_conf)
    collection.init_to_conn(**to_conf)

    collection.from2sql(sql=sql, table='table_dml_stat', extends=extends)

def main():

    args = parse_args() # 解析传入参数
    
    p = Pool(processes = int(args.threads))

    start_timeint = int(time.time()) # 开始收集的时间
    start_timestamp = time.localtime(start_timeint)

    collection_count = 0 # 收集了多少次


    while True:
        instances = get_instances() # 获得需要收集的实例数据源信息

        for instance in instances: # 循环收集统计信息
            # 查询数据的实例数据源
            from_conf = {
                'username': instance['username'],
                'password': instance['password'],
                'host': instance['host'],
                'port': instance['port'],
                'database': instance['database'],
            }

            # 保存数据的实例数据源
            to_conf = {
                'username': 'root',
                'password': 'root',
                'host': '127.0.0.1',
                'port': 3306,
                'database': 'db_stat',
            }

            # 查询语句
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

            p.apply_async(from2sql, (from_conf, to_conf, stat_sql, instance))

        collection_count += 1 # 收集次数 +1

        while True: # 计算是否经过了间隔时间。是否是需要进行下一次搜集了
            time.sleep(1)

            end_timeint = int(time.time())
            end_timestamp = time.localtime(end_timeint)
            start_time = time.strftime('%Y-%m-%d %H:%M:%S', start_timestamp)
            end_time = time.strftime('%Y-%m-%d %H:%M:%S', end_timestamp)
            print start_time, end_time, collection_count

            if ((end_timeint - start_timeint) % int(args.interval) == 0 or
                (end_timeint - start_timeint) / int(args.interval) >= collection_count):
                
                break
           

if __name__ == '__main__':
    main()

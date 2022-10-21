import traceback
import time
import re
import sys
import os
from clickhouse_driver import connect
from loguru import logger
from time_util import get_time
from utils import alarm
from lineage import emit_table_to_table


def param_transfer(in_sql, in_params):
    for param in in_params:
        param_kv = param.split("=")
        if re.match(r"{.*}", param_kv[1]):
            param_tuple = param_kv[1].replace("{", "").replace("}", "").split(",")
            if param_tuple.__len__() == 3:
                time_param = get_time(time_format=param_tuple[0], unit=param_tuple[1], delta=int(param_tuple[2]))
            else:
                time_param = get_time(time_format=param_tuple[0])
            in_sql = in_sql.replace("${" + param_kv[0] + "}", time_param)
            logger.info(f"[param]  {param_kv[0]}={time_param}")
        else:
            in_sql = in_sql.replace(f"${param_kv[0]}", param_kv[1])
            logger.info(f"[param] {param_kv[0]}={param_kv[1]}")
    return in_sql


def create_report(job_name, begin, duration):
    return f"""INSERT INTO hho_analytics_performance.cronjob_dist
SELECT  '{job_name}'
        ,{begin}
        ,{duration}
;"""


if __name__ == "__main__":
    # 初始化配置
    args = sys.argv[1:]
    params = args[0:]
    sql_name = os.getenv('sql_path')
    clickhouse_host = os.getenv('ch_host')
    clickhouse_port = os.getenv('ch_port')
    clickhouse_user = os.getenv('ch_user')
    clickhouse_password = os.getenv('ch_password')
    clickhouse_database = os.getenv('ch_database')
    enable_lineage = os.getenv('enable_lineage')
    logger.add("./log/" + sql_name + "_{time:YYYYMMDDHHmmss}.log",
               format="[{level}]{time: YYYY-MM-DD HH:mm:ss} {message}",
               level="INFO", rotation="00:00")
    logger.info(f"[config] job_name={sql_name}")
    logger.info(f"[config] clickhouse_host={clickhouse_host}")
    logger.info(f"[config] clickhouse_port={clickhouse_port}")
    logger.info(f"[config] clickhouse_user={clickhouse_user}")
    logger.info(f"[config] clickhouse_database={clickhouse_database}")
    try:
        # 创建连接
        sql_file = open(f'/var/clickhouse-executor/{sql_name}.sql', 'r')
        ch_conn = connect(host=clickhouse_host,
                          port=clickhouse_port,
                          database=clickhouse_database,
                          user=clickhouse_user,
                          password=clickhouse_password)
        cursor = ch_conn.cursor()
        sql = sql_file.read()
        sql = param_transfer(sql, params)
        if enable_lineage == 'true':
            # 生成sql lineage
            cursor.execute('EXPLAIN AST\n' + sql)
            sql_ast = cursor.fetchall()
            up_streams = set(map(lambda x: x[0].replace('TableIdentifier', '').strip(), filter(lambda x: 'TableIdentifier' in x[0] and 'hho_analytics' in x[0], sql_ast)))
            # 注册lineage
            emit_table_to_table(up_streams, 'hho_analytics.' + sql_name + '_dist')
        else:
            logger.info('skip lineage analytics and register')
        # 执行sql
        start_time = time.time_ns()
        logger.info(f"[sql] sql:\n{sql}")
        cursor.execute(sql)
        end_time = time.time_ns()
        logger.info(f"[result] times: {(end_time - start_time) / 1000000} ms")
        cursor.execute(create_report(sql_name, start_time / 1000000000, (end_time - start_time) / 1000000))

    except Exception:
        message = f'clickhouse cronjob异常，job name: {sql_name}, exception: {traceback.format_exc()}'
        alarm(message)
        logger.error(message)

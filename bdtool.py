#!/usr/bin/python3

# MustBe PY36+
import os
import re
from collections import namedtuple
from functools import partial, wraps
from pathlib import Path

import argparse
import textwrap
import subprocess

from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed, ALL_COMPLETED, wait

CONFIG = None  # Auto Load Config And Set
BREAK_ERROR_MODE = True  # final, it is True
BREAK_OUTPUT_MODE = True # final, it is True (if stdin, you can global it in func, and set it to False)


class Interactive:
    def __init__(self,):
        global BREAK_OUTPUT_MODE
        BREAK_OUTPUT_MODE = False

    def __enter__(self,):
        ...
        
    def __exit__(self, *args, **kwargs):
        BREAK_OUTPUT_MODE = True

class C:
    FLAG_NUM = 50  # === per_side is 50

    @staticmethod
    def red(s):
        return f"\033[31m{s}\33[0m"

    @staticmethod
    def purple(s):
        return f"\033[35m{s}\33[0m"

    @staticmethod
    def green(s):
        return f"\033[32m{s}\33[0m"

    @staticmethod
    # import platform
    # if platform.system().lower() == "linux":
    def print_(content):
        print(f"\033[41;30m{content}\33[0m")  # \33[0m can close color

    @staticmethod
    def cprint(content, n=FLAG_NUM):
        l_diff_ = n - int(len(str(content)) / 2)
        r_diff_ = n - int(len(str(content)) / 2) + 1
        C.print_(f"\033[41;30m<{l_diff_ * '='}\33[0m"
                 f"\033[40;35m{content}\33[0m"
                 f"\033[41;30m{'=' * r_diff_}>\33[0m")


class Assign:
    @staticmethod
    def doc_(doc):
        return f'# {doc}\n'

    @staticmethod
    def int_(key, value):
        return f'{key} = {value}\n'

    @staticmethod
    def str_(key, value=""):
        return f'{key} = \"{value}\"\n'

    @staticmethod
    def list_(key, value=""):
        return f'{key} = [{value}]\n'

    @staticmethod
    def dict_(key):
        return f'{key} = {{}}\n'

    @staticmethod
    def none_(key):
        return f'{key} = None\n'

    @staticmethod
    def line_():
        return "\n"


class Cluster:

    CONFIG_NAME = ".config.py"

    DEV_CONFIG = dict(
        # cluster nodes
        CLUSTER_NODES=[],
        HOST_NAME="node",
        BASE_INDEX=1,
        NODE_COUNT=3,

        # bd
        JAVA_HOME="",
        ZK_HOME="",
        KAFKA_HOME="",
        HADOOP_HOME="",
        SPARK_HOME="",

        # db
        HIVE_HOME="",
        CK_HOME="",
        HBASE_HOME="",
        SQOOP_HOME="",

        # sync time server
        SYNC_SERVER="",

        # beeline
        BEELINE_PATH = "",
        BEELINE_HOST = "",
        BEELINE_PORT = "",
        BEELINE_USER = "",

        # thrift
        THRIFT_HOST = "",
        THRIFT_PORT = "",
        THRIFT_MASTER = "",
    )
    # CONFIG = CONFIG._replace(key=value) # also change value
    # raw_dict = CONFIG._asdict()  #  also reverse to raw_dict

    # Not Work in ClassMeta
    # NODES = (
    #     f"{HOST_NAME + str(i)}" for i in range(BASE_INDEX, NODE_COUNT + 1)
    # )
    # NODES = []
    # for i in range(BASE_INDEX, NODE_COUNT + 1):
    #     NODES.append(f'{HOST_NAME + str(i)}')

    GLOBAL_LOCK = Lock()  # must be out of for-loop

    def raw_run(self, command):
        if isinstance(command, str):
            shell_str = True
        elif isinstance(command, list):
            shell_str = False
        else:
            raise Exception("type of command must be str or List")

        output_pipeline = subprocess.PIPE if BREAK_OUTPUT_MODE == True else None
        err_pipeline = subprocess.PIPE if BREAK_ERROR_MODE == True else None
        

        result = subprocess.run(
            command,
            stdout=output_pipeline, # if BREAK_OUTPUT_MODE: None - don't break pipe;  else: Break Ouput (default)
            stderr=err_pipeline,    # if BREAK_ERROR_MODE: None - don't break pipe;   else: Break Raw Linux Error Msg(default)
            shell=shell_str,
            encoding="utf-8"
        )  # MayBe Version
        # print(result.stderr)
        # print(result.returncode)
        return result.stdout, result.returncode

    def run(self, command):
        result, returncode = self.raw_run(command)
        return result

    @staticmethod
    def get_path(env):
        return Path(os.environ[f'{env}'])

    @staticmethod
    def make_config_dict():
        regex = re.compile(r'(.*?)=(.*)')

        config_path = (Path.home() / f'{Cluster.CONFIG_NAME}')

        a = Assign()

        if not config_path.exists():
            with open(str(config_path), "w") as f:
                final_config_str = \
                    a.doc_('Must Config, eg: ["node1", "node2", "node3"]') + \
                    a.list_('CLUSTER_NODES', '"node1", "node2", "node3"') + \
                    a.doc_('HOST_NAME="node"') + \
                    a.doc_('BASE_INDEX=1') + \
                    a.doc_('NODE_COUNT=3') + \
                    a.line_() + \
                    a.doc_("BD") + \
                    a.str_("JAVA_HOME") + \
                    a.str_("ZK_HOME") + \
                    a.str_("KAFKA_HOME") + \
                    a.str_("HADOOP_HOME") + \
                    a.str_("SPARK_HOME") + \
                    a.line_() + \
                    a.doc_("DB") + \
                    a.str_("HIVE_HOME") + \
                    a.str_("CK_HOME") + \
                    a.str_("HBASE_HOME") + \
                    a.str_("SQOOP_HOME") + \
                    a.line_() + \
                    a.doc_("SYNC TIME") + \
                    a.str_('SYNC_SERVER', 'ntp4.aliyun.com') + \
                    a.line_() + \
                    a.doc_("Beeline") + \
                    a.doc_("    $HIVE_HOME/bin/beeline") + \
                    a.doc_("    $SPARK_HOME/bin/beeline") + \
                    a.str_('BEELINE_PATH', "$SPARK_HOME/bin/beeline") + \
                    a.str_('BEELINE_HOST', "node1") + \
                    a.str_('BEELINE_PORT', "10000") + \
                    a.str_('BEELINE_USER', "root") +\
                    a.line_() + \
                    a.doc_("Thrift(Spark)") + \
                    a.str_('THRIFT_HOST', "node1") + \
                    a.str_('THRIFT_PORT', "10000") + \
                    a.doc_("--master") + \
                    a.doc_("    local[*]") + \
                    a.doc_("    yarn") + \
                    a.str_('THRIFT_MASTER', "local[*]") 

                f.write(final_config_str)

        with open(str(config_path), "r") as f:
            for line in f:
                if line.strip().startswith("#") or not line.strip():
                    pass
                else:
                    result = regex.match(line.strip())
                    key = result.group(1).strip()
                    value = result.group(2).strip()
                    Cluster.DEV_CONFIG[key] = value

        global CONFIG

        add_name = namedtuple(
            "CONFIG",
            Cluster.DEV_CONFIG.keys()  # add keys
        )

        CONFIG = add_name._make(Cluster.DEV_CONFIG.values())  # add values and set to global CONFIG


class Scp(Cluster):
    def _run_for_scp(self, node=None, cmd=None):
        if isinstance(cmd, str):
            return self.raw_run("ssh" + " " + node + " " + cmd)
        else:
            return self.raw_run(["ssh"] + [node] + cmd)

    def scp_async(self, file_list, msg=""):
        master = eval(CONFIG.CLUSTER_NODES)[0]
        workers = eval(CONFIG.CLUSTER_NODES)[1:]

        jobs = [
            (file, work) for file in file_list
            for work in workers
        ]
        executor = ThreadPoolExecutor(max_workers=len(jobs))

        def _scp_callback(future_, master_=None, worker_=None, msg_=""):
            with Cluster.GLOBAL_LOCK:
                # cprint(f'')
                _, code = future_.result()  # returncode = 1  # last cmd fail
                if code == 1:
                    worker_str = f"[{worker_}]"
                    err_msg = f"{msg_}\t# No Such File In {worker_}"

                    print(f"{C.red('[Failed ]')} \t"
                          f"{C.red(worker_str)} \t* "
                          f"{C.red(err_msg)}")

                else:
                    m_w_str = f'[{master_} => {worker_}]'
                    suc_msg = f'{msg_}'
                    print(f"{C.green('[Succeed]')} \t"
                          f"{C.green(m_w_str)} \t* "
                          f"{C.purple(suc_msg)}")

        wrong_file_print = {}

        for filename, worker in jobs:
            if Path(filename).exists():
                abs_dir = Path(filename).resolve()

                cmd = f'scp -r {abs_dir} {worker}:{abs_dir.parent}'

                new_f = partial(self._run_for_scp, cmd=cmd)

                future = executor.submit(new_f, master)

                new_callback = partial(_scp_callback, master_=master, worker_=worker, msg_=f"{filename}")
                future.add_done_callback(new_callback)
            else:
                wrong_file_print.setdefault(filename)  # Since Py3+ dict is real ordered

        if wrong_file_print:
            print(f"{C.red('[File Not Found]:')}")
            for file_name_ in [*wrong_file_print.keys()]:
                err_file = f'\t\t * {file_name_}'
                print(f"{C.red(err_file)}")


class Run(Cluster):

    def a_run(self, cmd, msg=""):
        for node in eval(CONFIG.CLUSTER_NODES):
            if isinstance(cmd, str):
                C.cprint(node)
                print(self.run("ssh" + " " + node + " " + cmd))
                print(msg)
            else:
                C.cprint(node)
                print(self.run(["ssh"] + [node] + cmd))
                print(msg)

    def _callback(self, future, node=None, msg=""):
        with Cluster.GLOBAL_LOCK:
            C.cprint(node)
            print(future.result())
            print(msg)

    def _run_for_async(self, node=None, cmd=None):
        if isinstance(cmd, str):
            return self.run("ssh" + " " + node + " " + cmd)
        else:
            return self.run(["ssh"] + [node] + cmd)

    def async_run(self, cmd, msg=""):

        executor = ThreadPoolExecutor(max_workers=CONFIG.NODE_COUNT + 2)
        new_f = partial(self._run_for_async, cmd=cmd)

        # Others:
        # futures = []
        # futures.append(future)
        # wait(futures, return_when=ALL_COMPLETED) # join, until all futures complete   # others: FIRST_COMPLETED
        # executor.shutdown()

        for node in eval(CONFIG.CLUSTER_NODES):
            future = executor.submit(new_f, node)

            new_callback = partial(self._callback, node=node, msg=msg)
            future.add_done_callback(new_callback)


class BaseAction(argparse.Action):
    def __init__(self,
                 option_strings,
                 dest=argparse.SUPPRESS,
                 default=argparse.SUPPRESS,
                 help=None):
        super(BaseAction, self).__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            nargs=0,
            help=help)

    def __call__(self, parser, namespace, values, option_string=None):
        self._common_action()
        parser.exit()

    def _common_action(self):
        pass


class _PingAction(BaseAction, Run):

    def _common_action(self):
        self.ping_async()

    def ping_async(self):
        master = eval(CONFIG.CLUSTER_NODES)[0]
        workers = eval(CONFIG.CLUSTER_NODES)[1:]
        executor = ThreadPoolExecutor(max_workers=len(workers))

        def _ping_callback(future_, master_=None, worker_=None):
            with Cluster.GLOBAL_LOCK:
                _, code = future_.result()
                # failed
                if code != 0:
                    m_w_str = f'[{master_} => {worker_}]'
                    err_msg = f"""Could Not Resolve or Other Nodes Try 'ssh-copy-id {worker_}' ?"""

                    print(f"{C.red('[Failed ]')} \t"
                          f"{C.red(m_w_str)} \t* "
                          f"{C.red(err_msg)}")
                # succeed
                else:
                    m_w_str = f'[{master_} => {worker_}]'
                    suc_msg = f'Succeed Connected Test!'
                    print(f"{C.green('[Succeed]')} \t"
                          f"{C.green(m_w_str)} \t* "
                          f"{C.purple(suc_msg)}")

        for worker in workers:
            future = executor.submit(self._run_for_ping, worker)
            new_callback = partial(_ping_callback, master_=master, worker_=worker)
            future.add_done_callback(new_callback)

    def _run_for_ping(self, node=None):
        return self.raw_run(f"ssh -o ConnectTimeout=10 {node} echo")


class _KillaAction(BaseAction, Run):

    def _common_action(self):
        self.a_run(
            r'''"jps | grep -ive 'jps\|=\|^$'  | awk '{print \$1}' | xargs -n1 kill -9 2>/dev/null"''',
            "Killing ......"
        )
        self.a_run("jps")


class _TimeSync(BaseAction, Run):
    def _common_action(self):
        self.a_run(
            f"ntpdate {CONFIG.SYNC_SERVER}",
            msg="Sync Cluster Time ......"
        )


def main():
    Cluster.make_config_dict()  # Cluster.CONFIG_DICT['xxx']  (Global Auto Init)
    r = Run()
    s = Scp()

    """
    ... will be better
    """
    ak_path = Cluster.get_path("KAFKA_HOME")
    spark_path = Cluster.get_path("SPARK_HOME")


    def beeline_common(arg_from):
        """
            arg_from:
                hive:  args.ahive
                    or
                spark: args.ark

            CONFIG.BEELINE_PATH:
                $HIVE_HOME/bin/beeline
                    or
                $SPARK_HOME/bin/beeline
        """


        command = f'{eval(CONFIG.BEELINE_PATH)} -u jdbc:hive2://{eval(CONFIG.BEELINE_HOST)}:{eval(CONFIG.BEELINE_PORT)} -n {eval(CONFIG.BEELINE_USER)}'
        with Interactive():
            # beeline no log (for query)
            if arg_from == ["bee"]:
                command += " --hiveconf hive.server2.logging.operation.level=NONE"
            # beeline with log (default)
            else:
                ...
            
            print(command)
            os.system(command)
        # CONFIG.BEELINE_PATH
            # $HIVE_HOME/bin/beeline
            # or
            # $SPARK_HOME/bin/beeline
        command = f'{eval(CONFIG.BEELINE_PATH)} -u jdbc:hive2://{eval(CONFIG.BEELINE_HOST)}:{eval(CONFIG.BEELINE_PORT)} -n {eval(CONFIG.BEELINE_USER)}'
        with Interactive():
            # beeline no log (for query)
            if arg_from == ["bee"]:
                command += " --hiveconf hive.server2.logging.operation.level=NONE"
            # beeline with log (default)
            else:
                ...
            
            print(command)
            os.system(command)


    # prefix_chars='a' replace "-" and "--"
    parser = argparse.ArgumentParser(
        prefix_chars='a',
        prog='LIN',
        # formatter_class=argparse.RawDescriptionHelpFormatter,
        formatter_class=argparse.RawTextHelpFormatter,
        usage="",
        description=textwrap.indent(r'''
        ┌───────────────Must Be Python3.6+───────────┐
        │ 1. make sure config your hosename          │
        │────────────────────────────────────────────│
        │ >> python lin.py aa ping -c 3 127.0.0.1    │
        │ >> python lin.py as ping -c 3 127.0.0.1    │
        └────────────────────────────────────────────┘''', " ")
    )

    parser.add_argument('aping', dest="aping", action=_PingAction,
                        help="Check SSH         master -> workers")


    parser.add_argument('atime', dest="atime", action=_TimeSync,
                        help="Sync Time         For All Cluster")

    parser.add_argument('akill', dest="akill", action=_KillaAction,
                        help="Kill JPS App      For All Cluster")

    parser.add_argument('aa', dest="aa", nargs='*', type=str,
                        help="Run SH            For All Cluster")

    parser.add_argument('as', dest="as_", nargs='*', type=str,
                        help="Run SH Async      For All Cluster Async")

    parser.add_argument('ap', dest="ap", nargs='+', type=str,
                        help="Scp Async:        master -> workers")


    parser.add_argument('azk', dest="azk", nargs=1, type=str,
                        help="Start|Status|Stop Zookeeper For All Cluster")

    parser.add_argument('ack', dest="ack", nargs=1, type=str,
                        help="Start|Status|Stop ClickHouse For All Cluster")

    parser.add_argument('ak', dest="ak", nargs="+", type=str,
                        help=textwrap.indent(
    """Start|Stop & Consumer|Producer & CURD Topic:
┌────────────────────────────────
│start:       fa ak start
│stop:        fa ak stop
│────────────────────────────────
│c(consumer): fa ak c <topic>
│p(producer): fa ak p <topic>
│────────────topic───────────────
│create:      fa ak create <topic> <part_num> <rep_num>
│desc:        fa ak desc <topic>
│delete:      fa ak delete <topic>
│list:        fa ak list
└────────────────────────────────
""","")
    )

    parser.add_argument('ahive', dest="ahive", nargs=1, type=str,
                        help=textwrap.indent(
    """Start|Stop Hive MetaStore & hiveserver2:
┌────────────metastore───────────
│start:       fa ahive start
│stop:        fa ahive stop
│────────────hiveserver2─────────
│start:       fa ahive start2
│stop:        fa ahive stop2
│────────────beeline─────────────
│bee(no log): fa ahive bee
│beeline:     fa ahive beeline
└────────────────────────────────
""","")
    )

    parser.add_argument('ark', dest="ark", nargs=1, type=str,
                        help=textwrap.indent(
    """Start|Stop Spark & Thrift Service:
┌────────────spark───────────────
│start:       fa ark start
│stop:        fa ark stop
│───────spark thrift service─────
│start:       fa ark thstart
│stop:        fa ark thstop
│────────────beeline─────────────
│bee(no log): fa ark bee
│beeline:     fa ark beeline
└────────────────────────────────
""","")
    )


    args = parser.parse_args()  # Namespace(args1=['option1',...], args2=['option2',...])

    # All
    if args.aa:
        r.a_run(args.aa)

    # All (Async)
    elif args.as_:
        r.async_run(args.as_)  # avoid conflict as(Python) as->as_

    # Zookeeper
    elif args.azk:
        if args.azk in [["start"], ['status'], ["stop"]]:
            zk_path = Cluster.get_path("ZK_HOME")
            r.a_run(f'{zk_path / "bin/zkServer.sh"} {args.azk[0]}')
        else:
            parser.print_help()
    
    # Kafka
    elif args.ak:
        if len(args.ak) == 1:
            if args.ak == ["start"]:
                r.a_run(
                    f'{ak_path / "bin/kafka-server-start.sh"} -daemon {ak_path / "config/server.properties"}',
                    msg="Starting Kafka ......"
                )
            elif args.ak == ["stop"]:
                r.a_run(
                    f'{ak_path / "bin/kafka-server-stop.sh"} {args.ak[0]}',
                    msg="Stopping Kafka ......"
                )
            # list topics
            elif args.ak[0] == "list":
                result = r.run( 
                    f'{ak_path / "bin/kafka-topics.sh --bootstrap-server"} ' \
                    + ",".join(node+":9092" for node in eval( CONFIG.CLUSTER_NODES)) + " " \
                    + "--list"
                )
                print(result)
            else:
                parser.print_help()

        elif len(args.ak) > 1:
            # consumer
            result = ""

            if args.ak[0] == "c":
                # global BREAK_OUTPUT_MODE
                # BREAK_OUTPUT_MODE = False
                with Interactive():
                    result = r.run( 
                        f'{ak_path / "bin/kafka-console-consumer.sh --bootstrap-server"} ' \
                        + ",".join(node+":9092" for node in eval( CONFIG.CLUSTER_NODES)) + " " \
                        + "--topic" + " " \
                        + args.ak[1]
                    )
                # BREAK_OUTPUT_MODE = True
                
            # producer
            elif args.ak[0] == "p":
                result = r.run( 
                    f'{ak_path / "bin/kafka-console-producer.sh --broker-list"} ' \
                    + ",".join(node+":9092" for node in eval( CONFIG.CLUSTER_NODES)) + " " \
                    + "--topic" + " " \
                    + args.ak[1]
                )

            # create one topic
            # kafka-topics.sh --create --bootstrap-server node1:9092 --topic first_xxx --partitions 2 --replication-factor 3
            elif args.ak[0] == "create":
                result = r.run( 
                    f'{ak_path / "bin/kafka-topics.sh --bootstrap-server"} ' \
                    + ",".join(node+":9092" for node in eval( CONFIG.CLUSTER_NODES)) + " " \
                    + "--create" + " " \
                    + "--topic" + " " \
                    + args.ak[1] + " " \
                    + "--partitions" + " " \
                    + args.ak[2] + " " \
                    + "--replication-factor" + " " \
                    + args.ak[3]
                )

            # describe one topic
            elif args.ak[0] == "desc":
                result = r.run( 
                    f'{ak_path / "bin/kafka-topics.sh --bootstrap-server"} ' \
                    + ",".join(node+":9092" for node in eval( CONFIG.CLUSTER_NODES)) + " " \
                    + "--describe" + " " \
                    + "--topic" + " " \
                    + args.ak[1]
                )
            # delete one topic
            elif args.ak[0] == "delete":
                result = r.run( 
                    f'{ak_path / "bin/kafka-topics.sh --bootstrap-server"} ' \
                    + ",".join(node+":9092" for node in eval( CONFIG.CLUSTER_NODES)) + " " \
                    + "--delete" + " " \
                    + "--topic" + " " \
                    + args.ak[1]
                )
            print(result)
            print()
        
    # ClickHouse
    elif args.ack:
        if args.ack in [["start"], ['status'], ["stop"]]:
            r.a_run(
                f'systemctl {args.ack[0]} clickhouse-server',
                msg=f"{args.ack[0].title()}ing ClickHouse ......" if args.ack[0] != "status" else ""

            )
        else:
            parser.print_help()

    # Hive(Master)
    elif args.ahive:

        # metastore
        if args.ahive == ["start"]:
            r.run(
                r'''/usr/bin/nohup $HIVE_HOME/bin/hive --service metastore > $HIVE_HOME/logs/hivemetastore-$(/bin/date '+%Y-%m-%d-%H-%M-%S').log 2>&1 &''',
            )
            print("Starting MetaStore ......")
        elif args.ahive == ["stop"]:
            r.run(
                r'''ps -ef | grep metastore | grep -v grep | awk '{print $2}' | xargs -n1 kill -9'''
            )
            print("Stopping MetaStore ......")

        # hiveserver2
        elif args.ahive == ["start2"]:
            r.run(
                r'''/usr/bin/nohup $HIVE_HOME/bin/hive --service hiveserver2 > $HIVE_HOME/logs/hiveserver2-$(/bin/date '+%Y-%m-%d-%H-%M-%S').log 2>&1 &''',
            )
            print("Starting hiveserver2 ......")
        elif args.ahive == ["stop2"]:
            r.run(
                r'''ps -ef | grep hiveserver2 | grep -v grep | awk '{print $2}' | xargs -n1 kill -9'''
            )
            print("Stopping hiveserver2 ......")

        # beeline
        elif args.ahive in [ ["beeline"], ["bee"] ]:
            beeline_common(args.ahive)

        else:
            parser.print_help()

    # Spark
    elif args.ark:

        # spark service
        if args.ark == ["start"]:
            ...

            # r.run(
            #     r'''/usr/bin/nohup $HIVE_HOME/bin/hive --service metastore > $HIVE_HOME/logs/hivemetastore-$(/bin/date '+%Y-%m-%d-%H-%M-%S').log 2>&1 &''',
            # )
            # print("Starting MetaStore ......")
        elif args.ark == ["stop"]:
            ...

            # r.run(
            #     r'''ps -ef | grep metastore | grep -v grep | awk '{print $2}' | xargs -n1 kill -9'''
            # )
            # print("Stopping MetaStore ......")

        # thrift
        elif args.ark == ["thstart"]:
            command = f'{spark_path}/sbin/start-thriftserver.sh '
            f'--hiveconf hive.server2.thrift.bind.host={CONFIG.THRIFT_HOST} '
            f'--hiveconf hive.server2.thrift.port={CONFIG.THRIFT_PORT} '
            f'--master {CONFIG.THRIFT_MASTER} '
            r.run(
                command
            )
            print("Starting Spark Thrift Server ......")

        elif args.ark == ["thstop"]:
            command = f'{spark_path}/sbin/stop-thriftserver.sh'
            r.run(
                command
            )
            print("Stopping Spark Thrift Server ......")

        # beeline
        elif args.ark in [ ["beeline"], ["bee"] ]:
            beeline_common(args.ark)

        else:
            parser.print_help()


    # Hadoop




    # scp (Async)
    elif args.ap:  # filename_list
        s.scp_async(args.ap)


if __name__ == '__main__':
    main()

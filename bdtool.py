#!/usr/bin/python3

# MustBe PY36+
import os
from functools import partial, wraps
from pathlib import Path

import configparser
import argparse
import textwrap
import subprocess

from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed, ALL_COMPLETED, wait

CONFIG = None  # Auto Load Config And Set
BREAK_ERROR_MODE = True  # final, it is True
BREAK_OUTPUT_MODE = True # final, it is True (if stdin, you can global it in func, and set it to False)



parser = configparser.ConfigParser()
config_name = ".config.cfg"
config_path = (Path.home() / config_name)

# for default and new stack
default_map = {
    # key is section for config
    "group0": {"cluster": True},
    "group1": {"cluster": True},

    "ping": {"cluster": True, "least_one": True},
    "run": {"cluster": True, "least_one": True},
    "scp": {"cluster": True, "least_one": True},
    "kill": {"cluster": True, "least_one": True},
    "time": {"cluster": True, "least_one": True},

    "java": {
        "path": "JAVA_HOME",
        "cluster": False
    },
    "zookeeper": {
        "path": "ZK_HOME",
        "cluster": True
    },
    "kafka": {
        "path": "KAFKA_HOME",
        "cluster": True
    },
    "hadoop": {
        "path": "HADOOP_HOME",
        "cluster": True
    },
    "spark": {
        "path": "SPARK_HOME",
        "cluster": True
    },
    "hive": {
        "path": "HIVE_HOME",
        "cluster": False
    },
    "clickhouse": {
        "path": "CK_HOME",
        "cluster": False
    },
    "hbase": {
        "path": "HBASE_HOME",
        "cluster": True
    },
    "flume": {
        "path": "FLUME_HOME",
        "cluster": True
    },
    "airflow": {
        "path": "AIRFLOW_HOME",
        "cluster": True
    },
    "maxwell": {
        "path": "MAXWELL_HOME",
        "cluster": True
    },
    "sqoop": {
        "path": "SQOOP_HOME",
        "cluster": False
    },

    # "beeline": {
    #     "path": "",
    # },
    # "thrift(spark)": {
    #     "path": "",
    # },
    # "spark-submit": {
    #     "path": "",
    # }

}

if not Path(config_path).exists():
    specified_nodes = ["cluster", "ping", "run", "sync", "kill", "time", "zookeeper", "kafka", "hadoop", "spark",
                       "hive", "clickhouse", "hbase", "sqoop", "airflow", "maxwell", "flume"]

    for key in default_map:
        parser[key] = {}
        value = default_map[key]
        if value.get("path"):
            parser[key]["#"] = f">> option {value['path']} default is ${value['path']}"

        if value["cluster"] is True:
            if key.startswith("group"):
                if key == "group0":
                    besides_keys = [_key for _key in default_map if _key != key and default_map[_key]["cluster"] is True \
                                    and default_map[_key].get("least_one")]

                    parser[key][";;)"] = f"at least 2 nodes need be set for below section"
                    parser[key]["#1)"] = f"section_name and nodes must be specified in groups"
                    parser[key][
                        "#2)"] = f"append section_name here; and you can define more section: [group2], [group3], ..."
                    parser[key]["#3)"] = f"if section is in multi groups, options will be 'union all(keep raw sort)' for every section"
                else:
                    parser[key]["#"] = f"if you want to add local mode: you can set them null like: nodes= "
                    besides_keys = [_key for _key in default_map if _key != key and default_map[_key]["cluster"] is True \
                                    and not default_map[_key].get("least_one") \
                                    and not _key.startswith("group")]


                parser[key]["section_names"] = ", ".join(besides_keys)
                parser[key]["nodes"] = "node1, node2, node3"

    parser["time"]["sync_server"] = "ntp4.aliyun.com"

    with Path(config_path).open("w") as f:
        parser.write(f)
else:
    parser.read(config_path)



def get_value(section="", option=""):
    return parser.get(section, option, fallback=None)


def get_home(section="", option=""):
    default = f"${option}"
    result = parser.get(section, option, fallback=default)
    return default if result == "" else result


def get_groups():
    return [sec for sec in parser.sections() if sec.startswith("group")]


def split_value(config_value_str, sep=","):
    return [*map(str.strip, config_value_str.split(sep))]


def get_section_nodes_map():
    section_2_nodes = {}


    for group in get_groups():
        try:
            section_names_value = parser.get(group, "section_names")
            nodes_value = parser.get(group, "nodes")
        except configparser.NoOptionError:
            raise Exception(f"missing <section_names> or <nodes> in {group}")

        section_names = split_value(section_names_value)
        nodes = split_value(nodes_value)

        for section in section_names:
            old_value = section_2_nodes.setdefault(section, nodes)  # first return new value(that be set)

            raw_list = old_value + nodes
            shuffle_list = list(set(raw_list))
            shuffle_list.sort(key=lambda x: raw_list.index(x))

            section_2_nodes[section] = shuffle_list

    # sort(key=lambda x:a.index(x))

    return section_2_nodes


# section_to_nodes = get_section_nodes_map()

def is_local_or_get_nodes(section):
    section_to_nodes = get_section_nodes_map()
    assert section in section_to_nodes, f"{section} is not in in {str([*section_to_nodes.keys()])}"
    # beside {key :[""]}
    if section in section_to_nodes and section_to_nodes[section] != [""]:
        return section_to_nodes[section]
    else:
        return True

# if is_local_or_get_nodes("ping") is True:
#
# else:
#     nodes = is_local_or_get_nodes("ping")

def get_nodes_for_service(section_name):
    judge_result = is_local_or_get_nodes(section_name)
    if judge_result is True:
        import socket
        # just local (one node)
        return [socket.gethostname()]
    else:
        return judge_result


class Interactive:
    def __init__(self,):
        global BREAK_OUTPUT_MODE
        BREAK_OUTPUT_MODE = False

    def __enter__(self,):
        ...

    def __exit__(self, *args, **kwargs):
        BREAK_OUTPUT_MODE = True

class InteractiveALL:
    def __init__(self,):
        global BREAK_ERROR_MODE
        global BREAK_OUTPUT_MODE
        BREAK_ERROR_MODE = False
        BREAK_OUTPUT_MODE = False

    def __enter__(self,):
        ...

    def __exit__(self, *args, **kwargs):
        BREAK_ERROR_MODE = True
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


class Cluster:
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
    def get_nodes(section_name):
        judge_result = is_local_or_get_nodes(section_name)
        if judge_result is True:
            raise Exception(f"{section_name} at least 2 nodes")
        else:
            return judge_result

    @staticmethod
    def get_path(env):
        return Path(os.environ[f'{env}'])


class Scp(Cluster):
    def _run_for_scp(self, node=None, cmd=None):
        if isinstance(cmd, str):
            return self.raw_run("ssh" + " " + node + " " + cmd)
        else:
            return self.raw_run(["ssh"] + [node] + cmd)

    def scp_async(self, file_list, msg=""):
        nodes = self.get_nodes("scp")

        master = nodes[0]
        workers = nodes[1:]

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

    def cluster_run(self, cmd, section, msg=""):
        nodes = self.get_nodes(section)

        for node in nodes:
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
        nodes = self.get_nodes("run")

        executor = ThreadPoolExecutor(max_workers=len(nodes) + 2)
        new_f = partial(self._run_for_async, cmd=cmd)

        # Others:
        # futures = []
        # futures.append(future)
        # wait(futures, return_when=ALL_COMPLETED) # join, until all futures complete   # others: FIRST_COMPLETED
        # executor.shutdown()

        for node in nodes:
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
        nodes = self.get_nodes("ping")

        master = nodes[0]
        workers = nodes[1:]
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
        self.cluster_run(
            r'''"jps | grep -ive 'jps\|=\|^$'  | awk '{print \$1}' | xargs -n1 kill -9 2>/dev/null"''',
            section="kill",
            msg="Killing ......"
        )
        self.cluster_run("jps", section="kill")


class _TimeSync(BaseAction, Run):
    def _common_action(self):
        self.cluster_run(
            f"ntpdate {get_value('time', 'sync_server')}",
            section="time",
            msg="Sync Cluster Time ......"
        )

def main():
    java_home = get_home("java", "JAVA_HOME")
    zk_home = get_home("zookeeper", "ZK_HOME")
    kafka_home = get_home("kafka", "KAFKA_HOME")
    hadoop_home = get_home("hadoop", "HADOOP_HOME")
    spark_home = get_home("spark", "SPARK_HOME")
    hive_home = get_home("hive", "HIVE_HOME")
    ck_home = get_home("clickhouse", "CK_HOME")
    hbase_home = get_home("hbase", "HBASE_HOME")
    sqoop_home = get_home("sqoop", "SQOOP_HOME")
    airflow_home = get_home("airflow", "AIRFLOW_HOME")
    maxwell_home = get_home("maxwell", "MAXWELL_HOME")
    flume_home = get_home("flume", "FLUME_HOME")


    r = Run()
    s = Scp()

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


    def submit_common(arg_from=None, type=""):
        """
            arg_from: args.arks
            type    : "scala" or "python"
        """

        command = f'{spark_path}/bin/spark-submit '

        if eval(CONFIG.master).strip() != "yarn":
            command += f'--master {CONFIG.master} '
        else:
            command += f'--master {CONFIG.master} '
            if eval(CONFIG.deploy_mode).strip():
                command += f'--deploy-mode {CONFIG.deploy_mode} '

        if eval(CONFIG.driver_memory).strip():
            command += f'--driver-memory {CONFIG.driver_memory} '
        if eval(CONFIG.executor_memory).strip():
            command += f'--executor-memory {CONFIG.executor_memory} '
        if eval(CONFIG.executor_cores).strip():
            command += f'--executor-cores {CONFIG.executor_cores} '

        # python or submit scala/java
        if type == "python":
            if eval(CONFIG.py_files).strip():
                command += f'--py-files {CONFIG.py_files} '

        if type == "scala":
            if eval(CONFIG.class_of_jar).strip():
                command += f'--class {CONFIG.class_of_jar} '

        command += " ".join( args.arks )
        print(command)

        with InteractiveALL():
            print ( r.run( command ) )


    # prefix_chars='a' replace "-" and "--"
    parser = argparse.ArgumentParser(
        prefix_chars='a',
        prog='LIN',
        # formatter_class=argparse.RawDescriptionHelpFormatter,
        formatter_class=argparse.RawTextHelpFormatter,
        usage="",
        description=textwrap.indent(r'''
        ┌───────────────Must Be Python3.6+───────────┐
        │ All Params Can Adjust In ->  ~/.config.py  │
        │────────────────────────────────────────────│
        │ >> fa ah                                   │
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

    parser.add_argument('arks', dest="arks", nargs="+", type=str,
                        help=textwrap.indent(
    """spark-submit: conf in ~/.config.py
┌────────────spark-submit────────
│.jar|.py     fa arks xxx.jar ...
└────────────────────────────────
""","")
    )
    parser.add_argument('air', dest="air", nargs=1, type=str,
                        help=textwrap.indent(
    """\
┌────────────Airflow─────────────
│fa air start|status|stop|list|wlist|slist
└────────────────────────────────
""","")
    )

    parser.add_argument('am', dest="am", nargs="+", type=str,
                        help=textwrap.indent(
    """\
┌────────────Maxwell─────────────
│fa am start|stop|restart
│fa am bootstrap <db> <table>
└────────────────────────────────
""","")
    )

    args = parser.parse_args()  # Namespace(args1=['option1',...], args2=['option2',...])

    # All
    if args.aa:
        r.cluster_run(args.aa, section="run")

    # All (Async)
    elif args.as_:
        r.async_run(args.as_)  # avoid conflict as(Python) as->as_

    # Zookeeper
    elif args.azk:
        if args.azk in [["start"], ['status'], ["stop"]]:
            r.cluster_run(
                f'{zk_home}/bin/zkServer.sh {args.azk[0]}',
                section="zookeeper"
            )
        else:
            parser.print_help()

    # Kafka
    elif args.ak:
        kafka_nodes = get_nodes_for_service("kafka")


        if len(args.ak) == 1:
            if args.ak == ["start"]:
                r.cluster_run(
                    f'{kafka_home}/bin/kafka-server-start.sh -daemon {kafka_home}/config/server.properties',
                    section="kafka",
                    msg="Starting Kafka ......"
                )
            elif args.ak == ["stop"]:
                r.cluster_run(
                    f'{kafka_home}/bin/kafka-server-stop.sh {args.ak[0]}',
                    section="kafka",
                    msg="Stopping Kafka ......"
                )
            # list topics
            elif args.ak[0] == "list":
                result = r.run(
                    f'{kafka_home}/bin/kafka-topics.sh --bootstrap-server ' \
                    + ",".join(node+":9092" for node in kafka_nodes ) + " " \
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
                        f'{kafka_home}/bin/kafka-console-consumer.sh --bootstrap-server ' \
                        + ",".join(node+":9092" for node in kafka_nodes) + " " \
                        + "--topic" + " " \
                        + args.ak[1]
                    )
                # BREAK_OUTPUT_MODE = True

            # producer
            elif args.ak[0] == "p":
                result = r.run(
                    f'{kafka_home}/bin/kafka-console-producer.sh --broker-list ' \
                    + ",".join(node+":9092" for node in kafka_nodes) + " " \
                    + "--topic" + " " \
                    + args.ak[1]
                )

            # create one topic
            # kafka-topics.sh --create --bootstrap-server node1:9092 --topic first_xxx --partitions 2 --replication-factor 3
            elif args.ak[0] == "create":
                result = r.run(
                    f'{kafka_home}/bin/bin/kafka-topics.sh --bootstrap-server ' \
                    + ",".join(node+":9092" for node in kafka_nodes) + " " \
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
                    f'{kafka_home}/bin/kafka-topics.sh --bootstrap-server ' \
                    + ",".join(node+":9092" for node in kafka_nodes) + " " \
                    + "--describe" + " " \
                    + "--topic" + " " \
                    + args.ak[1]
                )
            # delete one topic
            elif args.ak[0] == "delete":
                result = r.run(
                    f'{kafka_home}/bin/kafka-topics.sh --bootstrap-server ' \
                    + ",".join(node+":9092" for node in kafka_nodes) + " " \
                    + "--delete" + " " \
                    + "--topic" + " " \
                    + args.ak[1]
                )
            print(result)
            print()

    # ClickHouse
    elif args.ack:
        if args.ack in [["start"], ['status'], ["stop"]]:
            r.cluster_run(
                f'systemctl {args.ack[0]} clickhouse-server',
                section="clickhouse",
                msg=f"{args.ack[0].title()}ing ClickHouse ......" if args.ack[0] != "status" else ""
            )
        else:
            parser.print_help()

    # Hive(Master)
    elif args.ahive:

        # metastore
        if args.ahive == ["start"]:
            r.run(
                rf'''/usr/bin/nohup {hive_home}/bin/hive --service metastore > {hive_home}/logs/hivemetastore-$(/bin/date '+%Y-%m-%d-%H-%M-%S').log 2>&1 &''',
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
                rf'''/usr/bin/nohup {hive_home}/bin/hive --service hiveserver2 > {hive_home}/logs/hiveserver2-$(/bin/date '+%Y-%m-%d-%H-%M-%S').log 2>&1 &''',
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


    # spark-submit
    elif args.arks:
        # submit
        if args.arks[0].split(".")[-1] == "jar":
            command = submit_common(args.arks, "scala")

        #  pysubmit
        elif args.arks[0].split(".")[-1] == "py":
            command = submit_common(args.arks, "python")

        else:
            print("usage:")
            print("\tfile suffix must be .jar | .py")
            print()

    # Airflow
    elif args.air:
        # WebServer + Scheduler
        if args.air in [["start"], ['status'], ["stop"], ["list"], ["wlist"], ["slist"]]:
            if args.air == ["start"]:
                print(C.purple("[Starting]"))
                command = r"""airflow webserver -D && airflow scheduler -D"""
                with InteractiveALL():
                    r.run(command)

            elif args.air == ["status"]:
                print(C.purple("[Status]"))
                if r.run("ps aux | grep airflow | grep webserver | grep -v grep") \
                        and r.run("ps aux | grep airflow | grep scheduler | grep -v grep"):
                    print(f"\tAirflow is {C.green('running')}")
                else:
                    print(f"\tAirflow is {C.red('stopped')}")

            elif args.air == ["list"]:
                print(C.purple("[WebServer+Scheduler]"))
                command = "ps aux | grep -v grep | grep airflow"
                with InteractiveALL():
                    r.run(command)
            elif args.air == ["wlist"]:
                print(C.purple("[WebServer]"))
                command = "ps aux | grep -v grep | grep airflow | grep webserver "
                with InteractiveALL():
                    r.run(command)
            elif args.air == ["slist"]:
                print(C.purple("[Scheduler]"))
                command = "ps aux | grep -v grep  | grep airflow | grep scheduler"
                with InteractiveALL():
                    r.run(command)

            elif args.air == ["stop"]:
                print(C.purple("[Stopping]"))
                kill_pid = r"""ps aux | grep airflow | grep -v grep | awk '{print $2}' | xargs -n1 kill -9"""
                del_pid_file = rf"""rm -rf {airflow_home}/airflow-scheduler.pid && rm -rf {airflow_home}/airflow-webserver.pid && rm -rf {airflow_home}/airflow-webserver-monitor.pid"""
                r.run(kill_pid)
                r.run(del_pid_file)
                print(f"\tstopping airflow ......")

            else:
                parser.print_help()

        else:
            print("single service version in the future...")
        # Single WebServer
        # elif args.air in [["wstart"], ['wstatus'], ["wstop"]]:

        # Single Scheduler
        # elif args.air in [["sstart"], ['sstatus'], ["sstop"]]:


    # Maxwell
    elif args.am:
        if len(args.am) == 3: # fa am   bootstrap <db> <table>
            if args.am[0] in ["boot", "bootstrap"]:
                print(C.purple("[BootStrap]"))
                command = fr"""{maxwell_home}/bin/maxwell-bootstrap --config {maxwell_home}/config.properties --database {args.am[1]} --table {args.am[2]}"""
                with InteractiveALL():
                    r.run(command)
            else:
                parser.print_help()

        elif len(args.am) == 1:
            # if args.am in [["start"], ['stop'], ["restart"]]:
            if args.am == ["start"]:
                print(C.purple("[Starting]"))

                is_started = r.run( """ps aux | grep maxwell.Maxwell | grep -v grep | wc -l""" )
                if is_started.strip() == "1":
                    print(f"\tMaxwell is {C.green('running')}")
                else:
                    command = fr"""{maxwell_home}/bin/maxwell --config {maxwell_home}/config.properties --daemon"""
                    with InteractiveALL():
                        r.run(command)

            elif args.am == ["stop"]:
                print(C.purple("[Stopping]"))
                r.run(r"""ps aux | grep maxwell.Maxwell | grep -v grep | awk '{print $2}' | xargs -n1 kill -9""")
                print(f"\tstopping Maxwell ......")

            elif args.am == ["restart"]:
                print(C.purple("[Restart]"))
                r.run(r"""ps aux | grep maxwell.Maxwell | grep -v grep | awk '{print $2}' | xargs -n1 kill -9""")
                command = fr"""{maxwell_home}/bin/maxwell --config {maxwell_home}/config.properties --daemon"""
                with InteractiveALL():
                    r.run(command)
            else:
                parser.print_help()

        else:
            parser.print_help()



    # scp (Async)
    elif args.ap:  # filename_list
        s.scp_async(args.ap)


if __name__ == '__main__':
    main()

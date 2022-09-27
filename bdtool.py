#!/usr/bin/python3

# MustBe PY36+
import argparse
import configparser
import subprocess
import textwrap
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from threading import Lock


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

    "airflow": {
        "path": "AIRFLOW_HOME",
        "cluster": True
    },
    "flume": {
        "path": "FLUME_HOME",
        "cluster": True
    },

    "maxwell": {
        "path": "MAXWELL_HOME",
        "cluster": False
    },
    "sqoop": {
        "path": "SQOOP_HOME",
        "cluster": False
    },

    # "beeline": {"cluster": False},
    # "thrift(spark)": {
    #     "path": "",
    # },
    # "spark-submit": {
    #     "path": "",
    # }

}

if not Path(config_path).exists():
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

    parser["flume"]["#"] = ">> Warning! the job filename will be grep to kill(if stop flume by CLI), set the name as long as possible!"
    parser["flume"]["flume_job_config_path"] = "$FLUME_HOME/jobs/log_to_kafka.conf"
    parser["flume"]["agent_name"] = "a1"
    parser["flume"]["log_command"] = "tail -f $FLUME_HOME/logs/flume.log"
    parser["flume"]["log_start_command"] = "tail -f $FLUME_HOME/logs/start-flume.log"

    parser["hive"]["beeline_command"] = "$HIVE_HOME/bin/beeline -u jdbc:hive2://node1:10000 -n root --hiveconf hive.server2.logging.operation.level=NONE"


    parser["spark"]["THRIFT_HOST"] = "node1"
    parser["spark"]["THRIFT_PORT"] = "10000"
    parser["spark"]["THRIFT_MASTER"] = "local[*]"
    parser["spark"]["beeline_command"] = "$SPARK_HOME/bin/beeline -u jdbc:hive2://node1:10000 -n root --hiveconf hive.server2.logging.operation.level=NONE"


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
    return section_2_nodes


def is_local_or_get_nodes(section):
    section_to_nodes = get_section_nodes_map()
    # beside {key :[""]}
    if section in section_to_nodes and section_to_nodes.get(section) != [""]:
        return section_to_nodes[section]
    else:
        return True


def get_nodes(section_name):
    judge_result = is_local_or_get_nodes(section_name)
    if judge_result is True:
        import socket
        # just local (one node)
        return [socket.gethostname()]
    else:
        return judge_result

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


class s(str):
    def __init__(self, init: str):
        self.init = init

    def __or__(self, command: str):
        """ | """
        self.init = f"{self.init} | {command}"
        return self

    def __gt__(self, other):
        """ > """

        # return None, but directly print stdout and stderr
        # Note:
        # (for no break pipe) don't use universal_newlines=True
        # (for no break pipe) don't use encoding="utf-8"
        if other is None:
            only_return_code_obj = subprocess.run(self.init, shell=True)
            return only_return_code_obj.returncode
        else:
            sub_obj = subprocess.run(self.init, shell=True, universal_newlines=True, stderr=subprocess.PIPE,
                                 stdout=subprocess.PIPE)

            # return (stderr + stdout), but not print
            if other is Ellipsis:
                return sub_obj
            # only return stdout, not print
            elif other == 1:
                return sub_obj.stdout
            # only return stderr, not print
            elif other == 2:
                return sub_obj.stderr

    def __and__(self, other):
        """ & """
        # return None, but directly print stdout and stderr
        if other is None:
            subprocess.Popen(self.init, shell=True)

        else:
            sub_obj = subprocess.Popen(self.init, shell=True, universal_newlines=True, stderr=subprocess.PIPE,
                                   stdout=subprocess.PIPE)

            # return (stderr + stdout), but not print
            if other is Ellipsis:
                return sub_obj
            # only return stdout, not print
            elif other == 1:
                return sub_obj.stdout
            # only return stderr, not print
            elif other == 2:
                return sub_obj.stderr


class Run:
    GLOBAL_LOCK = Lock()  # must be out of for-loop

    def sync_cluster_run_without_return(self, cmd, section, msg=""):
        nodes = get_nodes(section)

        for node in nodes:
            C.cprint(node)
            # maybe out  scope " "
            s(f'ssh {node} {cmd}') > None
            print(msg)

    def sync_cluster_run_return(self, cmd, section, msg=""):
        nodes = get_nodes(section)

        for node in nodes:
            C.cprint(node)

            s(f'ssh {node} {cmd}') > None
            print(msg)

    ############################## Async ##############################
    def async_run(self, cmd, msg=""):
        nodes = get_nodes("run")

        len_nodes = len(nodes) or 1
        executor = ThreadPoolExecutor(max_workers=len_nodes)
        new_f = partial(self._run_for_async, cmd=cmd)

        for node in nodes:
            future = executor.submit(new_f, node)
            new_callback = partial(self._callback_for_async, node=node, msg=msg)
            future.add_done_callback(new_callback)

    def _run_for_async(self, node=None, cmd=None):

        return s(f'ssh {node} {cmd}') > ...

    def _callback_for_async(self, future, node=None, msg=""):
        with Run.GLOBAL_LOCK:
            C.cprint(node)
            print(future.result().stderr)
            print(future.result().stdout)
            print(msg)


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


class _KillaAction(BaseAction, Run):
    # main() for argparse action
    def _common_action(self):
        self.sync_cluster_run_without_return(
            r'''"jps | grep -ive 'jps\|=\|^$'  | awk '{print \$1}' | xargs -n1 kill -9 2>/dev/null"''',
            section="kill",
            msg="Killing ......"
        )
        self.sync_cluster_run_without_return("jps", section="kill")


class _TimeSync(BaseAction, Run):
    # main() for argparse action
    def _common_action(self):
        self.sync_cluster_run_without_return(
            f"ntpdate {get_value('time', 'sync_server')}",
            section="time",
            msg="Sync Cluster Time ......"
        )



class _PingAction(BaseAction):
    # main() for argparse action
    def _common_action(self):
        self.ping_async()

    def ping_async(self):
        section = "ping"

        nodes = get_nodes(section)

        if len(nodes) == 1:
            sp_obj = self._run_for_ping(nodes[0])
            self._ping_msg(sp_obj, master_=nodes[0])
            return

        master = nodes[0]
        workers = nodes[1:]

        len_workers = len(workers) or 1
        executor = ThreadPoolExecutor(max_workers=len_workers)

        for worker in workers:
            future = executor.submit(self._run_for_ping, worker)
            new_callback = partial(self._ping_callback, master_=master, worker_=worker)
            future.add_done_callback(new_callback)

    def _run_for_ping(self, node):
        return s(f"ssh -o ConnectTimeout=10 {node} echo") > ...


    def _ping_callback(self, future_, master_=None, worker_=None):
        with Run.GLOBAL_LOCK:
            sp_obj = future_.result()
            self._ping_msg(sp_obj, master_, worker_)

    def _ping_msg(self, sp_obj, master_=None, worker_=None):
        if not worker_:
            import socket
            worker_ = master_
            master_ = socket.gethostname()

        # failed
        if sp_obj.returncode != 0:
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

class Scp():
    def scp_async(self, file_list, msg=""):
        nodes = get_nodes("scp")
        if len(nodes) == 1:
            print(C.red('[Usage]'))
            usage_msg = f"\tthe section {C.green('[scp]')} at least {C.green('nodes=2')} !!!"
            print(C.purple(usage_msg))
            return 

        master = nodes[0]
        workers = nodes[1:]

        jobs = [
            (file, work) for file in file_list
            for work in workers
        ]

        len_jobs = len(jobs) or 1
        executor = ThreadPoolExecutor(max_workers=len_jobs)

        def _scp_callback(future_, master_=None, worker_=None, msg_=""):
            with Run.GLOBAL_LOCK:
                # cprint(f'')
                sp_obj = future_.result()
                if sp_obj.returncode == 1:
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

    def _run_for_scp(self, node=None, cmd=None):
        return s(f'ssh {node} {cmd}') > ...

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

    run = Run()
    scp = Scp()


    def submit_common(arg_from=None, type=""):
        """
            arg_from: args.arks
            type    : "scala" or "python"
        """

        command = f'{spark_home}/bin/spark-submit '

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
        │ All Params Can Adjust In ->  ~/.config.cfg │
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

    parser.add_argument('aa', dest="aa", nargs='+', type=str,
                        help="Run SH            For All Cluster")

    parser.add_argument('as', dest="as_", nargs='+', type=str,
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
    """spark-submit: conf in ~/.config.cfg
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


    parser.add_argument('af', dest="af", nargs=1, type=str,
                        help=textwrap.indent(
    """\
┌────────────Flume───────────────
│fa af start|start-console|stop
│fa af log  |log-start
│NOTE: make sure set job path in ~/.config.cfg
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
        run.sync_cluster_run_without_return(" ".join(args.aa), section="run")

    # All (Async)
    elif args.as_:
        run.async_run( " ".join(args.as_) )  # avoid conflict as(Python) as->as_

    # Zookeeper
    elif args.azk:
        if args.azk in [["start"], ['status'], ["stop"]]:
            run.sync_cluster_run_without_return(
                f'{zk_home}/bin/zkServer.sh {args.azk[0]}',
                section="zookeeper"
            )
        else:
            parser.print_help()

    # Kafka
    elif args.ak:
        kafka_nodes = get_nodes("kafka")


        if len(args.ak) == 1:
            if args.ak == ["start"]:
                run.sync_cluster_run_without_return(
                    f'{kafka_home}/bin/kafka-server-start.sh -daemon {kafka_home}/config/server.properties',
                    section="kafka",
                    msg="Starting Kafka ......"
                )
            elif args.ak == ["stop"]:
                run.sync_cluster_run_without_return(
                    f'{kafka_home}/bin/kafka-server-stop.sh {args.ak[0]}',
                    section="kafka",
                    msg="Stopping Kafka ......"
                )
            # list topics
            elif args.ak[0] == "list":
                s(
                    f'{kafka_home}/bin/kafka-topics.sh --bootstrap-server ' \
                    + ",".join(node+":9092" for node in kafka_nodes ) + " " \
                    + "--list"
                ) > None
                # print(result)

            else:
                parser.print_help()


        elif len(args.ak) > 1:
            if args.ak[0] == "c":
                s(
                    f'{kafka_home}/bin/kafka-console-consumer.sh --bootstrap-server ' \
                    + ",".join(node+":9092" for node in kafka_nodes) + " " \
                    + "--topic" + " " \
                    + args.ak[1]
                ) > None

            # producer
            elif args.ak[0] == "p":
                s(
                    f'{kafka_home}/bin/kafka-console-producer.sh --broker-list ' \
                    + ",".join(node+":9092" for node in kafka_nodes) + " " \
                    + "--topic" + " " \
                    + args.ak[1]
                ) > None



            # create one topic
            # kafka-topics.sh --create --bootstrap-server node1:9092 --topic first_xxx --partitions 2 --replication-factor 3
            elif args.ak[0] == "create":
                s(
                    f'{kafka_home}/bin/kafka-topics.sh --bootstrap-server ' \
                    + ",".join(node+":9092" for node in kafka_nodes) + " " \
                    + "--create" + " " \
                    + "--topic" + " " \
                    + args.ak[1] + " " \
                    + "--partitions" + " " \
                    + args.ak[2] + " " \
                    + "--replication-factor" + " " \
                    + args.ak[3]
                ) > None

            # describe one topic
            elif args.ak[0] == "desc":
                s(
                    f'{kafka_home}/bin/kafka-topics.sh --bootstrap-server ' \
                    + ",".join(node+":9092" for node in kafka_nodes) + " " \
                    + "--describe" + " " \
                    + "--topic" + " " \
                    + args.ak[1]
                ) > None
            # delete one topic
            elif args.ak[0] == "delete":
                s(
                    f'{kafka_home}/bin/kafka-topics.sh --bootstrap-server ' \
                    + ",".join(node+":9092" for node in kafka_nodes) + " " \
                    + "--delete" + " " \
                    + "--topic" + " " \
                    + args.ak[1]
                ) > None

    # ClickHouse
    elif args.ack:
        if args.ack in [["start"], ['status'], ["stop"]]:
            run.sync_cluster_run_without_return(
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
            s(
                rf'''/usr/bin/nohup {hive_home}/bin/hive --service metastore > {hive_home}/logs/hivemetastore-$(/bin/date '+%Y-%m-%d-%H-%M-%S').log 2>&1 &''',
            ) > None
            print("Starting MetaStore ......")
        elif args.ahive == ["stop"]:
            s(
                r'''ps -ef | grep metastore | grep -v grep | awk '{print $2}' | xargs -n1 kill -9'''
            ) > None
            print("Stopping MetaStore ......")

        # hiveserver2
        elif args.ahive == ["start2"]:
            s(
                rf'''/usr/bin/nohup {hive_home}/bin/hive --service hiveserver2 > {hive_home}/logs/hiveserver2-$(/bin/date '+%Y-%m-%d-%H-%M-%S').log 2>&1 &''',
            ) > None
            print("Starting hiveserver2 ......")
        elif args.ahive == ["stop2"]:
            s(
                r'''ps -ef | grep hiveserver2 | grep -v grep | awk '{print $2}' | xargs -n1 kill -9'''
            ) > None
            print("Stopping hiveserver2 ......")

        # beeline
        elif args.ahive in [ ["beeline"], ["bee"] ]:
            # beeline -u jdbc:hive2://node1:10000 -n root --hiveconf hive.server2.logging.operation.level=NONE
            quit_cmd = C.green("!quit")
            print(C.purple(f"[NOTICE]"))
            print(C.purple(f"\tyou must exit beeline by {C.green(quit_cmd)}"))
            s( get_value("hive", "beeline_command") ) > None

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

        # spark thrift
        elif args.ark == ["thstart"]:
            command = f'{spark_home}/sbin/start-thriftserver.sh '
            f'--hiveconf hive.server2.thrift.bind.host={get_value("spark", "THRIFT_HOST")} '
            f'--hiveconf hive.server2.thrift.port={get_value("spark", "THRIFT_PORT")} '
            f'--master {get_value("spark", "THRIFT_MASTER")} '

            s(command) > None
            print("Starting Spark Thrift Server ......")

        elif args.ark == ["thstop"]:
            command = f'{spark_home}/sbin/stop-thriftserver.sh'
            s(command) > None
            print("Stopping Spark Thrift Server ......")

        # beeline
        elif args.ark in [ ["beeline"], ["bee"] ]:
            # beeline -u jdbc:hive2://node1:10000 -n root --hiveconf hive.server2.logging.operation.level=NONE
            quit_cmd = C.green("!quit")
            print(C.purple(f"[NOTICE]"))
            print(C.purple(f"\tyou must exit beeline by {C.green(quit_cmd)}"))
            s( get_value("spark", "beeline_command") ) > None

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
                s(command) > None

            elif args.air == ["status"]:
                print(C.purple("[Status]"))
                if s("ps aux | grep airflow | grep webserver | grep -v grep") > 1 \
                        and s("ps aux | grep airflow | grep scheduler | grep -v grep") > 1:
                    print(f"\tAirflow is {C.green('running')}")
                else:
                    print(f"\tAirflow is {C.red('dead')}")

            elif args.air == ["list"]:
                print(C.purple("[WebServer+Scheduler]"))
                command = "ps aux | grep -v grep | grep airflow"
                s(command) > None

            elif args.air == ["wlist"]:
                print(C.purple("[WebServer]"))
                command = "ps aux | grep -v grep | grep airflow | grep webserver "
                s(command) > None

            elif args.air == ["slist"]:
                print(C.purple("[Scheduler]"))
                command = "ps aux | grep -v grep  | grep airflow | grep scheduler"
                s(command) > None

            elif args.air == ["stop"]:
                print(C.purple("[Stopping]"))
                kill_pid = r"""ps aux | grep airflow | grep -v grep | awk '{print $2}' | xargs -n1 kill -9"""
                del_pid_file = rf"""rm -rf {airflow_home}/airflow-scheduler.pid && rm -rf {airflow_home}/airflow-webserver.pid && rm -rf {airflow_home}/airflow-webserver-monitor.pid"""
                s(kill_pid) > 1
                s(del_pid_file) > 1
                print(f"\tstopping airflow ......")

            else:
                parser.print_help()

        else:
            print("single service version in the future...")
        # Single WebServer
        # elif args.air in [["wstart"], ['wstatus'], ["wstop"]]:

        # Single Scheduler
        # elif args.air in [["sstart"], ['sstatus'], ["sstop"]]:

    # Flume
    elif args.af:
        flume_job_config_path = get_value("flume", "flume_job_config_path")
        Path(flume_job_config_path)

        kill_job_name = flume_job_config_path.split("/")[-1]

        agent_name = get_value("flume", "agent_name")

        if args.af == ["start"]:
            cmd = fr"nohup {flume_home}/bin/flume-ng agent -c {flume_home}/conf -f {flume_job_config_path} -n {agent_name} 1>{flume_home}/logs/start-flume.log 2>&1 &"
            run.sync_cluster_run_without_return(
                cmd,
                # fr"nohup {flume_home}/bin/flume-ng agent -c {flume_home}/conf -f {flume_home}/jobs/logserver-flume-kafka.conf -n a1 -Dflume.root.logger=INFO,console 1>{flume_home}/logs/flume.log 2>&1 &"
                section="flume",
                msg = cmd
            )

        elif args.af == ["start-console"]:
            cmd = fr"{flume_home}/bin/flume-ng agent -c {flume_home}/conf -f {flume_job_config_path} -n {agent_name} -Dflume.root.logger=INFO,console"
            run.sync_cluster_run_without_return(
                cmd,
                section="flume",
                msg=cmd
            )

        elif args.af == ["log"]:
            cmd = get_value("flume", "log_command")
            run.sync_cluster_run_without_return(
                cmd,
                section="flume",
                msg=cmd
            )
        elif args.af == ["log-start"]:
            cmd = get_value("flume", "log_start_command")
            run.sync_cluster_run_without_return(
                cmd,
                section="flume",
                msg=cmd
            )

        elif args.af == ["stop"]:
            # '' string scope for ssh
            cmd = f''' \'ps -ef | grep {kill_job_name} | grep -v grep | awk "{{print \$2}}" | xargs -n1 kill -9 2>/dev/null\' '''
            run.sync_cluster_run_without_return(
                cmd,
                section="flume",
                msg=cmd
            )
        else:
            parser.print_help()

    # Maxwell
    elif args.am:
        if len(args.am) == 3: # fa am   bootstrap <db> <table>
            if args.am[0] in ["boot", "bootstrap"]:
                print(C.purple("[BootStrap]"))
                command = fr"""{maxwell_home}/bin/maxwell-bootstrap --config {maxwell_home}/config.properties --database {args.am[1]} --table {args.am[2]}"""
                s(command) > None
            else:
                parser.print_help()

        elif len(args.am) == 1:
            # if args.am in [["start"], ['stop'], ["restart"]]:
            judge_started_cmd = r"""ps aux | grep maxwell.Maxwell | grep -v grep | wc -l"""
            kill_cmd = r"""ps aux | grep maxwell.Maxwell | grep -v grep | awk '{print $2}' | xargs -n1 kill -9"""

            dead_msg = f"\tMaxwell is {C.red('dead')}"
            running_msg = f"\tMaxwell is {C.green('running')}"


            if args.am == ["start"]:
                print(C.purple("[Starting]"))

                is_started:str = s( judge_started_cmd ) > 1
                if is_started.strip() == "0":
                    command = fr"""{maxwell_home}/bin/maxwell --config {maxwell_home}/config.properties --daemon"""
                    s(command) > None
                else:
                    print(running_msg)

            elif args.am == ["stop"]:
                print(C.purple("[Stopping]"))
                is_started:str = s( judge_started_cmd ) > 1
                if is_started.strip() == "0":
                    print(dead_msg)
                else:
                    s( kill_cmd ) > None
                    print(f"\tstopping Maxwell ......")

            elif args.am == ["status"]:
                is_started:str = s( judge_started_cmd ) > 1
                if is_started.strip() == "0":
                    print(dead_msg)
                else:
                    print(running_msg)

            elif args.am == ["restart"]:
                print(C.purple("[Restart]"))
                command = fr"""{maxwell_home}/bin/maxwell --config {maxwell_home}/config.properties --daemon"""

                is_started:str = s( judge_started_cmd ) > 1
                if is_started.strip() == "0":
                    s(command) > None
                else:
                    s( kill_cmd ) > None
                    s(command) > None

            else:
                parser.print_help()

        else:
            parser.print_help()


    # scp (Async)
    elif args.ap:  # filename_list
        scp.scp_async(args.ap)


if __name__ == '__main__':
    main()

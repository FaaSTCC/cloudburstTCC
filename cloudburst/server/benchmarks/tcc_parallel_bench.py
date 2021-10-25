#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import os
import sys
import time
from datetime import datetime

import cloudpickle as cp
from cloudburst.shared.reference import CloudburstReference

from cloudburst.server.benchmarks.ZipfGenerator import ZipfGenerator
from cloudburst.shared.proto.cloudburst_pb2 import CloudburstError
from cloudburst.server.benchmarks import utils
from cloudburst.shared.serializer import Serializer
from cloudburst.shared.proto.cloudburst_pb2 import (
    Continuation,
    DagTrigger,
    FunctionCall,
    NORMAL, MULTI,  # Cloudburst's consistency modes,
    EXECUTION_ERROR, FUNC_NOT_FOUND,  # Cloudburst's error types
    MULTIEXEC # Cloudburst's execution types
)
from anna.lattices import WrenLattice

import pymongo

def getTime():
    return datetime.now().timestamp() * 1000

def func(cloudburst, futureReads, *argv):
    next_read = []
    keys = futureReads.pop(0)
    for key in keys:
        next_read.append(CloudburstReference(key, True))
    return (futureReads, *next_read)

def func_parallel_test(cloudburst, x):
    return x*2

def func_parallel_sink(cloudburst, x, y):
    return x+y

def run(cloudburst_client, num_requests, create, redis, dag_name, db_size, tx_size, dag_size, zipf):
    myclient = pymongo.MongoClient("mongodb://%s:%s@mongodb-service.default.svc.cluster.local:27017/" % ('root', 'root'))
    mydb = myclient["mydatabase"]
    mycol = mydb[dag_name]
    if create:
        serializer = Serializer()
        cloudburst_client.register(func_parallel_test, 'func0')
        cloudburst_client.register(func_parallel_test, 'func1')
        cloudburst_client.register(func_parallel_sink, 'func2')
        time.sleep(20)

        flag = True
        while flag:
            try:
                success, error = cloudburst_client.register_dag(dag_name, ["func0", "func1", "func2"],
                                                                [("func0", "func2"),("func1", "func2")])
                flag = False
            except:
                continue


        object = serializer.dump_lattice(1, WrenLattice)
        cloudburst_client.kvs_client.put("k0", [object])

        mycol.insert({"operation": "create", "dag_name": dag_name, "result" : success, "error":error, "db_size": db_size, "tx_size": tx_size, "dag_size": dag_size, "zipf" : zipf})

        return [], [], [], 0

    else:
        logging.info("Generating requests")

        arg_map = {'func0': [2], 'func1': [3]}
        total_time = []
        epoch_req_count = 0
        epoch_latencies = []

        epoch_start = getTime()
        epoch = 0
        for _ in range(num_requests):
            flag = True
            start = getTime()

            while flag:
                try:
                    res = cloudburst_client.call_dag(dag_name, arg_map, consistency=MULTI, output_key="k0",
                                                     direct_response=True)
                    if res == "abort":
                        logging.info("aborted")
                        continue
                    flag = False
                except Exception as e:
                    logging.info(e)
                    continue

            end = getTime()

            if res is not None:
                epoch_req_count += 1

            total_time += [end - start]
            epoch_latencies += [end - start]

            epoch_end = getTime()
            if epoch_end - epoch_start > 10:
                if redis:
                    logging.info("Have redis")
                    redis.publish("result",cp.dumps((epoch_req_count, epoch_latencies)))
                logging.info('EPOCH %d THROUGHPUT: %.2f' %
                             (epoch, (epoch_req_count / 10)))
                out = utils.print_latency_stats(epoch_latencies,
                                          'EPOCH %d E2E' % epoch, True)

                epoch += 1

                epoch_req_count = 0
                epoch_latencies.clear()
                epoch_start = getTime()

        out = utils.print_latency_stats(total_time, 'E2E', True)
        mycol.insert(
            {"operation": "finish", "dag_name": dag_name, "result": out, "error": None, "db_size": db_size,
             "tx_size": tx_size, "dag_size": dag_size, "zipf": zipf})
        return total_time, [], [], 0
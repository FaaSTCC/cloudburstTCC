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

def sink(cloudburst, futureReads, *argv):
    next_read = []
    keys = futureReads.pop(0)
    for key in keys:
        next_read.append(CloudburstReference(key, True))
    return 1

def func_parallel_test(cloudburst, x):
    return x*2

def func_parallel_sink(cloudburst, x, y):
    return x+y

def run(cloudburst_client, num_requests, create, redis, dag_name, db_size, tx_size, dag_size, zipf, warmup):
    myclient = pymongo.MongoClient("mongodb://%s:%s@mongodb-service.default.svc.cluster.local:27017/" % ('root', 'root'))
    mydb = myclient["mydatabase"]
    mycol = mydb[dag_name]
    if create:
        serializer = Serializer()

        functions = []
        last = ""
        connections = []
        for i in range(dag_size-1):
            functions.append('func' + str(i))
            cloudburst_client.register(func, 'func' + str(i))
            if (last != ""):
                connections.append((last, 'func' + str(i)))
            last = 'func' + str(i)
        functions.append('write')
        cloudburst_client.register(sink, 'write')
        if (last != ""):
            connections.append((last, 'write'))
        time.sleep(20)
        flag = True
        while flag:
            try:
                success, error = cloudburst_client.register_dag(dag_name, functions, connections)
                flag = False
            except:
                continue
        keys = []
        object = serializer.dump_lattice(1, WrenLattice)
        for i in range(db_size + 1):
            keys.append("k" + str(i))
            if i % 1000 == 0:
                cloudburst_client.kvs_client.put(keys, [object] * 1000)
                keys = []
            pass

        if len(keys) != 0:
            cloudburst_client.kvs_client.put(keys, [object] * len(keys))

        mycol.insert({"operation": "create", "dag_name": dag_name, "result" : success, "error":error, "db_size": db_size, "tx_size": tx_size, "dag_size": dag_size, "zipf" : zipf})

        return [], [], [], 0

    elif warmup:
        logging.info("Warming up")
        warmup_keys = 10000
        requests = []
        tx_size = 100
        for i in range(warmup_keys // tx_size):
            keys_to_read = []
            ref_to_key = []
            for j in range(tx_size):
                keys_to_read.append(CloudburstReference("k" + str(i * tx_size + j), True))
            for k in range(0, dag_size):
                next_request = []
                for m in range(tx_size):
                    next_request.append("k" + str((i * tx_size + k * tx_size + m) % warmup_keys))
                ref_to_key.append(next_request)

            arg_map = {'func0': [ref_to_key, *keys_to_read]}
            requests.append(arg_map)

        total_time = []
        epoch_req_count = 0
        epoch_latencies = []

        epoch_start = getTime()
        epoch = 0
        for request in requests:
            start = getTime()
            flag = True
            while flag:
                try:
                    res = cloudburst_client.call_dag(dag_name, request, consistency=MULTI, direct_response=True)
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
                    redis.publish("result", cp.dumps((epoch_req_count, epoch_latencies)))
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

    else:
        logging.info("Generating requests")
        logging.info("DB Size: %s zipf: %s" % (db_size, zipf))
        logging.info("zipf_" + str(zipf) + "_" + str(db_size) + ".json")
        logging.info("Current path: %s" % os.getcwd())
        zipfGenerator = ZipfGenerator(db_size, zipf)
        logging.info("zipf generator created")
        next_read = []
        keys = set()
        while len(keys) < tx_size:
            keys.add("k" + str(zipfGenerator.next()))

        for key in keys:
            next_read.append(CloudburstReference(key, True))

        total_time = []
        epoch_req_count = 0
        epoch_latencies = []

        epoch_start = getTime()
        epoch = 0
        requests = []
        for _ in range(num_requests):
            next_read = []
            keys = set()
            output_key = "k" + str(zipfGenerator.next())
            futureReads = []
            while len(keys) < tx_size:
                keys.add("k" + str(zipfGenerator.next()))
            for key in keys:
                next_read.append(CloudburstReference(key, True))
            for i in range(dag_size):
                f_read = []
                keys = set()
                while len(keys) < tx_size:
                    keys.add("k" + str(zipfGenerator.next()))
                futureReads.append(keys)

            arg_map = {'func0': [futureReads, *next_read]}
            requests.append(arg_map)
        ''' RUN DAG '''

        total_time = []
        epoch_req_count = 0
        epoch_latencies = []

        logging.info("Starting requests")

        epoch_start = getTime()
        epoch = 0
        for request in requests:
            output_key = "k" + str(zipfGenerator.next())
            start = getTime()
            flag = True
            while flag:
                try:
                    res = cloudburst_client.call_dag(dag_name, request, consistency=MULTI, output_key=output_key, direct_response=True)
                    if (res == "abort"):
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

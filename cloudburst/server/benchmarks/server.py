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
#  Modifications copyright (C) 2021 Taras Lykhenko, Rafael Soares

import logging
import sys

import zmq

from cloudburst.client.client import CloudburstConnection
from cloudburst.server.benchmarks import (
    composition,
    locality,
    tcc_bench,
    lambda_locality,
    mobilenet,
    predserving,
    scaling,
    utils
)
import cloudburst.server.utils as sutils
import redis


BENCHMARK_START_PORT = 3000

logging.basicConfig(filename='log_benchmark.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s')

def benchmark(ip, cloudburst_address, tid):
    cloudburst = CloudburstConnection(cloudburst_address, ip, tid)
    r = redis.Redis(host='redis-master.default.svc.cluster.local', port=6379, db=0, decode_responses=True)

    kvs = cloudburst.kvs_client

    p = r.pubsub()
    p.psubscribe('benchmark[%s,t]' % tid)

    for msg in p.listen():
        if "subscribe" in msg["type"]:
            continue
        msg = msg['data']
        splits = msg.split(':')
        bname = splits[0]
        num_requests = int(splits[1])


        dag_name = splits[2]
        db_size = int(splits[3])
        tx_size = int(splits[4])
        dag_size = int(splits[5])
        zipf = float(splits[6])


        create = eval(splits[7])

        run_bench(bname, num_requests, cloudburst, kvs, r, create, dag_name, db_size, tx_size, dag_size, zipf)


def run_bench(bname, num_requests, cloudburst, kvs, redis, create=False, dag_name = "", db_size = 0, tx_size = 0, dag_size = 0, zipf = 0):
    logging.info('Running benchmark %s, %d requests.' % (bname, num_requests))


    if bname == 'tcc':
        total, scheduler, kvs, retries = tcc_bench.run(cloudburst, num_requests,
                                                  create, redis, dag_name, db_size, tx_size, dag_size, zipf)

    else:
        logging.info('Unknown benchmark type: %s!' % (bname))
        return

    # some benchmark modes return no results
    if not total:
        redis.publish("result", b'END')
        logging.info('*** Benchmark %s finished. It returned no results. ***'
                     % (bname))
        return
    else:
        redis.publish("result", b'END')
        logging.info('*** Benchmark %s finished. ***' % (bname))

    logging.info('Total computation time: %.4f' % (sum(total)))
    if len(total) > 0:
        utils.print_latency_stats(total, 'E2E', True)
    if len(scheduler) > 0:
        utils.print_latency_stats(scheduler, 'SCHEDULER', True)
    if len(kvs) > 0:
        utils.print_latency_stats(kvs, 'KVS', True)
    logging.info('Number of KVS get retries: %d' % (retries))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        conf_file = sys.argv[1]
    else:
        conf_file = 'conf/cloudburst-config.yml'

    conf = sutils.load_conf(conf_file)
    bench_conf = conf['benchmark']

    benchmark(conf['ip'], bench_conf['cloudburst_address'],
              int(bench_conf['thread_id']))

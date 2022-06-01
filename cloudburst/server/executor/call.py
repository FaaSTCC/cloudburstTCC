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
import time

from anna.lattices import (
    Lattice,
    MapLattice,
    LWWPairLattice,
    MultiKeyCausalLattice,
    SetLattice,
    SingleKeyCausalLattice,
    VectorClock,
    WrenLattice
)

from cloudburst.server.executor import utils
import cloudburst.server.utils as sutils
from cloudburst.shared.proto.cloudburst_pb2 import (
    Continuation,
    DagTrigger,
    FunctionCall,
    NORMAL, MULTI,  # Cloudburst's consistency modes,
    EXECUTION_ERROR, FUNC_NOT_FOUND,  # Cloudburst's error types
    MULTIEXEC # Cloudburst's execution types
)
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.serializer import Serializer

serializer = Serializer()


def exec_function(exec_socket, kvs, user_library, cache, function_cache):
    call = FunctionCall()
    call.ParseFromString(exec_socket.recv())

    fargs = [serializer.load(arg) for arg in call.arguments.values]

    if call.name in function_cache:
        f = function_cache[call.name]
    else:
        f = utils.retrieve_function(call.name, kvs, user_library, call.consistency)

    if not f:
        logging.info('Function %s not found! Returning an error.' %
                     (call.name))
        sutils.error.error = FUNC_NOT_FOUND
        result = ('ERROR', sutils.error.SerializeToString())
    else:
        function_cache[call.name] = f
        try:
            if call.consistency == NORMAL:
                result = _exec_func_normal(kvs, f, fargs, user_library, cache)
                logging.info('Finished executing %s: %s!' % (call.name,
                                                             str(result)))
            else:
                result, new_t_low, new_t_high = _exec_func_causal(kvs, f, fargs, user_library)
        except Exception as e:
            logging.exception('Unexpected error %s while executing function.' %
                              (str(e)))
            sutils.error.error = EXECUTION_ERROR
            result = ('ERROR: ' + str(e), sutils.error.SerializeToString())

    if call.consistency == NORMAL:
        result = serializer.dump_lattice(result)
        succeed = kvs.put(call.response_key, result)
    else:
        result = serializer.dump_lattice(result, WrenLattice)
        succeed = kvs.causal_put(call.response_key, result)

    if not succeed:
        logging.info(f'Unsuccessful attempt to put key {call.response_key} '
                     + 'into the KVS.')


def _exec_func_normal(kvs, func, args, user_lib, cache):
    # NOTE: We may not want to keep this permanently but need it for
    # continuations if the upstream function returns multiple things.
    processed = tuple()
    for arg in args:
        if type(arg) == tuple:
            processed += arg
        else:
            processed += (arg,)
    args = processed

    if all([type(arg) == list for arg in args]): # A batching request.
        refs = []

        # For a batching request, we pull out the references in each sublist of
        # arguments.
        for arg in args:
            arg_refs = list(
                filter(lambda a: isinstance(a, CloudburstReference), arg))
            refs.extend(arg_refs)
    else:
        # For non-batching requests, we just filter all of the arguments.
        refs = list(filter(lambda a: isinstance(a, CloudburstReference), args))

    if refs:
        refs = _resolve_ref_normal(refs, kvs, cache)

    return _run_function(func, refs, args, user_lib)


def _exec_func_causal(kvs, func, args, user_lib, schedule=None,
                      t_low=0, t_high= 2**64-1):
    refs = list(filter(lambda a: isinstance(a, CloudburstReference), args))

    if refs:
        refs, t_low, t_high = _resolve_ref_causal(refs, kvs, schedule, t_low,
                                   t_high)

    return _run_function(func, refs, args, user_lib), t_low, t_high


def _run_function(func, refs, args, user_lib):
    # Set the first argument to the user library.
    func_args = (user_lib,)

    # If any of the arguments are references, we insert the resolved reference
    # instead of the raw value.
    for arg in args:
        # The standard non-batching approach to resolving references. We simply
        # take the KV-pairs and swap in the actual values for the references.
        if type(arg) != list:
            if isinstance(arg, CloudburstReference):
                func_args += (refs[arg.key],)
            else:
                func_args += (arg,)
        else:
            # The batching approach: We look at each value to check if it's a
            # ref then append the whole list to the argument set.
            for idx, val in enumerate(arg):
                if isinstance(val, CloudburstReference):
                    arg[idx] = refs[val.key]

            func_args += (arg,)

    return func(*func_args)


def _resolve_ref_normal(refs, kvs, cache):
    deserialize_map = {}
    kv_pairs = {}
    keys = set()

    for ref in refs:
        deserialize_map[ref.key] = ref.deserialize
        if ref.key in cache:
            kv_pairs[ref.key] = cache[ref.key]
        else:
            keys.add(ref.key)

    keys = list(keys)

    if len(keys) != 0:
        returned_kv_pairs = kvs.get(keys)

        # When chaining function executions, we must wait, so we check to see
        # if certain values have not been resolved yet.
        while None in returned_kv_pairs.values():
            returned_kv_pairs = kvs.get(keys)

        for key in keys:
            # Because references might be repeated, we check to make sure that
            # we haven't already deserialized this ref.
            if deserialize_map[key] and isinstance(returned_kv_pairs[key],
                                                   Lattice):
                kv_pairs[key] = serializer.load_lattice(returned_kv_pairs[key])
            else:
                kv_pairs[key] = returned_kv_pairs[key].reveal()

            # Cache the deserialized payload for future use
            cache[key] = kv_pairs[key]

    return kv_pairs


def _resolve_ref_causal(refs, kvs, schedule, t_low,
                        t_high):
    if schedule:
        client_id = schedule.client_id
        consistency = schedule.consistency
    else:
        client_id = 0
        consistency = MULTI

    keys = [ref.key for ref in refs]
    kv_pairs = kvs.causal_get(keys, t_low, t_high, consistency, client_id)

    while None in kv_pairs.values():
        kv_pairs = kvs.causal_get(keys, t_low, t_high, consistency, client_id)



    for ref in refs:
        key = ref.key
        if ref.deserialize:
            # In causal mode, you can only use these two lattice types.
            if (isinstance(kv_pairs[key], LWWPairLattice)):
                # If there are multiple values, we choose the first one listed
                # at random.
                t_low = max(kv_pairs[key].ts, t_low)
                t_high = min(kv_pairs[key].promise, t_high)
                kv_pairs[key] = serializer.load_lattice(kv_pairs[key])

            else:
                raise ValueError(('Invalid lattice type %s encountered when' +
                                 ' executing in causal mode.') %
                                 str(type(kv_pairs[key])))
        else:
            kv_pairs[key] = kv_pairs[key].reveal()

    return kv_pairs, t_low, t_high


def exec_dag_function(pusher_cache, kvs, trigger_sets, function, schedules,
                      user_library, dag_runtimes, cache, schedulers, batching):
    if schedules[0].consistency == NORMAL:
        finished, successes = _exec_dag_function_normal(pusher_cache, kvs,
                                                        trigger_sets, function,
                                                        schedules,
                                                        user_library, cache,
                                                        schedulers, batching)
    else:
        finished, successes = _exec_dag_function_causal(pusher_cache, kvs,
                                                        trigger_sets, function,
                                                        schedules, user_library)

    # If finished is true, that means that this executor finished the DAG
    # request. We will report the end-to-end latency for this DAG if so.
    if finished:
        for schedule, success in zip(schedules, successes):
            if success:
                dname = schedule.dag.name
                if dname not in dag_runtimes:
                    dag_runtimes[dname] = []

                runtime = time.time() - schedule.start_time
                dag_runtimes[schedule.dag.name].append(runtime)

    return successes


def _construct_trigger(sid, fname, result):
    trigger = DagTrigger()
    trigger.id = sid
    trigger.source = fname

    if type(result) != tuple:
        result = (result,)

    trigger.arguments.values.extend(list(
        map(lambda v: serializer.dump(v, None, False), result)))
    return trigger


def _exec_dag_function_normal(pusher_cache, kvs, trigger_sets, function,
                              schedules, user_lib, cache, schedulers,
                              batching):
    fname = schedules[0].target_function

    # We construct farg_sets to have a request by request set of arguments.
    # That is, each element in farg_sets will have all the arguments for one
    # invocation.
    farg_sets = []
    for schedule, trigger_set in zip(schedules, trigger_sets):
        fargs = list(schedule.arguments[fname].values)

        for trigger in trigger_set:
            fargs += list(trigger.arguments.values)

        fargs = [serializer.load(arg) for arg in fargs]
        farg_sets.append(fargs)

    if batching:
        fargs = [[]] * len(farg_sets[0])
        for idx in range(len(fargs)):
            fargs[idx] = [fset[idx] for fset in farg_sets]
    else: # There will only be one thing in farg_sets
        fargs = farg_sets[0]

    result_list = _exec_func_normal(kvs, function, fargs, user_lib, cache)
    if not isinstance(result_list, list):
        result_list = [result_list]

    successes = []
    is_sink = True

    for schedule, result in zip(schedules, result_list):
        this_ref = None
        for ref in schedule.dag.functions:
            if ref.name == fname:
                this_ref = ref # There must be a match.

        if this_ref.type == MULTIEXEC:
            if serializer.dump(result) in this_ref.invalid_results:
                successes.append(False)
                continue

        successes.append(True)
        new_trigger = _construct_trigger(schedule.id, fname, result)
        for conn in schedule.dag.connections:
            if conn.source == fname:
                is_sink = False
                new_trigger.target_function = conn.sink

                dest_ip = schedule.locations[conn.sink]
                sckt = pusher_cache.get(sutils.get_dag_trigger_address(dest_ip))
                sckt.send(new_trigger.SerializeToString())

    if is_sink:
        if schedule.continuation.name:
            for idx, pair in enumerate(zip(schedules, result_list)):
                schedule, result = pair
                if successes[idx]:
                    cont = schedule.continuation
                    cont.id = schedule.id
                    cont.result = serializer.dump(result)

                    logging.info('Sending continuation to scheduler for DAG %s.' %
                                 (schedule.id))
                    sckt = pusher_cache.get(utils.get_continuation_address(schedulers))
                    sckt.send(cont.SerializeToString())
        elif schedule.response_address:
            for idx, pair in enumerate(zip(schedules, result_list)):
                schedule, result = pair
                if successes[idx]:
                    sckt = pusher_cache.get(schedule.response_address)
                    logging.info('DAG %s (ID %s) result returned to requester.' %
                                 (schedule.dag.name, trigger.id))
                    sckt.send(serializer.dump(result))
        else:
            keys = []
            lattices = []
            for idx, pair in enumerate(zip(schedules, result_list)):
                schedule, result = pair
                if successes[idx]:
                    lattice = serializer.dump_lattice(result)
                    output_key = schedule.output_key if schedule.output_key \
                        else schedule.id
                    logging.info('DAG %s (ID %s) result in KVS at %s.' %
                                 (schedule.dag.name, schedule.id, output_key))

                    keys.append(output_key)
                    lattices.append(lattice)
            kvs.put(keys, lattices)

    return is_sink, successes


# Causal mode does not currently support batching, so there should only ever be
# one trigger set and oone schedule.
def _exec_dag_function_causal(pusher_cache, kvs, triggers, function, schedule,
                              user_lib):
    schedule = schedule[0]
    triggers = triggers[0]

    fname = schedule.target_function
    fargs = list(schedule.arguments[fname].values)


    for trigger in triggers:
        fargs += list(trigger.arguments.values)


    fargs = [serializer.load(arg) for arg in fargs]

    if(trigger.t_high == 0):
        trigger.t_high = 2**64-1
    result, new_t_low, new_t_high = _exec_func_causal(kvs, function, fargs, user_lib, schedule,
                               trigger.t_low, trigger.t_high)

    this_ref = None
    for ref in schedule.dag.functions:
        if ref.name == fname:
            this_ref = ref # There must be a match.

    success = True
    if this_ref.type == MULTIEXEC:
        if serializer.dump(result) in this_ref.invalid_results:
            return False, False

    # Create a new trigger with the schedule ID and results of this execution.
    new_trigger = _construct_trigger(schedule.id, fname, result)

    if trigger.t_high == 2**64-1:
        logging.info("Putting snapshot as: {}", new_t_high)
        new_trigger.t_low = new_t_high
        new_trigger.t_high = new_t_high
    else:
        logging.info("Snapshot continues as: trigger low {} trigger high {}", trigger.t_low, trigger.t_high)
        new_trigger.t_low = trigger.t_low
        new_trigger.t_high = trigger.t_high


    is_sink = True
    for conn in schedule.dag.connections:
        if conn.source == fname:
            is_sink = False
            new_trigger.target_function = conn.sink
            logging.info("Was not sink, passing function along")

            dest_ip = schedule.locations[conn.sink]
            sckt = pusher_cache.get(sutils.get_dag_trigger_address(dest_ip))
            sckt.send(new_trigger.SerializeToString())

    if is_sink:
        logging.info('DAG %s (ID %s) completed in causal mode; result at %s.' %
                     (schedule.dag.name, schedule.id, schedule.output_key))


        # Serialize result into a MultiKeyCausalLattice.
        result = serializer.dump_lattice(result,WrenLattice);

        succeed = kvs.causal_put(schedule.output_key,
                                 result, schedule.client_id)
        while not succeed:
            succeed = kvs.causal_put(schedule.output_key,
                                     result, schedule.client_id)
        if schedule.response_address:
            sckt = pusher_cache.get(schedule.response_address)
            logging.info('DAG %s (ID %s) result returned to requester.' %
                         (schedule.dag.name, trigger.id))
            sckt.send(serializer.dump(result))


    return is_sink, [success]


def _compute_children_read_set(schedule):
    future_read_set = set()
    fname = schedule.target_function
    children = set()
    delta = {fname}

    while len(delta) > 0:
        new_delta = set()
        for conn in schedule.dag.connections:
            if conn.source in delta:
                children.add(conn.sink)
                new_delta.add(conn.sink)
        delta = new_delta

    for child in children:
        refs = list(filter(lambda arg: type(arg) == CloudburstReference,
                           [serializer.load(arg) for arg in
                            schedule.arguments[child].values]))
        for ref in refs:
            future_read_set.add(ref.key)

    return future_read_set

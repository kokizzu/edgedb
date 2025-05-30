#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations
from typing import Any, Optional

import pickle

import immutables

from edb import edgeql
from edb import graphql
from edb.common import uuidgen
from edb.pgsql import params as pgparams
from edb.schema import schema as s_schema
from edb.server import compiler
from edb.server import config
from edb.server import defines

from . import state
from . import worker_proc


INITED: bool = False
DBS: state.DatabasesState = immutables.Map()
BACKEND_RUNTIME_PARAMS: pgparams.BackendRuntimeParams = \
    pgparams.get_default_runtime_params()
COMPILER: compiler.Compiler
LAST_STATE: Optional[compiler.dbstate.CompilerConnectionState] = None
LAST_STATE_PICKLE: Optional[bytes] = None
STD_SCHEMA: s_schema.FlatSchema
GLOBAL_SCHEMA: s_schema.FlatSchema
INSTANCE_CONFIG: immutables.Map[str, config.SettingValue]


def __init_worker__(
    init_args_pickled: bytes,
) -> None:
    global INITED
    global BACKEND_RUNTIME_PARAMS
    global COMPILER
    global STD_SCHEMA
    global GLOBAL_SCHEMA
    global INSTANCE_CONFIG

    (
        backend_runtime_params,
        std_schema,
        refl_schema,
        schema_class_layout,
        global_schema_pickle,
        system_config,
    ) = pickle.loads(init_args_pickled)

    INITED = True
    BACKEND_RUNTIME_PARAMS = backend_runtime_params
    STD_SCHEMA = std_schema
    GLOBAL_SCHEMA = pickle.loads(global_schema_pickle)
    INSTANCE_CONFIG = system_config

    COMPILER = compiler.new_compiler(
        std_schema,
        refl_schema,
        schema_class_layout,
        backend_runtime_params=BACKEND_RUNTIME_PARAMS,
        config_spec=None,
    )


def __sync__(
    dbname: str,
    evicted_dbs: list[str],
    user_schema: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
) -> state.DatabaseState:
    global DBS
    global GLOBAL_SCHEMA
    global INSTANCE_CONFIG

    try:
        if evicted_dbs:
            dbs = DBS.mutate()
            for name in evicted_dbs:
                dbs.pop(name, None)
            DBS = dbs.finish()

        db = DBS.get(dbname)
        if db is None:
            assert user_schema is not None
            assert reflection_cache is not None
            assert database_config is not None
            user_schema_unpacked = pickle.loads(user_schema)
            reflection_cache_unpacked = pickle.loads(reflection_cache)
            database_config_unpacked = pickle.loads(database_config)
            db = state.DatabaseState(
                dbname,
                user_schema_unpacked,
                reflection_cache_unpacked,
                database_config_unpacked,
            )
            DBS = DBS.set(dbname, db)
        else:
            updates = {}

            if user_schema is not None:
                updates['user_schema'] = pickle.loads(user_schema)
            if reflection_cache is not None:
                updates['reflection_cache'] = pickle.loads(reflection_cache)
            if database_config is not None:
                updates['database_config'] = pickle.loads(database_config)

            if updates:
                db = db._replace(**updates)
                DBS = DBS.set(dbname, db)

        if global_schema is not None:
            GLOBAL_SCHEMA = pickle.loads(global_schema)

        if system_config is not None:
            INSTANCE_CONFIG = pickle.loads(system_config)

    except Exception as ex:
        raise state.FailedStateSync(
            f'failed to sync worker state: {type(ex).__name__}({ex})') from ex

    return db


def compile(
    dbname: str,
    evicted_dbs: list[str],
    user_schema: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
    *compile_args: Any,
    **compile_kwargs: Any,
):
    db = __sync__(
        dbname,
        evicted_dbs,
        user_schema,
        reflection_cache,
        global_schema,
        database_config,
        system_config,
    )

    units, cstate = COMPILER.compile_serialized_request(
        db.user_schema,
        GLOBAL_SCHEMA,
        db.reflection_cache,
        db.database_config,
        INSTANCE_CONFIG,
        *compile_args,
        **compile_kwargs
    )

    global LAST_STATE, LAST_STATE_PICKLE

    LAST_STATE = cstate
    LAST_STATE_PICKLE = None
    if cstate is not None:
        LAST_STATE_PICKLE = pickle.dumps(cstate, -1)

    return units, LAST_STATE_PICKLE


def compile_in_tx(
    dbname: Optional[str], user_schema: Optional[bytes], cstate, *args, **kwargs
):
    global LAST_STATE, LAST_STATE_PICKLE

    prev_last_state_key = None
    if cstate == state.REUSE_LAST_STATE_MARKER:
        assert LAST_STATE is not None
        cstate = LAST_STATE
        prev_last_state_key = cstate.get_state_key()
    else:
        cstate = pickle.loads(cstate)
        LAST_STATE_PICKLE = None
        if dbname is None:
            assert user_schema is not None
            cstate.set_root_user_schema(pickle.loads(user_schema))
        else:
            cstate.set_root_user_schema(DBS[dbname].user_schema)
    units, cstate = COMPILER.compile_serialized_request_in_tx(
        cstate, *args, **kwargs)

    LAST_STATE = cstate

    # We don't want to continuously re-pickle transaction state
    # for every new query in a transaction that doesn't actually change
    # its state in every query. I.e. it doesn't run DDL, configures
    # new session aliases, configs, or globals.
    if (prev_last_state_key is None or
        LAST_STATE_PICKLE is None or
        prev_last_state_key != cstate.get_state_key()
    ):
        LAST_STATE_PICKLE = pickle.dumps(cstate, -1)

    return units, LAST_STATE_PICKLE


def compile_notebook(
    dbname: str,
    evicted_dbs: list[str],
    user_schema: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
    *compile_args: Any,
    **compile_kwargs: Any,
):
    db = __sync__(
        dbname,
        evicted_dbs,
        user_schema,
        reflection_cache,
        global_schema,
        database_config,
        system_config,
    )

    return COMPILER.compile_notebook(
        db.user_schema,
        GLOBAL_SCHEMA,
        db.reflection_cache,
        db.database_config,
        INSTANCE_CONFIG,
        *compile_args,
        **compile_kwargs
    )


def compile_graphql(
    dbname: str,
    evicted_dbs: list[str],
    user_schema: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
    *compile_args: Any,
    **compile_kwargs: Any,
) -> tuple[compiler.QueryUnitGroup, graphql.TranspiledOperation]:
    db = __sync__(
        dbname,
        evicted_dbs,
        user_schema,
        reflection_cache,
        global_schema,
        database_config,
        system_config,
    )

    gql_op = graphql.compile_graphql(
        STD_SCHEMA,
        db.user_schema,
        GLOBAL_SCHEMA,
        db.database_config,
        INSTANCE_CONFIG,
        *compile_args,
        **compile_kwargs
    )

    source = edgeql.Source.from_string(
        edgeql.generate_source(gql_op.edgeql_ast, pretty=True),
    )

    cfg_ser = COMPILER.state.compilation_config_serializer
    request = compiler.CompilationRequest(
        source=source,
        protocol_version=defines.CURRENT_PROTOCOL,
        schema_version=uuidgen.uuid4(),
        compilation_config_serializer=cfg_ser,
        output_format=compiler.OutputFormat.JSON,
        input_format=compiler.InputFormat.JSON,
        expect_one=True,
        implicit_limit=0,
        inline_typeids=False,
        inline_typenames=False,
        inline_objectids=False,
        modaliases=None,
        session_config=None,
    )

    unit_group, _ = COMPILER.compile(
        user_schema=db.user_schema,
        global_schema=GLOBAL_SCHEMA,
        reflection_cache=db.reflection_cache,
        database_config=db.database_config,
        system_config=INSTANCE_CONFIG,
        request=request,
    )

    return unit_group, gql_op  # type: ignore[return-value]


def compile_sql(
    dbname: str,
    evicted_dbs: list[str],
    user_schema: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
    *compile_args: Any,
    **compile_kwargs: Any,
):
    db = __sync__(
        dbname,
        evicted_dbs,
        user_schema,
        reflection_cache,
        global_schema,
        database_config,
        system_config,
    )

    return COMPILER.compile_sql(
        db.user_schema,
        GLOBAL_SCHEMA,
        db.reflection_cache,
        db.database_config,
        INSTANCE_CONFIG,
        *compile_args,
        **compile_kwargs
    )


def get_handler(methname):
    if methname == "__init_worker__":
        meth = __init_worker__
    else:
        if not INITED:
            raise RuntimeError(
                "call on uninitialized compiler worker"
            )
        if methname == "compile":
            meth = compile
        elif methname == "compile_in_tx":
            meth = compile_in_tx
        elif methname == "compile_notebook":
            meth = compile_notebook
        elif methname == "compile_graphql":
            meth = compile_graphql
        elif methname == "compile_sql":
            meth = compile_sql
        else:
            meth = getattr(COMPILER, methname)
    return meth


if __name__ == "__main__":
    try:
        worker_proc.main(get_handler)
    except KeyboardInterrupt:
        pass

/*
 * Quarry FFI Implementation
 * C bindings for SQLite with external class registration
 */

#include <lean/lean.h>
#include <sqlite3.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ========================================================================== */
/* External Class Registration                                                 */
/* ========================================================================== */

static lean_external_class* g_database_class = NULL;
static lean_external_class* g_statement_class = NULL;
static lean_external_class* g_backup_class = NULL;
static lean_external_class* g_blob_class = NULL;

/* Wrapper for backup to track if it's been explicitly finished */
typedef struct {
    sqlite3_backup* backup;
    int finished;  /* 1 if finish was called explicitly */
} BackupWrapper;

/* Wrapper for blob to track if it's been explicitly closed */
typedef struct {
    sqlite3_blob* blob;
    int closed;  /* 1 if close was called explicitly */
} BlobWrapper;

/* ========================================================================== */
/* Finalizers                                                                  */
/* ========================================================================== */

static void database_finalizer(void* ptr) {
    sqlite3* db = (sqlite3*)ptr;
    if (db) {
        sqlite3_close_v2(db);
    }
}

static void statement_finalizer(void* ptr) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)ptr;
    if (stmt) {
        sqlite3_finalize(stmt);
    }
}

static void backup_finalizer(void* ptr) {
    BackupWrapper* wrapper = (BackupWrapper*)ptr;
    if (wrapper) {
        if (!wrapper->finished && wrapper->backup) {
            sqlite3_backup_finish(wrapper->backup);
        }
        free(wrapper);
    }
}

static void blob_finalizer(void* ptr) {
    BlobWrapper* wrapper = (BlobWrapper*)ptr;
    if (wrapper) {
        if (!wrapper->closed && wrapper->blob) {
            sqlite3_blob_close(wrapper->blob);
        }
        free(wrapper);
    }
}

static void noop_foreach(void* ptr, b_lean_obj_arg arg) {
    (void)ptr;
    (void)arg;
}

/* ========================================================================== */
/* Initialization                                                              */
/* ========================================================================== */

static void init_external_classes(void) {
    if (g_database_class == NULL) {
        g_database_class = lean_register_external_class(database_finalizer, noop_foreach);
        g_statement_class = lean_register_external_class(statement_finalizer, noop_foreach);
        g_backup_class = lean_register_external_class(backup_finalizer, noop_foreach);
        g_blob_class = lean_register_external_class(blob_finalizer, noop_foreach);
    }
}

static lean_object* mk_io_error(const char* msg) {
    return lean_io_result_mk_error(lean_mk_io_user_error(lean_mk_string(msg)));
}

static lean_object* mk_sqlite_error(sqlite3* db) {
    const char* msg = sqlite3_errmsg(db);
    return lean_io_result_mk_error(lean_mk_io_user_error(lean_mk_string(msg)));
}

/* ========================================================================== */
/* Database Operations                                                         */
/* ========================================================================== */

LEAN_EXPORT lean_obj_res quarry_db_open(b_lean_obj_arg path_obj, lean_obj_arg world) {
    init_external_classes();

    const char* path = lean_string_cstr(path_obj);
    sqlite3* db = NULL;

    int rc = sqlite3_open(path, &db);
    if (rc != SQLITE_OK) {
        const char* err = db ? sqlite3_errmsg(db) : "Failed to open database";
        if (db) sqlite3_close(db);
        return mk_io_error(err);
    }

    lean_object* obj = lean_alloc_external(g_database_class, db);
    return lean_io_result_mk_ok(obj);
}

LEAN_EXPORT lean_obj_res quarry_db_open_memory(lean_obj_arg world) {
    init_external_classes();

    sqlite3* db = NULL;
    int rc = sqlite3_open(":memory:", &db);
    if (rc != SQLITE_OK) {
        const char* err = db ? sqlite3_errmsg(db) : "Failed to open in-memory database";
        if (db) sqlite3_close(db);
        return mk_io_error(err);
    }

    lean_object* obj = lean_alloc_external(g_database_class, db);
    return lean_io_result_mk_ok(obj);
}

LEAN_EXPORT lean_obj_res quarry_db_close(b_lean_obj_arg db_obj, lean_obj_arg world) {
    /* Intentionally a no-op: finalizer handles cleanup */
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res quarry_db_errmsg(b_lean_obj_arg db_obj, lean_obj_arg world) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    const char* msg = sqlite3_errmsg(db);
    return lean_io_result_mk_ok(lean_mk_string(msg ? msg : ""));
}

LEAN_EXPORT lean_obj_res quarry_db_errcode(b_lean_obj_arg db_obj, lean_obj_arg world) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    int code = sqlite3_errcode(db);
    return lean_io_result_mk_ok(lean_int_to_int(code));
}

LEAN_EXPORT lean_obj_res quarry_db_exec(b_lean_obj_arg db_obj, b_lean_obj_arg sql_obj, lean_obj_arg world) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    const char* sql = lean_string_cstr(sql_obj);
    char* err_msg = NULL;

    int rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        lean_object* err = lean_mk_string(err_msg ? err_msg : "SQL execution failed");
        if (err_msg) sqlite3_free(err_msg);
        return lean_io_result_mk_error(lean_mk_io_user_error(err));
    }

    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res quarry_db_last_insert_rowid(b_lean_obj_arg db_obj, lean_obj_arg world) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    sqlite3_int64 rowid = sqlite3_last_insert_rowid(db);
    return lean_io_result_mk_ok(lean_int_to_int((int64_t)rowid));
}

LEAN_EXPORT lean_obj_res quarry_db_changes(b_lean_obj_arg db_obj, lean_obj_arg world) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    int changes = sqlite3_changes(db);
    return lean_io_result_mk_ok(lean_int_to_int(changes));
}

LEAN_EXPORT lean_obj_res quarry_db_total_changes(b_lean_obj_arg db_obj, lean_obj_arg world) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    int total = sqlite3_total_changes(db);
    return lean_io_result_mk_ok(lean_int_to_int(total));
}

LEAN_EXPORT lean_obj_res quarry_db_busy_timeout(b_lean_obj_arg db_obj, uint32_t ms, lean_obj_arg world) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    int rc = sqlite3_busy_timeout(db, (int)ms);
    if (rc != SQLITE_OK) {
        return mk_sqlite_error(db);
    }
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res quarry_db_interrupt(b_lean_obj_arg db_obj, lean_obj_arg world) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    sqlite3_interrupt(db);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res quarry_db_is_interrupted(b_lean_obj_arg db_obj, lean_obj_arg world) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    int interrupted = sqlite3_is_interrupted(db);
    return lean_io_result_mk_ok(lean_box(interrupted ? 1 : 0));
}

/* ========================================================================== */
/* User-Defined Functions                                                      */
/* ========================================================================== */

/* Context structure for scalar UDFs */
typedef struct {
    lean_object* callback;  /* Lean function: Array Value -> IO Value */
    int nArgs;
} ScalarUdfContext;

/* Context structure for aggregate UDFs */
typedef struct {
    lean_object* init;      /* IO Value - initial accumulator */
    lean_object* step;      /* Value -> Array Value -> IO Value */
    lean_object* final_fn;  /* Value -> IO Value */
    int nArgs;
} AggregateUdfContext;

/* Destructor for scalar UDF context */
static void scalar_udf_destroy(void* ptr) {
    ScalarUdfContext* ctx = (ScalarUdfContext*)ptr;
    if (ctx) {
        if (ctx->callback) lean_dec(ctx->callback);
        free(ctx);
    }
}

/* Destructor for aggregate UDF context */
static void aggregate_udf_destroy(void* ptr) {
    AggregateUdfContext* ctx = (AggregateUdfContext*)ptr;
    if (ctx) {
        if (ctx->init) lean_dec(ctx->init);
        if (ctx->step) lean_dec(ctx->step);
        if (ctx->final_fn) lean_dec(ctx->final_fn);
        free(ctx);
    }
}

/* Convert sqlite3_value to Lean Value
 * Value is defined as:
 *   inductive Value where
 *     | null     -- tag 0
 *     | integer  -- tag 1
 *     | real     -- tag 2
 *     | text     -- tag 3
 *     | blob     -- tag 4
 */
static lean_object* sqlite_value_to_lean(sqlite3_value* val) {
    int type = sqlite3_value_type(val);
    switch (type) {
        case SQLITE_INTEGER: {
            int64_t n = sqlite3_value_int64(val);
            lean_object* obj = lean_alloc_ctor(1, 1, 0);  /* tag 1 = integer */
            lean_ctor_set(obj, 0, lean_int64_to_int(n));
            return obj;
        }
        case SQLITE_FLOAT: {
            double d = sqlite3_value_double(val);
            /* Float is stored as a scalar field, not an object field */
            lean_object* obj = lean_alloc_ctor(2, 0, sizeof(double));  /* tag 2 = real */
            lean_ctor_set_float(obj, 0, d);
            return obj;
        }
        case SQLITE_TEXT: {
            const char* s = (const char*)sqlite3_value_text(val);
            int len = sqlite3_value_bytes(val);
            lean_object* obj = lean_alloc_ctor(3, 1, 0);  /* tag 3 = text */
            lean_ctor_set(obj, 0, lean_mk_string_from_bytes(s, len));
            return obj;
        }
        case SQLITE_BLOB: {
            const void* data = sqlite3_value_blob(val);
            int size = sqlite3_value_bytes(val);
            lean_object* arr = lean_alloc_sarray(1, size, size);
            if (data && size > 0) {
                memcpy(lean_sarray_cptr(arr), data, size);
            }
            lean_object* obj = lean_alloc_ctor(4, 1, 0);  /* tag 4 = blob */
            lean_ctor_set(obj, 0, arr);
            return obj;
        }
        default:  /* SQLITE_NULL */
            return lean_alloc_ctor(0, 0, 0);  /* tag 0 = null */
    }
}

/* Set sqlite3 result from Lean Value */
static void lean_value_to_sqlite_result(sqlite3_context* ctx, lean_object* val) {
    unsigned tag = lean_obj_tag(val);
    switch (tag) {
        case 0:  /* null */
            sqlite3_result_null(ctx);
            break;
        case 1: {  /* integer */
            lean_object* n = lean_ctor_get(val, 0);
            sqlite3_result_int64(ctx, lean_int64_of_int(n));
            break;
        }
        case 2: {  /* real */
            /* Float is stored as a scalar field, not an object field */
            double d = lean_ctor_get_float(val, 0);
            sqlite3_result_double(ctx, d);
            break;
        }
        case 3: {  /* text */
            lean_object* s = lean_ctor_get(val, 0);
            const char* str = lean_string_cstr(s);
            size_t len = lean_string_size(s) - 1;
            sqlite3_result_text(ctx, str, (int)len, SQLITE_TRANSIENT);
            break;
        }
        case 4: {  /* blob */
            lean_object* arr = lean_ctor_get(val, 0);
            size_t size = lean_sarray_size(arr);
            uint8_t* data = lean_sarray_cptr(arr);
            sqlite3_result_blob(ctx, data, (int)size, SQLITE_TRANSIENT);
            break;
        }
    }
}

/* Build a Lean Array Value from sqlite3_value arguments */
static lean_object* build_args_array(int argc, sqlite3_value** argv) {
    lean_object* args = lean_mk_empty_array();
    for (int i = 0; i < argc; i++) {
        lean_object* val = sqlite_value_to_lean(argv[i]);
        args = lean_array_push(args, val);
    }
    return args;
}

/* Scalar function callback - called by SQLite when UDF is invoked */
static void scalar_function_callback(
    sqlite3_context* ctx,
    int argc,
    sqlite3_value** argv
) {
    ScalarUdfContext* udf = (ScalarUdfContext*)sqlite3_user_data(ctx);

    /* Build Array of Values */
    lean_object* args = build_args_array(argc, argv);

    /* Call Lean function: callback : Array Value -> IO Value
     * First apply the Array argument to get IO Value (a thunk),
     * then apply the world token to actually run it. */
    lean_inc(udf->callback);
    lean_object* io_action = lean_apply_1(udf->callback, args);
    lean_object* io_result = lean_apply_1(io_action, lean_io_mk_world());

    /* Extract result from IO */
    if (lean_io_result_is_ok(io_result)) {
        lean_object* value = lean_io_result_get_value(io_result);
        lean_value_to_sqlite_result(ctx, value);
        /* Note: value is borrowed from io_result, don't dec separately */
    } else {
        sqlite3_result_error(ctx, "Lean function error", -1);
    }
    lean_dec(io_result);
}

/* Aggregate step callback - called by SQLite for each row */
static void aggregate_step_callback(
    sqlite3_context* ctx,
    int argc,
    sqlite3_value** argv
) {
    AggregateUdfContext* udf = (AggregateUdfContext*)sqlite3_user_data(ctx);

    /* Get or initialize accumulator (stored in SQLite's aggregate context) */
    lean_object** acc_ptr = (lean_object**)sqlite3_aggregate_context(ctx, sizeof(lean_object*));
    if (*acc_ptr == NULL) {
        /* First call - get initial value from init function */
        lean_inc(udf->init);
        lean_object* io_result = lean_apply_1(udf->init, lean_io_mk_world());
        if (lean_io_result_is_ok(io_result)) {
            *acc_ptr = lean_io_result_get_value(io_result);
            lean_inc(*acc_ptr);  /* Keep reference in aggregate context */
        }
        lean_dec(io_result);
    }

    if (*acc_ptr == NULL) return;  /* Init failed */

    /* Build Array of Values from arguments */
    lean_object* args = build_args_array(argc, argv);

    /* Call step: Value -> Array Value -> IO Value */
    lean_inc(udf->step);
    lean_inc(*acc_ptr);
    lean_object* io_result = lean_apply_3(udf->step, *acc_ptr, args, lean_io_mk_world());

    if (lean_io_result_is_ok(io_result)) {
        lean_dec(*acc_ptr);  /* Release old accumulator */
        *acc_ptr = lean_io_result_get_value(io_result);
        lean_inc(*acc_ptr);  /* Keep new accumulator */
    }
    lean_dec(io_result);
}

/* Aggregate final callback - called by SQLite after all rows processed */
static void aggregate_final_callback(sqlite3_context* ctx) {
    AggregateUdfContext* udf = (AggregateUdfContext*)sqlite3_user_data(ctx);

    lean_object** acc_ptr = (lean_object**)sqlite3_aggregate_context(ctx, 0);
    if (acc_ptr == NULL || *acc_ptr == NULL) {
        /* No rows - return NULL */
        sqlite3_result_null(ctx);
        return;
    }

    /* Call final: Value -> IO Value */
    lean_inc(udf->final_fn);
    lean_inc(*acc_ptr);
    lean_object* io_result = lean_apply_2(udf->final_fn, *acc_ptr, lean_io_mk_world());

    if (lean_io_result_is_ok(io_result)) {
        lean_object* value = lean_io_result_get_value(io_result);
        lean_value_to_sqlite_result(ctx, value);
        /* Note: value is borrowed from io_result, don't dec separately */
    } else {
        sqlite3_result_error(ctx, "Lean aggregate final error", -1);
    }
    lean_dec(io_result);

    /* Cleanup accumulator */
    lean_dec(*acc_ptr);
    *acc_ptr = NULL;
}

/* Register a scalar function */
LEAN_EXPORT lean_obj_res quarry_db_create_scalar_function(
    b_lean_obj_arg db_obj,
    b_lean_obj_arg name_obj,
    int32_t nArgs,
    lean_obj_arg callback,  /* Array Value -> IO Value */
    lean_obj_arg world
) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    const char* name = lean_string_cstr(name_obj);

    ScalarUdfContext* ctx = (ScalarUdfContext*)malloc(sizeof(ScalarUdfContext));
    ctx->callback = callback;
    ctx->nArgs = nArgs;

    int rc = sqlite3_create_function_v2(
        db, name, nArgs, SQLITE_UTF8, ctx,
        scalar_function_callback,  /* xFunc */
        NULL, NULL,                /* xStep, xFinal (not used for scalar) */
        scalar_udf_destroy
    );

    if (rc != SQLITE_OK) {
        scalar_udf_destroy(ctx);
        return mk_sqlite_error(db);
    }
    return lean_io_result_mk_ok(lean_box(0));
}

/* Register an aggregate function */
LEAN_EXPORT lean_obj_res quarry_db_create_aggregate_function(
    b_lean_obj_arg db_obj,
    b_lean_obj_arg name_obj,
    int32_t nArgs,
    lean_obj_arg init,      /* IO Value */
    lean_obj_arg step,      /* Value -> Array Value -> IO Value */
    lean_obj_arg final_fn,  /* Value -> IO Value */
    lean_obj_arg world
) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    const char* name = lean_string_cstr(name_obj);

    AggregateUdfContext* ctx = (AggregateUdfContext*)malloc(sizeof(AggregateUdfContext));
    ctx->init = init;
    ctx->step = step;
    ctx->final_fn = final_fn;
    ctx->nArgs = nArgs;

    int rc = sqlite3_create_function_v2(
        db, name, nArgs, SQLITE_UTF8, ctx,
        NULL,                       /* xFunc (not used for aggregate) */
        aggregate_step_callback,    /* xStep */
        aggregate_final_callback,   /* xFinal */
        aggregate_udf_destroy
    );

    if (rc != SQLITE_OK) {
        aggregate_udf_destroy(ctx);
        return mk_sqlite_error(db);
    }
    return lean_io_result_mk_ok(lean_box(0));
}

/* Remove a function (works for both scalar and aggregate) */
LEAN_EXPORT lean_obj_res quarry_db_remove_function(
    b_lean_obj_arg db_obj,
    b_lean_obj_arg name_obj,
    int32_t nArgs,
    lean_obj_arg world
) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    const char* name = lean_string_cstr(name_obj);

    int rc = sqlite3_create_function_v2(
        db, name, nArgs, SQLITE_UTF8,
        NULL, NULL, NULL, NULL, NULL
    );

    if (rc != SQLITE_OK) {
        return mk_sqlite_error(db);
    }
    return lean_io_result_mk_ok(lean_box(0));
}

/* ========================================================================== */
/* Update Hook                                                                 */
/* ========================================================================== */

/* Context for update hook callback */
typedef struct {
    lean_object* callback;  /* UInt8 -> String -> Int -> IO Unit */
} UpdateHookContext;

/* Destructor for update hook context */
static void update_hook_destroy(void* ptr) {
    UpdateHookContext* ctx = (UpdateHookContext*)ptr;
    if (ctx) {
        if (ctx->callback) lean_dec(ctx->callback);
        free(ctx);
    }
}

/* C callback invoked by SQLite on INSERT/UPDATE/DELETE */
static void update_hook_callback(
    void* pArg,
    int op,
    const char* zDb,
    const char* zTable,
    sqlite3_int64 rowid
) {
    UpdateHookContext* ctx = (UpdateHookContext*)pArg;
    if (!ctx || !ctx->callback) return;

    /* Map SQLite operation codes to our enum: insert=0, update=1, delete=2 */
    uint8_t opTag;
    switch (op) {
        case SQLITE_INSERT: opTag = 0; break;
        case SQLITE_UPDATE: opTag = 1; break;
        case SQLITE_DELETE: opTag = 2; break;
        default: return;
    }

    /* Call: UInt8 -> String -> Int -> IO Unit */
    lean_inc(ctx->callback);
    lean_object* io_action = lean_apply_3(
        ctx->callback,
        lean_box(opTag),
        lean_mk_string(zTable),
        lean_int64_to_int(rowid)
    );
    lean_object* io_result = lean_apply_1(io_action, lean_io_mk_world());
    lean_dec(io_result);
}

/* Set update hook - returns Unit */
LEAN_EXPORT lean_obj_res quarry_db_set_update_hook(
    b_lean_obj_arg db_obj,
    lean_obj_arg callback,
    lean_obj_arg world
) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);

    UpdateHookContext* ctx = (UpdateHookContext*)malloc(sizeof(UpdateHookContext));
    ctx->callback = callback;

    /* Register hook - returns old user data pointer */
    void* old_ctx = sqlite3_update_hook(db, update_hook_callback, ctx);

    /* Free old context if there was one */
    if (old_ctx) {
        update_hook_destroy(old_ctx);
    }

    return lean_io_result_mk_ok(lean_box(0));
}

/* Clear update hook */
LEAN_EXPORT lean_obj_res quarry_db_clear_update_hook(
    b_lean_obj_arg db_obj,
    lean_obj_arg world
) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);

    /* Pass NULL to clear hook - returns old user data */
    void* old_ctx = sqlite3_update_hook(db, NULL, NULL);

    if (old_ctx) {
        update_hook_destroy(old_ctx);
    }

    return lean_io_result_mk_ok(lean_box(0));
}

/* ========================================================================== */
/* Serialize/Deserialize                                                       */
/* ========================================================================== */

/* Serialize database to ByteArray */
LEAN_EXPORT lean_obj_res quarry_db_serialize(
    b_lean_obj_arg db_obj,
    b_lean_obj_arg schema_obj,
    lean_obj_arg world
) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    const char* schema = lean_string_cstr(schema_obj);
    sqlite3_int64 size = 0;

    unsigned char* data = sqlite3_serialize(db, schema, &size, 0);
    if (data == NULL && size == 0) {
        /* Empty database - return empty ByteArray */
        lean_object* arr = lean_alloc_sarray(1, 0, 0);
        return lean_io_result_mk_ok(arr);
    }
    if (data == NULL) {
        return lean_io_result_mk_error(
            lean_mk_io_user_error(lean_mk_string("Failed to serialize database"))
        );
    }

    /* Copy to Lean ByteArray */
    lean_object* arr = lean_alloc_sarray(1, (size_t)size, (size_t)size);
    memcpy(lean_sarray_cptr(arr), data, (size_t)size);
    sqlite3_free(data);

    return lean_io_result_mk_ok(arr);
}

/* Deserialize ByteArray into database connection */
LEAN_EXPORT lean_obj_res quarry_db_deserialize(
    b_lean_obj_arg db_obj,
    b_lean_obj_arg schema_obj,
    b_lean_obj_arg data_obj,
    uint8_t readOnly,
    lean_obj_arg world
) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    const char* schema = lean_string_cstr(schema_obj);
    size_t size = lean_sarray_size(data_obj);

    /* Allocate buffer with sqlite3_malloc64 so SQLite can take ownership */
    unsigned char* buf = (unsigned char*)sqlite3_malloc64(size);
    if (buf == NULL && size > 0) {
        return lean_io_result_mk_error(
            lean_mk_io_user_error(lean_mk_string("Failed to allocate memory for deserialize"))
        );
    }

    /* Copy data from Lean ByteArray */
    if (size > 0) {
        memcpy(buf, lean_sarray_cptr(data_obj), size);
    }

    /* Set flags - always FREEONCLOSE so SQLite manages memory */
    unsigned flags = SQLITE_DESERIALIZE_FREEONCLOSE;
    if (!readOnly) {
        flags |= SQLITE_DESERIALIZE_RESIZEABLE;
    } else {
        flags |= SQLITE_DESERIALIZE_READONLY;
    }

    int rc = sqlite3_deserialize(db, schema, buf, size, size, flags);
    if (rc != SQLITE_OK) {
        /* Note: SQLite already freed buf on failure when FREEONCLOSE is set */
        return mk_sqlite_error(db);
    }

    return lean_io_result_mk_ok(lean_box(0));
}

/* ========================================================================== */
/* Statement Operations                                                        */
/* ========================================================================== */

LEAN_EXPORT lean_obj_res quarry_stmt_prepare(b_lean_obj_arg db_obj, b_lean_obj_arg sql_obj, lean_obj_arg world) {
    init_external_classes();

    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    const char* sql = lean_string_cstr(sql_obj);
    sqlite3_stmt* stmt = NULL;

    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        return mk_sqlite_error(db);
    }

    lean_object* obj = lean_alloc_external(g_statement_class, stmt);
    return lean_io_result_mk_ok(obj);
}

LEAN_EXPORT lean_obj_res quarry_stmt_finalize(b_lean_obj_arg stmt_obj, lean_obj_arg world) {
    /* Intentionally a no-op: finalizer handles cleanup */
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res quarry_stmt_reset(b_lean_obj_arg stmt_obj, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    sqlite3_reset(stmt);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res quarry_stmt_clear_bindings(b_lean_obj_arg stmt_obj, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    sqlite3_clear_bindings(stmt);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res quarry_stmt_step(b_lean_obj_arg stmt_obj, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    int rc = sqlite3_step(stmt);
    return lean_io_result_mk_ok(lean_int_to_int(rc));
}

/* ========================================================================== */
/* Parameter Binding                                                           */
/* ========================================================================== */

LEAN_EXPORT lean_obj_res quarry_stmt_bind_null(b_lean_obj_arg stmt_obj, uint32_t idx, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    int rc = sqlite3_bind_null(stmt, (int)idx);
    if (rc != SQLITE_OK) {
        sqlite3* db = sqlite3_db_handle(stmt);
        return mk_sqlite_error(db);
    }
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res quarry_stmt_bind_int(b_lean_obj_arg stmt_obj, uint32_t idx, b_lean_obj_arg value, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    int64_t v = (int64_t)lean_int64_of_int(value);
    int rc = sqlite3_bind_int64(stmt, (int)idx, v);
    if (rc != SQLITE_OK) {
        sqlite3* db = sqlite3_db_handle(stmt);
        return mk_sqlite_error(db);
    }
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res quarry_stmt_bind_double(b_lean_obj_arg stmt_obj, uint32_t idx, double value, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    int rc = sqlite3_bind_double(stmt, (int)idx, value);
    if (rc != SQLITE_OK) {
        sqlite3* db = sqlite3_db_handle(stmt);
        return mk_sqlite_error(db);
    }
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res quarry_stmt_bind_text(b_lean_obj_arg stmt_obj, uint32_t idx, b_lean_obj_arg value, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    const char* str = lean_string_cstr(value);
    size_t len = lean_string_size(value) - 1;
    int rc = sqlite3_bind_text(stmt, (int)idx, str, (int)len, SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) {
        sqlite3* db = sqlite3_db_handle(stmt);
        return mk_sqlite_error(db);
    }
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res quarry_stmt_bind_blob(b_lean_obj_arg stmt_obj, uint32_t idx, b_lean_obj_arg value, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    size_t size = lean_sarray_size(value);
    uint8_t* data = lean_sarray_cptr(value);
    int rc = sqlite3_bind_blob(stmt, (int)idx, data, (int)size, SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) {
        sqlite3* db = sqlite3_db_handle(stmt);
        return mk_sqlite_error(db);
    }
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res quarry_stmt_bind_parameter_index(b_lean_obj_arg stmt_obj, b_lean_obj_arg name, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    const char* param_name = lean_string_cstr(name);
    int idx = sqlite3_bind_parameter_index(stmt, param_name);
    return lean_io_result_mk_ok(lean_int_to_int(idx));
}

LEAN_EXPORT lean_obj_res quarry_stmt_bind_parameter_count(b_lean_obj_arg stmt_obj, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    int count = sqlite3_bind_parameter_count(stmt);
    return lean_io_result_mk_ok(lean_box_uint32((uint32_t)count));
}

/* ========================================================================== */
/* Column Access                                                               */
/* ========================================================================== */

LEAN_EXPORT lean_obj_res quarry_stmt_column_count(b_lean_obj_arg stmt_obj, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    int count = sqlite3_column_count(stmt);
    return lean_io_result_mk_ok(lean_box_uint32((uint32_t)count));
}

LEAN_EXPORT lean_obj_res quarry_stmt_column_type(b_lean_obj_arg stmt_obj, uint32_t idx, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    int type = sqlite3_column_type(stmt, (int)idx);
    return lean_io_result_mk_ok(lean_int_to_int(type));
}

LEAN_EXPORT lean_obj_res quarry_stmt_column_name(b_lean_obj_arg stmt_obj, uint32_t idx, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    const char* name = sqlite3_column_name(stmt, (int)idx);
    return lean_io_result_mk_ok(lean_mk_string(name ? name : ""));
}

/* Helper to create Option.none (tag 0, no fields) */
static inline lean_object* mk_option_none(void) {
    return lean_box(0);
}

/* Helper to create Option.some x (tag 1, one field) */
static inline lean_object* mk_option_some(lean_object* val) {
    lean_object* obj = lean_alloc_ctor(1, 1, 0);
    lean_ctor_set(obj, 0, val);
    return obj;
}

/* Column metadata - returns Option String (none if NULL) */
LEAN_EXPORT lean_obj_res quarry_stmt_column_database_name(b_lean_obj_arg stmt_obj, uint32_t idx, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    const char* name = sqlite3_column_database_name(stmt, (int)idx);
    if (name == NULL) {
        return lean_io_result_mk_ok(mk_option_none());
    }
    return lean_io_result_mk_ok(mk_option_some(lean_mk_string(name)));
}

LEAN_EXPORT lean_obj_res quarry_stmt_column_table_name(b_lean_obj_arg stmt_obj, uint32_t idx, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    const char* name = sqlite3_column_table_name(stmt, (int)idx);
    if (name == NULL) {
        return lean_io_result_mk_ok(mk_option_none());
    }
    return lean_io_result_mk_ok(mk_option_some(lean_mk_string(name)));
}

LEAN_EXPORT lean_obj_res quarry_stmt_column_origin_name(b_lean_obj_arg stmt_obj, uint32_t idx, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    const char* name = sqlite3_column_origin_name(stmt, (int)idx);
    if (name == NULL) {
        return lean_io_result_mk_ok(mk_option_none());
    }
    return lean_io_result_mk_ok(mk_option_some(lean_mk_string(name)));
}

LEAN_EXPORT lean_obj_res quarry_stmt_column_int(b_lean_obj_arg stmt_obj, uint32_t idx, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    sqlite3_int64 value = sqlite3_column_int64(stmt, (int)idx);
    return lean_io_result_mk_ok(lean_int64_to_int(value));
}

LEAN_EXPORT lean_obj_res quarry_stmt_column_double(b_lean_obj_arg stmt_obj, uint32_t idx, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    double value = sqlite3_column_double(stmt, (int)idx);
    return lean_io_result_mk_ok(lean_box_float(value));
}

LEAN_EXPORT lean_obj_res quarry_stmt_column_text(b_lean_obj_arg stmt_obj, uint32_t idx, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    const unsigned char* text = sqlite3_column_text(stmt, (int)idx);
    int len = sqlite3_column_bytes(stmt, (int)idx);
    if (text == NULL || len == 0) {
        return lean_io_result_mk_ok(lean_mk_string(""));
    }
    return lean_io_result_mk_ok(lean_mk_string_from_bytes((const char*)text, len));
}

LEAN_EXPORT lean_obj_res quarry_stmt_column_blob(b_lean_obj_arg stmt_obj, uint32_t idx, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    const void* data = sqlite3_column_blob(stmt, (int)idx);
    int size = sqlite3_column_bytes(stmt, (int)idx);

    lean_object* arr = lean_alloc_sarray(1, size, size);
    if (data && size > 0) {
        memcpy(lean_sarray_cptr(arr), data, size);
    }
    return lean_io_result_mk_ok(arr);
}

LEAN_EXPORT lean_obj_res quarry_stmt_column_bytes(b_lean_obj_arg stmt_obj, uint32_t idx, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    int bytes = sqlite3_column_bytes(stmt, (int)idx);
    return lean_io_result_mk_ok(lean_int_to_int(bytes));
}

LEAN_EXPORT lean_obj_res quarry_stmt_sql(b_lean_obj_arg stmt_obj, lean_obj_arg world) {
    sqlite3_stmt* stmt = (sqlite3_stmt*)lean_get_external_data(stmt_obj);
    const char* sql = sqlite3_sql(stmt);
    return lean_io_result_mk_ok(lean_mk_string(sql ? sql : ""));
}

/* ========================================================================== */
/* Backup Operations                                                           */
/* ========================================================================== */

/* Initialize a backup from source to destination database.
 * Both databases must be open. The backup copies from srcDb to destDb.
 * srcName and destName are typically "main" for the main database. */
LEAN_EXPORT lean_obj_res quarry_backup_init(
    b_lean_obj_arg dest_db_obj,
    b_lean_obj_arg dest_name_obj,
    b_lean_obj_arg src_db_obj,
    b_lean_obj_arg src_name_obj,
    lean_obj_arg world
) {
    init_external_classes();

    sqlite3* destDb = (sqlite3*)lean_get_external_data(dest_db_obj);
    sqlite3* srcDb = (sqlite3*)lean_get_external_data(src_db_obj);
    const char* destName = lean_string_cstr(dest_name_obj);
    const char* srcName = lean_string_cstr(src_name_obj);

    sqlite3_backup* backup = sqlite3_backup_init(destDb, destName, srcDb, srcName);
    if (backup == NULL) {
        return mk_sqlite_error(destDb);
    }

    BackupWrapper* wrapper = (BackupWrapper*)malloc(sizeof(BackupWrapper));
    if (wrapper == NULL) {
        sqlite3_backup_finish(backup);
        return mk_io_error("Failed to allocate backup wrapper");
    }
    wrapper->backup = backup;
    wrapper->finished = 0;

    lean_object* obj = lean_alloc_external(g_backup_class, wrapper);
    return lean_io_result_mk_ok(obj);
}

/* Perform a step of the backup, copying up to nPages pages.
 * Use -1 to copy all remaining pages in one step.
 * Returns the SQLite result code:
 *   SQLITE_OK (0) - more pages to copy
 *   SQLITE_DONE (101) - backup complete
 *   Other - error occurred */
LEAN_EXPORT lean_obj_res quarry_backup_step(b_lean_obj_arg backup_obj, int32_t nPages, lean_obj_arg world) {
    BackupWrapper* wrapper = (BackupWrapper*)lean_get_external_data(backup_obj);
    if (wrapper == NULL || wrapper->backup == NULL || wrapper->finished) {
        return mk_io_error("Backup handle is invalid or already finished");
    }
    int rc = sqlite3_backup_step(wrapper->backup, nPages);
    return lean_io_result_mk_ok(lean_int_to_int(rc));
}

/* Finish and release the backup handle.
 * Returns SQLITE_OK on success. */
LEAN_EXPORT lean_obj_res quarry_backup_finish(b_lean_obj_arg backup_obj, lean_obj_arg world) {
    BackupWrapper* wrapper = (BackupWrapper*)lean_get_external_data(backup_obj);
    if (wrapper == NULL) {
        return mk_io_error("Backup wrapper is NULL");
    }
    if (wrapper->finished) {
        /* Already finished, return OK */
        return lean_io_result_mk_ok(lean_int_to_int(0));
    }
    int rc = sqlite3_backup_finish(wrapper->backup);
    wrapper->backup = NULL;
    wrapper->finished = 1;
    return lean_io_result_mk_ok(lean_int_to_int(rc));
}

/* Get the number of pages remaining to be backed up */
LEAN_EXPORT lean_obj_res quarry_backup_remaining(b_lean_obj_arg backup_obj, lean_obj_arg world) {
    BackupWrapper* wrapper = (BackupWrapper*)lean_get_external_data(backup_obj);
    if (wrapper == NULL || wrapper->backup == NULL) {
        return lean_io_result_mk_ok(lean_int_to_int(0));
    }
    int remaining = sqlite3_backup_remaining(wrapper->backup);
    return lean_io_result_mk_ok(lean_int_to_int(remaining));
}

/* Get the total number of pages in the source database */
LEAN_EXPORT lean_obj_res quarry_backup_page_count(b_lean_obj_arg backup_obj, lean_obj_arg world) {
    BackupWrapper* wrapper = (BackupWrapper*)lean_get_external_data(backup_obj);
    if (wrapper == NULL || wrapper->backup == NULL) {
        return lean_io_result_mk_ok(lean_int_to_int(0));
    }
    int total = sqlite3_backup_pagecount(wrapper->backup);
    return lean_io_result_mk_ok(lean_int_to_int(total));
}

/* ========================================================================== */
/* Incremental BLOB I/O                                                        */
/* ========================================================================== */

/* Open a blob for incremental I/O.
 * flags: 0 = read-only, 1 = read-write */
LEAN_EXPORT lean_obj_res quarry_blob_open(
    b_lean_obj_arg db_obj,
    b_lean_obj_arg db_name_obj,
    b_lean_obj_arg table_obj,
    b_lean_obj_arg column_obj,
    b_lean_obj_arg rowid_obj,
    uint8_t flags,
    lean_obj_arg world
) {
    init_external_classes();

    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    const char* db_name = lean_string_cstr(db_name_obj);
    const char* table = lean_string_cstr(table_obj);
    const char* column = lean_string_cstr(column_obj);
    int64_t rowid = lean_int64_of_int(rowid_obj);

    sqlite3_blob* blob = NULL;
    int rc = sqlite3_blob_open(db, db_name, table, column, rowid, flags, &blob);
    if (rc != SQLITE_OK) {
        return mk_sqlite_error(db);
    }

    BlobWrapper* wrapper = (BlobWrapper*)malloc(sizeof(BlobWrapper));
    if (wrapper == NULL) {
        sqlite3_blob_close(blob);
        return mk_io_error("Failed to allocate blob wrapper");
    }
    wrapper->blob = blob;
    wrapper->closed = 0;

    lean_object* obj = lean_alloc_external(g_blob_class, wrapper);
    return lean_io_result_mk_ok(obj);
}

/* Read bytes from blob at offset */
LEAN_EXPORT lean_obj_res quarry_blob_read(
    b_lean_obj_arg blob_obj,
    uint32_t offset,
    uint32_t size,
    lean_obj_arg world
) {
    BlobWrapper* wrapper = (BlobWrapper*)lean_get_external_data(blob_obj);
    if (wrapper == NULL || wrapper->blob == NULL || wrapper->closed) {
        return mk_io_error("Blob handle is invalid or closed");
    }

    /* Allocate ByteArray for result */
    lean_object* arr = lean_alloc_sarray(1, size, size);
    uint8_t* data = lean_sarray_cptr(arr);

    int rc = sqlite3_blob_read(wrapper->blob, data, (int)size, (int)offset);
    if (rc != SQLITE_OK) {
        lean_dec(arr);
        return mk_io_error("Blob read failed");
    }

    return lean_io_result_mk_ok(arr);
}

/* Write bytes to blob at offset */
LEAN_EXPORT lean_obj_res quarry_blob_write(
    b_lean_obj_arg blob_obj,
    uint32_t offset,
    b_lean_obj_arg data_obj,
    lean_obj_arg world
) {
    BlobWrapper* wrapper = (BlobWrapper*)lean_get_external_data(blob_obj);
    if (wrapper == NULL || wrapper->blob == NULL || wrapper->closed) {
        return mk_io_error("Blob handle is invalid or closed");
    }

    size_t size = lean_sarray_size(data_obj);
    uint8_t* data = lean_sarray_cptr(data_obj);

    int rc = sqlite3_blob_write(wrapper->blob, data, (int)size, (int)offset);
    if (rc != SQLITE_OK) {
        return mk_io_error("Blob write failed");
    }

    return lean_io_result_mk_ok(lean_box(0));
}

/* Get total blob size in bytes */
LEAN_EXPORT lean_obj_res quarry_blob_bytes(b_lean_obj_arg blob_obj, lean_obj_arg world) {
    BlobWrapper* wrapper = (BlobWrapper*)lean_get_external_data(blob_obj);
    if (wrapper == NULL || wrapper->blob == NULL || wrapper->closed) {
        return lean_io_result_mk_ok(lean_int_to_int(0));
    }

    int size = sqlite3_blob_bytes(wrapper->blob);
    return lean_io_result_mk_ok(lean_int_to_int(size));
}

/* Close blob handle explicitly */
LEAN_EXPORT lean_obj_res quarry_blob_close(b_lean_obj_arg blob_obj, lean_obj_arg world) {
    BlobWrapper* wrapper = (BlobWrapper*)lean_get_external_data(blob_obj);
    if (wrapper == NULL) {
        return mk_io_error("Blob wrapper is NULL");
    }
    if (wrapper->closed) {
        /* Already closed, return OK (idempotent) */
        return lean_io_result_mk_ok(lean_box(0));
    }

    int rc = sqlite3_blob_close(wrapper->blob);
    wrapper->blob = NULL;
    wrapper->closed = 1;

    if (rc != SQLITE_OK) {
        return mk_io_error("Blob close failed");
    }
    return lean_io_result_mk_ok(lean_box(0));
}

/* Reopen blob for a different row (reuses handle) */
LEAN_EXPORT lean_obj_res quarry_blob_reopen(
    b_lean_obj_arg blob_obj,
    b_lean_obj_arg rowid_obj,
    lean_obj_arg world
) {
    BlobWrapper* wrapper = (BlobWrapper*)lean_get_external_data(blob_obj);
    if (wrapper == NULL || wrapper->blob == NULL || wrapper->closed) {
        return mk_io_error("Blob handle is invalid or closed");
    }

    int64_t rowid = lean_int64_of_int(rowid_obj);
    int rc = sqlite3_blob_reopen(wrapper->blob, rowid);
    if (rc != SQLITE_OK) {
        return mk_io_error("Blob reopen failed");
    }

    return lean_io_result_mk_ok(lean_box(0));
}

/* ========================================================================== */
/* Virtual Tables                                                              */
/* ========================================================================== */

/* Context for a virtual table module - holds Lean callbacks */
typedef struct {
    lean_object* table_data;      /* τ - the table instance */
    lean_object* schema_fn;       /* τ → VTableSchema */
    lean_object* best_index_fn;   /* τ → VTableIndexInfo → IO VTableIndexOutput */
    lean_object* open_fn;         /* τ → Int → Array Value → IO σ */
    lean_object* eof_fn;          /* σ → IO Bool */
    lean_object* next_fn;         /* σ → IO σ */
    lean_object* column_fn;       /* σ → Nat → IO Value */
    lean_object* rowid_fn;        /* σ → IO Int */
    lean_object* update_fn;       /* τ → VTableUpdateOp → IO (Option Int) - may be null */
} VTableModuleContext;

/* Per-table instance (created by xCreate/xConnect) */
typedef struct {
    sqlite3_vtab base;            /* SQLite requires this first */
    VTableModuleContext* module;  /* Back-reference to module */
} QuarryVTab;

/* Cursor instance (created by xOpen) */
typedef struct {
    sqlite3_vtab_cursor base;     /* SQLite requires this first */
    lean_object* cursor_state;    /* σ - Lean cursor state */
    QuarryVTab* vtab;             /* Back-reference to table */
} QuarryVTabCursor;

/* Convert VTableSchema to CREATE TABLE SQL string */
static char* vtab_schema_to_sql(lean_object* schema) {
    /* VTableSchema is a single-field structure (columns : Array VTableColumn)
     * Lean optimizes this to just be the array directly at runtime.
     * VTableColumn has: name : String, sqlType : String, isHidden : Bool */
    lean_object* columns = schema;  /* Schema IS the columns array */
    size_t num_cols = lean_array_size(columns);

    /* Start building the SQL */
    char* result = (char*)malloc(4096);
    strcpy(result, "CREATE TABLE x(");

    for (size_t i = 0; i < num_cols; i++) {
        lean_object* col = lean_array_get_core(columns, i);
        lean_object* name_obj = lean_ctor_get(col, 0);
        lean_object* type_obj = lean_ctor_get(col, 1);
        /* Skip isHidden for now - just assume false */
        uint8_t is_hidden = 0;

        const char* name = lean_string_cstr(name_obj);
        const char* type = lean_string_cstr(type_obj);

        if (i > 0) strcat(result, ", ");
        strcat(result, name);
        strcat(result, " ");
        strcat(result, type);
        if (is_hidden) strcat(result, " HIDDEN");
    }
    strcat(result, ")");

    return result;
}

/* xCreate/xConnect - Initialize virtual table instance */
static int vtab_create(
    sqlite3* db,
    void* pAux,
    int argc,
    const char* const* argv,
    sqlite3_vtab** ppVTab,
    char** pzErr
) {
    VTableModuleContext* ctx = (VTableModuleContext*)pAux;

    /* Get schema from Lean - schema_fn : τ → IO VTableSchema */
    lean_inc(ctx->schema_fn);
    lean_inc(ctx->table_data);

    lean_object* io_action = lean_apply_1(ctx->schema_fn, ctx->table_data);
    lean_object* io_result = lean_apply_1(io_action, lean_io_mk_world());

    /* Check for IO error */
    if (lean_io_result_is_error(io_result)) {
        lean_object* err = lean_io_result_get_error(io_result);
        *pzErr = sqlite3_mprintf("Schema error: %s", lean_string_cstr(lean_ctor_get(err, 0)));
        lean_dec(io_result);
        return SQLITE_ERROR;
    }

    lean_object* schema = lean_io_result_get_value(io_result);
    lean_inc(schema);  /* Keep a reference since io_result will be dec'd */
    lean_dec(io_result);

    /* Convert schema to CREATE TABLE statement */
    char* create_sql = vtab_schema_to_sql(schema);
    lean_dec(schema);

    /* Declare the schema to SQLite */
    int rc = sqlite3_declare_vtab(db, create_sql);
    free(create_sql);

    if (rc != SQLITE_OK) {
        *pzErr = sqlite3_mprintf("Failed to declare vtab schema: %s", sqlite3_errmsg(db));
        return rc;
    }

    /* Allocate vtab structure */
    QuarryVTab* vtab = (QuarryVTab*)sqlite3_malloc(sizeof(QuarryVTab));
    memset(vtab, 0, sizeof(QuarryVTab));
    vtab->module = ctx;

    *ppVTab = &vtab->base;
    return SQLITE_OK;
}

/* Build VTableIndexInfo from sqlite3_index_info
 * VTableIndexInfo has:
 *   constraints : Array VTableConstraint  (Array of (column, op, usable))
 *   orderBy : Array VTableOrderBy         (Array of (column, desc))
 *
 * VTableConstraint has: column : Nat, op : VTableOp, usable : Bool
 * VTableOrderBy has: column : Nat, desc : Bool
 */
static lean_object* build_index_info(sqlite3_index_info* pIdxInfo) {
    /* Build constraints array */
    lean_object* constraints = lean_mk_empty_array();
    for (int i = 0; i < pIdxInfo->nConstraint; i++) {
        struct sqlite3_index_constraint* c = &pIdxInfo->aConstraint[i];

        /* Create VTableConstraint structure */
        lean_object* constraint = lean_alloc_ctor(0, 3, 0);
        lean_ctor_set(constraint, 0, lean_box(c->iColumn >= 0 ? c->iColumn : 0));  /* column : Nat */
        lean_ctor_set(constraint, 1, lean_box(c->op));  /* op : VTableOp (raw value) */
        lean_ctor_set(constraint, 2, lean_box(c->usable ? 1 : 0));  /* usable : Bool */

        constraints = lean_array_push(constraints, constraint);
    }

    /* Build orderBy array */
    lean_object* order_by = lean_mk_empty_array();
    for (int i = 0; i < pIdxInfo->nOrderBy; i++) {
        struct sqlite3_index_orderby* o = &pIdxInfo->aOrderBy[i];

        /* Create VTableOrderBy structure */
        lean_object* order = lean_alloc_ctor(0, 2, 0);
        lean_ctor_set(order, 0, lean_box(o->iColumn));  /* column : Nat */
        lean_ctor_set(order, 1, lean_box(o->desc ? 1 : 0));  /* desc : Bool */

        order_by = lean_array_push(order_by, order);
    }

    /* Create VTableIndexInfo structure */
    lean_object* index_info = lean_alloc_ctor(0, 2, 0);
    lean_ctor_set(index_info, 0, constraints);
    lean_ctor_set(index_info, 1, order_by);

    return index_info;
}

/* Apply VTableIndexOutput to sqlite3_index_info
 *
 * Note: VTableIndexOutput has a complex layout with mixed boxed/unboxed fields.
 * For simplicity, we just set reasonable defaults for a full table scan.
 * The query planner will still work, just without optimizations.
 */
static void apply_index_output(sqlite3_index_info* pIdxInfo, lean_object* output) {
    (void)output;  /* We're not extracting fields due to layout complexity */

    /* Set defaults for full table scan */
    pIdxInfo->idxNum = 0;
    pIdxInfo->estimatedCost = 1000000.0;
    pIdxInfo->estimatedRows = 1000000;
}

/* xBestIndex - Query planning */
static int vtab_best_index(
    sqlite3_vtab* pVTab,
    sqlite3_index_info* pIdxInfo
) {
    QuarryVTab* vtab = (QuarryVTab*)pVTab;
    VTableModuleContext* ctx = vtab->module;

    /* Build VTableIndexInfo from sqlite3_index_info */
    lean_object* index_info = build_index_info(pIdxInfo);

    /* Call Lean bestIndex function: τ → VTableIndexInfo → IO VTableIndexOutput */
    lean_inc(ctx->best_index_fn);
    lean_inc(ctx->table_data);
    lean_object* io_action = lean_apply_2(ctx->best_index_fn, ctx->table_data, index_info);
    lean_object* io_result = lean_apply_1(io_action, lean_io_mk_world());

    if (!lean_io_result_is_ok(io_result)) {
        lean_dec(io_result);
        return SQLITE_ERROR;
    }

    lean_object* output = lean_io_result_get_value(io_result);

    /* Apply output to sqlite3_index_info */
    apply_index_output(pIdxInfo, output);

    lean_dec(io_result);
    return SQLITE_OK;
}

/* xOpen - Create cursor */
static int vtab_open(
    sqlite3_vtab* pVTab,
    sqlite3_vtab_cursor** ppCursor
) {
    QuarryVTab* vtab = (QuarryVTab*)pVTab;

    QuarryVTabCursor* cursor = (QuarryVTabCursor*)sqlite3_malloc(sizeof(QuarryVTabCursor));
    memset(cursor, 0, sizeof(QuarryVTabCursor));
    cursor->vtab = vtab;
    cursor->cursor_state = NULL;  /* Will be set in xFilter */

    *ppCursor = &cursor->base;
    return SQLITE_OK;
}

/* xClose - Destroy cursor */
static int vtab_close(sqlite3_vtab_cursor* pCursor) {
    QuarryVTabCursor* cursor = (QuarryVTabCursor*)pCursor;

    if (cursor->cursor_state) {
        lean_dec(cursor->cursor_state);
    }

    sqlite3_free(cursor);
    return SQLITE_OK;
}

/* xFilter - Initialize cursor for query */
static int vtab_filter(
    sqlite3_vtab_cursor* pCursor,
    int idxNum,
    const char* idxStr,
    int argc,
    sqlite3_value** argv
) {
    QuarryVTabCursor* cursor = (QuarryVTabCursor*)pCursor;
    VTableModuleContext* ctx = cursor->vtab->module;

    /* Clean up previous cursor state */
    if (cursor->cursor_state) {
        lean_dec(cursor->cursor_state);
        cursor->cursor_state = NULL;
    }

    /* Build array of filter arguments */
    lean_object* args = build_args_array(argc, argv);

    /* Call Lean open function: τ → Int → Array Value → IO σ */
    lean_inc(ctx->open_fn);
    lean_inc(ctx->table_data);
    lean_object* io_action = lean_apply_3(
        ctx->open_fn,
        ctx->table_data,
        lean_int64_to_int(idxNum),
        args
    );
    lean_object* io_result = lean_apply_1(io_action, lean_io_mk_world());

    if (!lean_io_result_is_ok(io_result)) {
        lean_dec(io_result);
        cursor->vtab->base.zErrMsg = sqlite3_mprintf("Lean vtab open error");
        return SQLITE_ERROR;
    }

    cursor->cursor_state = lean_io_result_get_value(io_result);
    lean_inc(cursor->cursor_state);  /* Keep reference */
    lean_dec(io_result);

    return SQLITE_OK;
}

/* xNext - Advance cursor */
static int vtab_next(sqlite3_vtab_cursor* pCursor) {
    QuarryVTabCursor* cursor = (QuarryVTabCursor*)pCursor;
    VTableModuleContext* ctx = cursor->vtab->module;

    if (!cursor->cursor_state) {
        return SQLITE_ERROR;
    }

    /* Call Lean next function: σ → IO σ */
    lean_inc(ctx->next_fn);
    lean_inc(cursor->cursor_state);
    lean_object* io_action = lean_apply_1(ctx->next_fn, cursor->cursor_state);
    lean_object* io_result = lean_apply_1(io_action, lean_io_mk_world());

    if (!lean_io_result_is_ok(io_result)) {
        lean_dec(io_result);
        return SQLITE_ERROR;
    }

    lean_object* new_state = lean_io_result_get_value(io_result);
    lean_inc(new_state);
    lean_dec(cursor->cursor_state);
    cursor->cursor_state = new_state;
    lean_dec(io_result);

    return SQLITE_OK;
}

/* xEof - Check if at end */
static int vtab_eof(sqlite3_vtab_cursor* pCursor) {
    QuarryVTabCursor* cursor = (QuarryVTabCursor*)pCursor;
    VTableModuleContext* ctx = cursor->vtab->module;

    if (!cursor->cursor_state) {
        return 1;  /* No state = EOF */
    }

    /* Call Lean eof function: σ → IO Bool */
    lean_inc(ctx->eof_fn);
    lean_inc(cursor->cursor_state);
    lean_object* io_action = lean_apply_1(ctx->eof_fn, cursor->cursor_state);
    lean_object* io_result = lean_apply_1(io_action, lean_io_mk_world());

    int eof = 1;  /* Default to EOF on error */
    if (lean_io_result_is_ok(io_result)) {
        lean_object* result = lean_io_result_get_value(io_result);
        eof = lean_unbox(result) ? 1 : 0;
    }
    lean_dec(io_result);

    return eof;
}

/* xColumn - Get column value */
static int vtab_column(
    sqlite3_vtab_cursor* pCursor,
    sqlite3_context* ctx,
    int iCol
) {
    QuarryVTabCursor* cursor = (QuarryVTabCursor*)pCursor;
    VTableModuleContext* mod_ctx = cursor->vtab->module;

    if (!cursor->cursor_state) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    /* Call Lean column function: σ → Nat → IO Value */
    lean_inc(mod_ctx->column_fn);
    lean_inc(cursor->cursor_state);
    lean_object* io_action = lean_apply_2(
        mod_ctx->column_fn,
        cursor->cursor_state,
        lean_box(iCol)  /* Nat */
    );
    lean_object* io_result = lean_apply_1(io_action, lean_io_mk_world());

    if (lean_io_result_is_ok(io_result)) {
        lean_object* value = lean_io_result_get_value(io_result);
        lean_value_to_sqlite_result(ctx, value);
    } else {
        sqlite3_result_null(ctx);
    }
    lean_dec(io_result);

    return SQLITE_OK;
}

/* xRowid - Get current rowid */
static int vtab_rowid(
    sqlite3_vtab_cursor* pCursor,
    sqlite3_int64* pRowid
) {
    QuarryVTabCursor* cursor = (QuarryVTabCursor*)pCursor;
    VTableModuleContext* ctx = cursor->vtab->module;

    if (!cursor->cursor_state) {
        *pRowid = 0;
        return SQLITE_OK;
    }

    /* Call Lean rowid function: σ → IO Int */
    lean_inc(ctx->rowid_fn);
    lean_inc(cursor->cursor_state);
    lean_object* io_action = lean_apply_1(ctx->rowid_fn, cursor->cursor_state);
    lean_object* io_result = lean_apply_1(io_action, lean_io_mk_world());

    if (lean_io_result_is_ok(io_result)) {
        lean_object* value = lean_io_result_get_value(io_result);
        *pRowid = lean_int64_of_int(value);
    } else {
        *pRowid = 0;
    }
    lean_dec(io_result);

    return SQLITE_OK;
}

/* xUpdate - Handle INSERT/UPDATE/DELETE
 * argc == 1: DELETE (argv[0] = rowid to delete)
 * argc > 1 && argv[0] == NULL: INSERT (argv[1] = new rowid or NULL, argv[2..] = values)
 * argc > 1 && argv[0] != NULL: UPDATE (argv[0] = old rowid, argv[1] = new rowid, argv[2..] = values)
 *
 * VTableUpdateOp is:
 *   | delete (rowid : Int)                                    -- tag 0
 *   | insert (rowid : Option Int) (values : Array Value)      -- tag 1
 *   | update (oldRowid : Int) (newRowid : Int) (values : Array Value)  -- tag 2
 */
static int vtab_update(
    sqlite3_vtab* pVTab,
    int argc,
    sqlite3_value** argv,
    sqlite3_int64* pRowid
) {
    QuarryVTab* vtab = (QuarryVTab*)pVTab;
    VTableModuleContext* ctx = vtab->module;

    /* Check if updates are supported */
    if (!ctx->update_fn || lean_obj_tag(ctx->update_fn) == 0) {
        vtab->base.zErrMsg = sqlite3_mprintf("Virtual table is read-only");
        return SQLITE_READONLY;
    }

    lean_object* update_op;

    if (argc == 1) {
        /* DELETE */
        int64_t rowid = sqlite3_value_int64(argv[0]);
        update_op = lean_alloc_ctor(0, 1, 0);  /* tag 0 = delete */
        lean_ctor_set(update_op, 0, lean_int64_to_int(rowid));
    } else if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        /* INSERT */
        lean_object* rowid_opt;
        if (sqlite3_value_type(argv[1]) == SQLITE_NULL) {
            rowid_opt = lean_box(0);  /* None */
        } else {
            int64_t rowid = sqlite3_value_int64(argv[1]);
            rowid_opt = lean_alloc_ctor(1, 1, 0);  /* Some */
            lean_ctor_set(rowid_opt, 0, lean_int64_to_int(rowid));
        }

        /* Build values array from argv[2..] */
        lean_object* values = lean_mk_empty_array();
        for (int i = 2; i < argc; i++) {
            lean_object* val = sqlite_value_to_lean(argv[i]);
            values = lean_array_push(values, val);
        }

        update_op = lean_alloc_ctor(1, 2, 0);  /* tag 1 = insert */
        lean_ctor_set(update_op, 0, rowid_opt);
        lean_ctor_set(update_op, 1, values);
    } else {
        /* UPDATE */
        int64_t old_rowid = sqlite3_value_int64(argv[0]);
        int64_t new_rowid = sqlite3_value_int64(argv[1]);

        /* Build values array from argv[2..] */
        lean_object* values = lean_mk_empty_array();
        for (int i = 2; i < argc; i++) {
            lean_object* val = sqlite_value_to_lean(argv[i]);
            values = lean_array_push(values, val);
        }

        update_op = lean_alloc_ctor(2, 3, 0);  /* tag 2 = update */
        lean_ctor_set(update_op, 0, lean_int64_to_int(old_rowid));
        lean_ctor_set(update_op, 1, lean_int64_to_int(new_rowid));
        lean_ctor_set(update_op, 2, values);
    }

    /* Call Lean update function: τ → VTableUpdateOp → IO (Option Int) */
    lean_inc(ctx->update_fn);
    lean_inc(ctx->table_data);
    lean_object* io_action = lean_apply_2(ctx->update_fn, ctx->table_data, update_op);
    lean_object* io_result = lean_apply_1(io_action, lean_io_mk_world());

    if (!lean_io_result_is_ok(io_result)) {
        lean_dec(io_result);
        vtab->base.zErrMsg = sqlite3_mprintf("Virtual table update failed");
        return SQLITE_ERROR;
    }

    /* Extract new rowid if returned (for INSERT) */
    lean_object* result = lean_io_result_get_value(io_result);
    if (lean_obj_tag(result) == 1) {  /* Some */
        lean_object* new_rowid = lean_ctor_get(result, 0);
        *pRowid = lean_int64_of_int(new_rowid);
    }

    lean_dec(io_result);
    return SQLITE_OK;
}

/* xDisconnect/xDestroy - Cleanup table */
static int vtab_disconnect(sqlite3_vtab* pVTab) {
    QuarryVTab* vtab = (QuarryVTab*)pVTab;
    sqlite3_free(vtab);
    return SQLITE_OK;
}

/* Module destructor - called when module is removed */
static void vtab_module_destroy(void* pAux) {
    VTableModuleContext* ctx = (VTableModuleContext*)pAux;
    if (ctx) {
        if (ctx->table_data) lean_dec(ctx->table_data);
        if (ctx->schema_fn) lean_dec(ctx->schema_fn);
        if (ctx->best_index_fn) lean_dec(ctx->best_index_fn);
        if (ctx->open_fn) lean_dec(ctx->open_fn);
        if (ctx->eof_fn) lean_dec(ctx->eof_fn);
        if (ctx->next_fn) lean_dec(ctx->next_fn);
        if (ctx->column_fn) lean_dec(ctx->column_fn);
        if (ctx->rowid_fn) lean_dec(ctx->rowid_fn);
        if (ctx->update_fn) lean_dec(ctx->update_fn);
        free(ctx);
    }
}

/* Static module definition */
static sqlite3_module quarry_vtab_module = {
    0,                  /* iVersion */
    vtab_create,        /* xCreate */
    vtab_create,        /* xConnect (same as Create for eponymous) */
    vtab_best_index,    /* xBestIndex */
    vtab_disconnect,    /* xDisconnect */
    vtab_disconnect,    /* xDestroy */
    vtab_open,          /* xOpen */
    vtab_close,         /* xClose */
    vtab_filter,        /* xFilter */
    vtab_next,          /* xNext */
    vtab_eof,           /* xEof */
    vtab_column,        /* xColumn */
    vtab_rowid,         /* xRowid */
    vtab_update,        /* xUpdate */
    NULL,               /* xBegin */
    NULL,               /* xSync */
    NULL,               /* xCommit */
    NULL,               /* xRollback */
    NULL,               /* xFindFunction */
    NULL,               /* xRename */
    NULL,               /* xSavepoint */
    NULL,               /* xRelease */
    NULL,               /* xRollbackTo */
    NULL                /* xShadowName */
};

/* Register a virtual table module */
LEAN_EXPORT lean_obj_res quarry_db_create_vtab_module(
    b_lean_obj_arg db_obj,
    b_lean_obj_arg name_obj,
    lean_obj_arg table_data,
    lean_obj_arg schema_fn,
    lean_obj_arg best_index_fn,
    lean_obj_arg open_fn,
    lean_obj_arg eof_fn,
    lean_obj_arg next_fn,
    lean_obj_arg column_fn,
    lean_obj_arg rowid_fn,
    lean_obj_arg update_fn,
    lean_obj_arg world
) {
    sqlite3* db = (sqlite3*)lean_get_external_data(db_obj);
    const char* name = lean_string_cstr(name_obj);

    VTableModuleContext* ctx = (VTableModuleContext*)malloc(sizeof(VTableModuleContext));
    ctx->table_data = table_data;
    ctx->schema_fn = schema_fn;
    ctx->best_index_fn = best_index_fn;
    ctx->open_fn = open_fn;
    ctx->eof_fn = eof_fn;
    ctx->next_fn = next_fn;
    ctx->column_fn = column_fn;
    ctx->rowid_fn = rowid_fn;
    ctx->update_fn = update_fn;

    int rc = sqlite3_create_module_v2(
        db,
        name,
        &quarry_vtab_module,
        ctx,
        vtab_module_destroy
    );

    if (rc != SQLITE_OK) {
        vtab_module_destroy(ctx);
        return mk_sqlite_error(db);
    }

    return lean_io_result_mk_ok(lean_box(0));
}

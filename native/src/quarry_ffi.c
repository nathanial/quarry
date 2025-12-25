/*
 * Quarry FFI Implementation
 * C bindings for SQLite with external class registration
 */

#include <lean/lean.h>
#include <sqlite3.h>
#include <string.h>
#include <stdlib.h>

/* ========================================================================== */
/* External Class Registration                                                 */
/* ========================================================================== */

static lean_external_class* g_database_class = NULL;
static lean_external_class* g_statement_class = NULL;

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

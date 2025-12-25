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

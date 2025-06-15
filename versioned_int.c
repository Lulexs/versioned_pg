#include "postgres.h"
#include "fmgr.h"
#include "datatype/timestamp.h"
#include "utils/timestamp.h"
#include "funcapi.h"

#include <stdio.h>
#include <stdlib.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

typedef struct
{
    int64 value;
    Timestamp time;
} VersionedIntEntry;

typedef struct
{
    int32 v1_len_;
    int32 count;
    int32 cap;
    int32 pad_;
    VersionedIntEntry entries[FLEXIBLE_ARRAY_MEMBER];
} VersionedInt;

PG_FUNCTION_INFO_V1(versioned_int_in);
Datum versioned_int_in(PG_FUNCTION_ARGS)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED)),
            errmsg("Conversion between text representation and versioned_int is not possible"));
}

PG_FUNCTION_INFO_V1(make_versioned);
Datum make_versioned(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = NULL;
    VersionedInt *newVersionedInt = NULL;
    int64 newValue;
    TimestampTz time = GetCurrentTimestamp();
    if (!PG_ARGISNULL(0))
    {
        versionedInt = (VersionedInt *)PG_GETARG_POINTER(0);
    }
    if (PG_ARGISNULL(1))
    {
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)),
                errmsg("Cannot insert \"null\" as the value of versioned_int type"));
    }
    newValue = PG_GETARG_INT64(1);

    if (versionedInt == NULL)
    {
        versionedInt = (VersionedInt *)palloc0(sizeof(VersionedInt) + sizeof(VersionedIntEntry));
        SET_VARSIZE(versionedInt, sizeof(VersionedInt) + sizeof(VersionedIntEntry));
        versionedInt->cap = 1;
        versionedInt->count = 1;
        versionedInt->entries[0].value = newValue;
        versionedInt->entries[0].time = time;
        newVersionedInt = versionedInt;
    }
    else
    {
        if (versionedInt->count == versionedInt->cap)
        {
            int32 size = sizeof(VersionedInt) + 2 * versionedInt->cap * sizeof(VersionedIntEntry);
            newVersionedInt = (VersionedInt *)palloc0(size);
            SET_VARSIZE(newVersionedInt, size);
            newVersionedInt->cap = 2 * versionedInt->cap;
            newVersionedInt->count = versionedInt->count;
            memcpy(newVersionedInt->entries, versionedInt->entries, versionedInt->count * sizeof(VersionedIntEntry));
            newVersionedInt->entries[newVersionedInt->count].value = newValue;
            newVersionedInt->entries[newVersionedInt->count].time = time;
            newVersionedInt->count += 1;
        }
        else
        {
            versionedInt->entries[versionedInt->count].value = newValue;
            versionedInt->entries[versionedInt->count].time = time;
            versionedInt->count += 1;
            newVersionedInt = versionedInt;
        }
    }

    PG_RETURN_POINTER(newVersionedInt);
}

PG_FUNCTION_INFO_V1(get_history);
Datum get_history(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    TupleDesc tupdesc;
    HeapTuple heaptuple;
    VersionedInt *versionedInt = (VersionedInt *)PG_GETARG_POINTER(0);
    int call_cntr;
    int max_calls;
    int64 idx;
    Datum *values;
    bool *nulls;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldContext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldContext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        funcctx->max_calls = versionedInt->count;

        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning composite called in a context that does not accept one")));
        }
        tupdesc = BlessTupleDesc(tupdesc);
        funcctx->tuple_desc = tupdesc;

        MemoryContextSwitchTo(oldContext);
    }

    funcctx = SRF_PERCALL_SETUP();
    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;
    tupdesc = funcctx->tuple_desc;
    idx = call_cntr;

    if (call_cntr == max_calls)
    {
        SRF_RETURN_DONE(funcctx);
    }

    values = (Datum *)palloc(2 * sizeof(Datum));
    nulls = (bool *)palloc(2 * sizeof(bool));
    nulls[0] = false;
    nulls[1] = false;

    values[0] = TimestampTzGetDatum(versionedInt->entries[idx].time);
    values[1] = Int64GetDatum(versionedInt->entries[idx].value);

    heaptuple = heap_form_tuple(tupdesc, values, nulls);

    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(heaptuple));
}

PG_FUNCTION_INFO_V1(versioned_int_out);
Datum versioned_int_out(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_GETARG_POINTER(0);
    char *result;

    result = psprintf("%ld", versionedInt->entries[versionedInt->count - 1].value);
    PG_RETURN_CSTRING(result);
}

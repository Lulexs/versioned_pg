#include "postgres.h"
#include "fmgr.h"
#include "datatype/timestamp.h"
#include "utils/timestamp.h"
#include "funcapi.h"

#include <stdio.h>
#include <stdlib.h>

#define MAX_VERSIONED_INT_SIZE (512 * 1024 * 1024)

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
    Size size;
    VersionedInt *versionedInt = NULL;
    VersionedInt *newVersionedInt = NULL;
    int64 newValue;
    TimestampTz time = GetCurrentTimestamp();
    if (!PG_ARGISNULL(0))
    {
        versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
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
            size = sizeof(VersionedInt) + 2 * versionedInt->cap * sizeof(VersionedIntEntry);
            if (size >= (Size)MAX_VERSIONED_INT_SIZE)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_OUT_OF_MEMORY)),
                        errmsg("Extending column would push it pass the size of 512MB. Aborting"));
            }
        }
        else
        {
            size = sizeof(VersionedInt) + versionedInt->cap * sizeof(VersionedIntEntry);
        }
        newVersionedInt = (VersionedInt *)palloc0(size);
        SET_VARSIZE(newVersionedInt, size);
        newVersionedInt->cap = 2 * versionedInt->cap;
        newVersionedInt->count = versionedInt->count;
        memcpy(newVersionedInt->entries, versionedInt->entries, versionedInt->count * sizeof(VersionedIntEntry));
        newVersionedInt->entries[newVersionedInt->count].value = newValue;
        newVersionedInt->entries[newVersionedInt->count].time = time;
        newVersionedInt->count += 1;
    }

    PG_RETURN_POINTER(newVersionedInt);
}

typedef struct
{
    Datum values[2];
    bool nulls[2];
} NullsAndValues;

PG_FUNCTION_INFO_V1(get_history);
Datum get_history(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    TupleDesc tupdesc;
    HeapTuple heaptuple;
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
    int call_cntr;
    int max_calls;
    int64 idx;
    NullsAndValues *state;

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
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        state = (NullsAndValues *)palloc(sizeof(NullsAndValues));
        funcctx->user_fctx = (void *)state;

        MemoryContextSwitchTo(oldContext);
    }

    funcctx = SRF_PERCALL_SETUP();
    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;
    tupdesc = funcctx->tuple_desc;
    idx = call_cntr;
    state = (NullsAndValues *)funcctx->user_fctx;

    if (call_cntr >= max_calls)
    {
        SRF_RETURN_DONE(funcctx);
    }

    state->nulls[0] = false;
    state->nulls[1] = false;
    state->values[0] = TimestampTzGetDatum(versionedInt->entries[idx].time);
    state->values[1] = Int64GetDatum(versionedInt->entries[idx].value);

    heaptuple = heap_form_tuple(tupdesc, state->values, state->nulls);
    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(heaptuple));
}

VersionedIntEntry *get_versioned_ints_value_at_time(VersionedInt *versionedInt, TimestampTz timestamp);
VersionedIntEntry *get_versioned_ints_value_at_time(VersionedInt *versionedInt, TimestampTz timestamp)
{
    VersionedIntEntry *entries = versionedInt->entries;
    int32 l = 0;
    int32 r = versionedInt->count - 1;
    int32 mid;

    if (versionedInt->count == 0)
    {
        return NULL;
    }

    while (l <= r)
    {
        mid = l + (r - l) / 2;

        if (entries[mid].time == timestamp)
        {
            return &entries[mid];
        }
        else if (entries[mid].time < timestamp)
        {
            l = mid + 1;
        }
        else
        {
            r = mid - 1;
        }
    }

    if (r >= 0)
    {
        return &entries[r];
    }

    return NULL;
}

PG_FUNCTION_INFO_V1(versioned_int_at_time);
Datum versioned_int_at_time(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
    TimestampTz time_at = PG_GETARG_TIMESTAMPTZ(1);

    VersionedIntEntry *entry = get_versioned_ints_value_at_time(versionedInt, time_at);
    if (entry == NULL)
    {
        PG_RETURN_NULL();
    }

    PG_RETURN_INT64(entry->value);
}

PG_FUNCTION_INFO_V1(versioned_int_at_time_eq);
Datum versioned_int_at_time_eq(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
    HeapTupleHeader t = PG_GETARG_HEAPTUPLEHEADER(1);
    bool isNull;
    Datum timestampDatum, valueDatum;
    int64 value;
    TimestampTz timestamp;
    VersionedIntEntry *entry;

    timestampDatum = GetAttributeByName(t, "ts", &isNull);
    if (isNull)
    {
        ereport(ERROR, (errmsg("ts cannot be NULL")));
    }

    valueDatum = GetAttributeByName(t, "value", &isNull);
    if (isNull)
    {
        ereport(ERROR, (errmsg("ts cannot be NULL")));
    }

    value = DatumGetInt64(valueDatum);
    timestamp = DatumGetTimestampTz(timestampDatum);

    entry = get_versioned_ints_value_at_time(versionedInt, timestamp);
    if (entry == NULL)
    {
        PG_RETURN_NULL();
    }

    PG_RETURN_BOOL(entry->value == value);
}

PG_FUNCTION_INFO_V1(versioned_int_out);
Datum versioned_int_out(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
    char *result;

    result = psprintf("%ld", versionedInt->entries[versionedInt->count - 1].value);
    PG_RETURN_CSTRING(result);
}

/*
 *
 * COMPARISON OPERATORS FOR versioned_int and bigint
 *
 */
/* versioned_int = bigint */
PG_FUNCTION_INFO_V1(versioned_int_eq_bigint);
Datum versioned_int_eq_bigint(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
    int64 bigInt = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(versionedInt->entries[versionedInt->count - 1].value == bigInt);
}

/* versioned_int <> bigint */
PG_FUNCTION_INFO_V1(versioned_int_neq_bigint);
Datum versioned_int_neq_bigint(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
    int64 bigInt = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(versionedInt->entries[versionedInt->count - 1].value != bigInt);
}

/* versioned_int > bigint */
PG_FUNCTION_INFO_V1(versioned_int_gt_bigint);
Datum versioned_int_gt_bigint(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
    int64 bigInt = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(versionedInt->entries[versionedInt->count - 1].value > bigInt);
}

/* versioned_int >= bigint */
PG_FUNCTION_INFO_V1(versioned_int_ge_bigint);
Datum versioned_int_ge_bigint(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
    int64 bigInt = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(versionedInt->entries[versionedInt->count - 1].value >= bigInt);
}

/* versioned_int < bigint */
PG_FUNCTION_INFO_V1(versioned_int_lt_bigint);
Datum versioned_int_lt_bigint(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
    int64 bigInt = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(versionedInt->entries[versionedInt->count - 1].value < bigInt);
}

/* versioned_int <= bigint */
PG_FUNCTION_INFO_V1(versioned_int_le_bigint);
Datum versioned_int_le_bigint(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
    int64 bigInt = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(versionedInt->entries[versionedInt->count - 1].value <= bigInt);
}

#include "postgres.h"
#include "fmgr.h"
#include "datatype/timestamp.h"
#include "utils/timestamp.h"
#include "funcapi.h"
#include "access/gist.h"
#include "access/heapam.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"

#include <stdio.h>
#include <stdlib.h>

#define MAX_VERSIONED_INT_SIZE (512 * 1024 * 1024)
#define VERINT_MODIFIER_MAX_VALUE (1 << 24)
#define MODIFIER_CHARSHIFT (24)
#define LEN_MASK ((1 << MODIFIER_CHARSHIFT) - 1)

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*
 *
 * Struct that holds single entry in versioned_int type.
 * value is value at specified timestamp time
 *
 */
typedef struct
{
    int64 value;
    TimestampTz time;
} VersionedIntEntry;

/*
 *
 * Struct that is versioned_int's internal representation.
 * v1_len_ is mandatory field for varlena types that holds total length in bytes
 * count is number of entries in entries array
 * cap is current capacity of entries array
 * pad_ is padding added for aligning entries array
 * entries is an array that holds integer's history
 * min_val and max_val are fields used to speed inserting entry in gist index
 *
 */
typedef struct
{
    int32 v1_len_;
    int32 count;
    int32 cap;
    int32 pad_;
    int64 valid;
    VersionedIntEntry entries[FLEXIBLE_ARRAY_MEMBER];
} VersionedInt;

/*
 *
 * Helper struct that holds values array and nulls array required
 * for specifying return in srfs.
 *
 */
typedef struct
{
    Datum values[2];
    bool nulls[2];
} NullsAndValues;

/*
 *
 * This struct represents node of Gist index. It is essentially
 * bounding box (rectangle) whose dimensions are time and value
 *
 */

typedef struct
{
    TimestampTz lower_tzbound;
    TimestampTz upper_tzbound;
    int64 lower_val;
    int64 upper_val;
} verint_rect;

typedef struct
{
    int64 verint_min;
    int64 verint_max;
} VerintMinMax;

PG_FUNCTION_INFO_V1(versioned_int_in);
PG_FUNCTION_INFO_V1(versioned_int_out);
PG_FUNCTION_INFO_V1(versioned_int_typemod_in);
PG_FUNCTION_INFO_V1(versioned_int_typemod_out);
PG_FUNCTION_INFO_V1(make_versioned);
PG_FUNCTION_INFO_V1(make_versioned_with_ts);
PG_FUNCTION_INFO_V1(get_history);
PG_FUNCTION_INFO_V1(versioned_int_at_time);
PG_FUNCTION_INFO_V1(versioned_int_at_time_eq);
PG_FUNCTION_INFO_V1(versioned_int_enforce_modifier);

// Gist support
PG_FUNCTION_INFO_V1(verint_rect_in);
PG_FUNCTION_INFO_V1(verint_rect_out);
PG_FUNCTION_INFO_V1(versioned_int_consistent);
PG_FUNCTION_INFO_V1(versioned_int_union);
PG_FUNCTION_INFO_V1(versioned_int_compress);
PG_FUNCTION_INFO_V1(versioned_int_penalty);
PG_FUNCTION_INFO_V1(versioned_int_same);
PG_FUNCTION_INFO_V1(versioned_int_picksplit);

// Btree
PG_FUNCTION_INFO_V1(versioned_int_btree_cmp);
static int versioned_int_cmp_internal(VersionedInt *a, VersionedInt *b);

static VersionedInt *enforce_N_retention(VersionedInt *versionedInt, int32 maxCap);
static VersionedInt *enforce_Time_retention(VersionedInt *versionedInt, int64 time);
static VersionedIntEntry *get_versioned_ints_value_at_time(VersionedInt *versionedInt, TimestampTz timestamp);
static int32 first_time_greater_than_cutoff(VersionedIntEntry *entries, int32 count, TimestampTz cutoff);
static int32 get_ts_insert_location(VersionedIntEntry *entries, int32 count, TimestampTz time);
static inline float8 get_area(const verint_rect *r);
static inline float8 get_union_area(const verint_rect *r1, const verint_rect *r2);
static inline void get_union_rect(const verint_rect *r1, const verint_rect *r2, verint_rect *dst);
static VerintMinMax get_versioned_ints_min_max(VersionedInt *verint);

static TimestampTz get_first_write_ts();
static TimestampTz first_write_ts = 0;
static void xact_callback(XactEvent event, void *arg);

void _PG_init(void)
{
    RegisterXactCallback(xact_callback, NULL);
}

static void xact_callback(XactEvent event, void *arg)
{
    if (event == XACT_EVENT_COMMIT)
    {
        first_write_ts = 0;
    }
}

static TimestampTz get_first_write_ts()
{
    if (first_write_ts == 0)
    {
        first_write_ts = GetCurrentTimestamp();
    }

    return first_write_ts;
}

/*
 *
 * Input function for versioned_int, i.e. function that turns
 * string to type's internal representation. For versioned_int
 * this conversion is disabled.
 *
 */
Datum versioned_int_in(PG_FUNCTION_ARGS)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED)),
            errmsg("Conversion between text representation and versioned_int is not possible"));
}

/*
 *
 * Type modifier Input function for versioned_int, i.e. function that
 * takes list of strings and returns int4 (internal typemod) value
 *
 */
Datum versioned_int_typemod_in(PG_FUNCTION_ARGS)
{
    ArrayType *arr = PG_GETARG_ARRAYTYPE_P(0);
    Datum *elems;
    int n_elems;
    int32 typmod;
    int64 len;
    char *cstr;
    unsigned char ch;

    deconstruct_array(arr, CSTRINGOID, -2, false, 'c',
                      &elems, NULL, &n_elems);

    if (n_elems != 2)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("type modifier requires exactly (length, char)")));

    len = strtol(DatumGetCString(elems[0]), NULL, 10);
    if (len <= 0 || len > VERINT_MODIFIER_MAX_VALUE)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("length must be between 1 and 2^24")));

    cstr = DatumGetCString(elems[1]);
    if (strlen(cstr) != 1)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("char modifier must be exactly one character")));
    ch = cstr[0];

    if (ch != 'N' && ch != 'D')
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("char modifier must be 'N' or 'D'")));

    typmod = (int32)len | ((int32)ch << MODIFIER_CHARSHIFT);
    PG_RETURN_INT32(typmod);
}

/*
 *
 * Type modifier Input function for versioned_int, i.e. function that
 * takes list of strings and returns int4 (internal typemod) value
 *
 */
Datum versioned_int_typemod_out(PG_FUNCTION_ARGS)
{
    int32 typmod = PG_GETARG_INT32(0);
    int len;
    char ch;

    if (typmod < 0)
        PG_RETURN_CSTRING(pstrdup(""));

    len = typmod & LEN_MASK;
    ch = (typmod >> MODIFIER_CHARSHIFT) & 0xFF;

    PG_RETURN_CSTRING(psprintf("(%d,'%c')", len, ch));
}

/*
 *
 * Function that enforces versioned_ints type modifier. It is used
 * in implicit cast of versioned_int to versioned_int, and it's the only
 * way to get hold of type modifier on custom type. Typmod here will never
 * be -1, because if no typmod specified on column, no cast will be called.
 *
 *
 */
Datum versioned_int_enforce_modifier(PG_FUNCTION_ARGS)
{
    VersionedInt *src = (VersionedInt *)PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(0));
    int32 typmod = PG_GETARG_INT32(1);
    int32 len = typmod & LEN_MASK;
    char ch = (typmod >> MODIFIER_CHARSHIFT) & 0xFF;

    if (ch == 'N')
    {
        src = enforce_N_retention(src, len);
    }
    else if (ch == 'D')
    {
        src = enforce_Time_retention(src, (int64)len * 24 * 60 * 60 * 1000000);
    }
    else
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("unknown retention policy character \"%c\"", ch)));
    }

    PG_RETURN_POINTER(src);
}

/*
 *
 * make_versioned is a function that takes two arguments - versioned_int
 * and new value of that versioned int and adds new value to int's history.
 * is called like
 * make_versioned(null, new_value)
 * make_versioned(verint, new_value)
 *
 */
Datum make_versioned(PG_FUNCTION_ARGS)
{
    Size size;
    VersionedInt *versionedInt = NULL;
    VersionedInt *newVersionedInt = NULL;
    int64 newValue;
    TimestampTz time = get_first_write_ts();
    if (!PG_ARGISNULL(0))
    {
        versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
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
            newVersionedInt = (VersionedInt *)palloc0(size);
            SET_VARSIZE(newVersionedInt, size);
            newVersionedInt->cap = 2 * versionedInt->cap;
        }
        else
        {
            size = sizeof(VersionedInt) + versionedInt->cap * sizeof(VersionedIntEntry);
            newVersionedInt = (VersionedInt *)palloc0(size);
            SET_VARSIZE(newVersionedInt, size);
            newVersionedInt->cap = versionedInt->cap;
        }

        newVersionedInt->count = versionedInt->count;
        memcpy(newVersionedInt->entries, versionedInt->entries, versionedInt->count * sizeof(VersionedIntEntry));
        newVersionedInt->entries[newVersionedInt->count].value = newValue;
        newVersionedInt->entries[newVersionedInt->count].time = time;
        newVersionedInt->count += 1;
    }

    PG_RETURN_POINTER(newVersionedInt);
}

/*
 *
 * make_versioned_with_ts is a function that takes three arguments - versioned_int,
 * new value and a timestamp adds new value to int's history.
 * is called like
 * make_versioned(NULL, new_value, timestamp)
 * make_versioned(verint, new_value, timestamp)
 *
 */
Datum make_versioned_with_ts(PG_FUNCTION_ARGS)
{
    Size size;
    VersionedInt *versionedInt = NULL;
    VersionedInt *newVersionedInt = NULL;
    int64 newValue;
    TimestampTz time;
    int32 idx;

    if (!PG_ARGISNULL(0))
    {
        versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    }
    if (PG_ARGISNULL(1))
    {
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)),
                errmsg("Cannot insert \"null\" as the value of versioned_int type"));
    }
    if (PG_ARGISNULL(2))
    {
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)),
                errmsg("Cannot insert \"null\" as the timestamp of versioned_int type"));
    }

    newValue = PG_GETARG_INT64(1);
    time = PG_GETARG_TIMESTAMPTZ(2);

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
            newVersionedInt = (VersionedInt *)palloc0(size);
            SET_VARSIZE(newVersionedInt, size);
            newVersionedInt->cap = 2 * versionedInt->cap;
        }
        else
        {
            size = sizeof(VersionedInt) + versionedInt->cap * sizeof(VersionedIntEntry);
            newVersionedInt = (VersionedInt *)palloc0(size);
            SET_VARSIZE(newVersionedInt, size);
            newVersionedInt->cap = versionedInt->cap;
        }

        newVersionedInt->count = versionedInt->count + 1;
        idx = get_ts_insert_location(versionedInt->entries, versionedInt->count, time);

        memcpy(newVersionedInt->entries, versionedInt->entries, idx * sizeof(VersionedIntEntry));
        memcpy(&newVersionedInt->entries[idx + 1], &versionedInt->entries[idx], (versionedInt->count - idx) * sizeof(VersionedIntEntry));
        newVersionedInt->entries[idx].value = newValue;
        newVersionedInt->entries[idx].time = time;
    }

    PG_RETURN_POINTER(newVersionedInt);
}

/*
 *
 * Function that returns versioned_int's history in format
 * timetsamptz, int64
 *
 */
Datum get_history(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    TupleDesc tupdesc;
    HeapTuple heaptuple;
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
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

/*
 *
 * Function that is used for getting versioned_int's value at timestamp, i.e
 * when user uses @ operator (versioned_int @ timestamp)
 *
 */
Datum versioned_int_at_time(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    TimestampTz time_at = PG_GETARG_TIMESTAMPTZ(1);

    VersionedIntEntry *entry = get_versioned_ints_value_at_time(versionedInt, time_at);
    if (entry == NULL)
    {
        PG_RETURN_NULL();
    }

    PG_RETURN_INT64(entry->value);
}

/*
 *
 * Function that is used for comparing versioned_int with composite (timestamp, value).
 * In sql that would look like versioned_int @= (timestamp, value).
 *
 */
Datum versioned_int_at_time_eq(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
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

/*
 *
 * Output function for versioned_int, i.e. function that turns
 * type's internal representation to string. Here we just get
 * versioned_int's current value.
 *
 */
Datum versioned_int_out(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    char *result;

    if (versionedInt->count == 0)
    {
        result = psprintf("NULL");
    }
    else
    {
        result = psprintf("%ld", versionedInt->entries[versionedInt->count - 1].value);
    }
    PG_RETURN_CSTRING(result);
}

/*
 *
 * GIST INDEX METHOD SUPPORT FOR VERSIONED_INT
 *
 */
Datum verint_rect_in(PG_FUNCTION_ARGS)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED)),
            errmsg("Conversion between text representation and verint_rect is not possible"));
}

Datum verint_rect_out(PG_FUNCTION_ARGS)
{
    verint_rect *rect = (verint_rect *)PG_GETARG_POINTER(0);

    const char *lower_ts_str = timestamptz_to_str(rect->lower_tzbound);

    const char *upper_ts_str = timestamptz_to_str(rect->upper_tzbound);

    char *result = psprintf("(%s,%s,%lld,%lld)",
                            lower_ts_str,
                            upper_ts_str,
                            (long long)rect->lower_val,
                            (long long)rect->upper_val);

    PG_RETURN_CSTRING(result);
}

/*
 *
 * versioned_int's gist consistency function
 *
 */
Datum versioned_int_consistent(PG_FUNCTION_ARGS)
{
    bool isNull;
    Datum valueDatum, time_at_datum;
    int64 value;
    TimestampTz time_at;
    GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);
    Datum query_datum = PG_GETARG_DATUM(1);
    bool *recheck = (bool *)PG_GETARG_POINTER(4);
    verint_rect *key = (verint_rect *)DatumGetPointer(entry->key);

    HeapTupleHeader t = DatumGetHeapTupleHeader(query_datum);

    time_at_datum = GetAttributeByName(t, "ts", &isNull);
    if (isNull)
    {
        ereport(ERROR, (errmsg("ts cannot be NULL")));
    }
    valueDatum = GetAttributeByName(t, "value", &isNull);
    if (isNull)
    {
        ereport(ERROR, (errmsg("value cannot be NULL")));
    }

    value = DatumGetInt64(valueDatum);
    time_at = DatumGetTimestampTz(time_at_datum);

    if (key->lower_tzbound <= time_at &&
        time_at <= key->upper_tzbound &&
        key->lower_val <= value &&
        value <= key->upper_val)
    {
        if (GIST_LEAF(entry))
        {
            *recheck = true;
            PG_RETURN_BOOL(true);
        }
        else
        {
            *recheck = false;
            PG_RETURN_BOOL(true);
        }
    }
    *recheck = false;
    PG_RETURN_BOOL(false);
}

/*
 *
 * versioned_int's gist union function
 *
 */
Datum versioned_int_union(PG_FUNCTION_ARGS)
{
    int i;
    verint_rect *nodeentry;
    GistEntryVector *entryvec = (GistEntryVector *)PG_GETARG_POINTER(0);
    GISTENTRY *ent = entryvec->vector;
    int n = entryvec->n;
    verint_rect *ret = (verint_rect *)palloc0(sizeof(verint_rect));

    ret->lower_tzbound = PG_INT64_MAX;
    ret->upper_tzbound = PG_INT64_MIN;
    ret->lower_val = PG_INT64_MAX;
    ret->upper_val = PG_INT64_MIN;

    for (i = 0; i < n; i++)
    {
        nodeentry = (verint_rect *)DatumGetPointer(ent[i].key);
        ret->lower_tzbound = Min(ret->lower_tzbound, nodeentry->lower_tzbound);
        ret->upper_tzbound = Max(ret->upper_tzbound, nodeentry->upper_tzbound);
        ret->lower_val = Min(ret->lower_val, nodeentry->lower_val);
        ret->upper_val = Max(ret->upper_val, nodeentry->upper_val);
    }

    PG_RETURN_POINTER(ret);
}

/*
 *
 * versioned_int's gist compress function
 *
 */
Datum versioned_int_compress(PG_FUNCTION_ARGS)
{
    GISTENTRY *retval;
    verint_rect *rect;
    VersionedInt *verint;
    GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);
    VerintMinMax minmax;

    if (entry->leafkey)
    {
        rect = (verint_rect *)palloc(sizeof(verint_rect));
        verint = (VersionedInt *)PG_DETOAST_DATUM(entry->key);

        rect->lower_tzbound = verint->entries[0].time;
        rect->upper_tzbound = PG_INT64_MAX - 1;
        minmax = get_versioned_ints_min_max(verint);
        rect->lower_val = minmax.verint_min;
        rect->upper_val = minmax.verint_max;

        retval = palloc(sizeof(GISTENTRY));
        gistentryinit(*retval, PointerGetDatum(rect), entry->rel, entry->page, entry->offset, false);
    }
    else
    {
        retval = entry;
    }

    PG_RETURN_POINTER(retval);
}

/*
 *
 * versioned_int's gist penalty function
 *
 */
Datum versioned_int_penalty(PG_FUNCTION_ARGS)
{
    GISTENTRY *origentry = (GISTENTRY *)PG_GETARG_POINTER(0);
    GISTENTRY *newentry = (GISTENTRY *)PG_GETARG_POINTER(1);
    float *penalty = (float *)PG_GETARG_POINTER(2);
    verint_rect *origrect = (verint_rect *)DatumGetPointer(origentry->key);
    verint_rect *newrect = (verint_rect *)DatumGetPointer(newentry->key);

    float8 extra = 0;

    if (newrect->lower_tzbound < origrect->lower_tzbound)
        extra += (float8)(origrect->lower_tzbound - newrect->lower_tzbound);

    if (newrect->upper_tzbound > origrect->upper_tzbound)
        extra += (float8)(newrect->upper_tzbound - origrect->upper_tzbound);

    if (newrect->lower_val < origrect->lower_val)
        extra += (float8)(origrect->lower_val - newrect->lower_val);

    if (newrect->upper_val > origrect->upper_val)
        extra += (float8)(newrect->upper_val - origrect->upper_val);

    *penalty = (float)extra;
    PG_RETURN_POINTER(penalty);
}

/*
 *
 * versioned_int's gist same function
 *
 */
Datum versioned_int_same(PG_FUNCTION_ARGS)
{
    verint_rect *r1 = (verint_rect *)PG_GETARG_POINTER(0);
    verint_rect *r2 = (verint_rect *)PG_GETARG_POINTER(1);
    bool *result = (bool *)PG_GETARG_POINTER(2);

    *result = (r1->lower_tzbound == r2->lower_tzbound) &&
              (r1->upper_tzbound == r2->upper_tzbound) &&
              (r1->lower_val == r2->lower_val) &&
              (r1->upper_val == r2->upper_val);

    // fancy
    // *result = (memcmp(r1, r2, sizeof(verint_rect)) == 0)

    PG_RETURN_POINTER(result);
}

/*
 *
 * versioned_int's gist picksplit function
 *
 */
static inline float8 get_area(const verint_rect *r)
{
    return (float8)(r->upper_tzbound - r->lower_tzbound) *
           (float8)(r->upper_val - r->lower_val);
}

static inline float8 get_union_area(const verint_rect *r1, const verint_rect *r2)
{
    TimestampTz lo_t = Min(r1->lower_tzbound, r2->lower_tzbound);
    TimestampTz hi_t = Max(r1->upper_tzbound, r2->upper_tzbound);
    int64 lo_v = Min(r1->lower_val, r2->lower_val);
    int64 hi_v = Max(r1->upper_val, r2->upper_val);

    return (float8)(hi_t - lo_t) * (float8)(hi_v - lo_v);
}

static inline void get_union_rect(const verint_rect *r1, const verint_rect *r2, verint_rect *dst)
{
    dst->lower_tzbound = Min(r1->lower_tzbound, r2->lower_tzbound);
    dst->upper_tzbound = Max(r1->upper_tzbound, r2->upper_tzbound);
    dst->lower_val = Min(r1->lower_val, r2->lower_val);
    dst->upper_val = Max(r1->upper_val, r2->upper_val);
}

Datum versioned_int_picksplit(PG_FUNCTION_ARGS)
{
    GistEntryVector *entryvec = (GistEntryVector *)PG_GETARG_POINTER(0);
    GIST_SPLITVEC *v = (GIST_SPLITVEC *)PG_GETARG_POINTER(1);

    OffsetNumber maxoff = entryvec->n - 1;
    OffsetNumber i, j;
    int nbytes;
    verint_rect *unionL, *unionR, *r, tmpL, tmpR;
    float8 enlargementL, enlargementR;

    int seed1 = -1, seed2 = -1;
    float8 worst_waste = -1;

    for (i = FirstOffsetNumber; i < maxoff; i = OffsetNumberNext(i))
    {
        verint_rect *r1 = (verint_rect *)DatumGetPointer(entryvec->vector[i].key);
        for (j = OffsetNumberNext(i); j <= maxoff; j = OffsetNumberNext(j))
        {
            verint_rect *r2 = (verint_rect *)DatumGetPointer(entryvec->vector[j].key);

            float8 area1 = get_area(r1);
            float8 area2 = get_area(r2);
            float8 union_area = get_union_area(r1, r2);
            float8 waste = union_area - area1 - area2;

            if (waste > worst_waste)
            {
                worst_waste = waste;
                seed1 = i;
                seed2 = j;
            }
        }
    }

    nbytes = (maxoff + 1) * sizeof(OffsetNumber);
    v->spl_left = (OffsetNumber *)palloc(nbytes);
    v->spl_right = (OffsetNumber *)palloc(nbytes);
    v->spl_nleft = v->spl_nright = 0;

    unionL = (verint_rect *)palloc(sizeof(verint_rect));
    unionR = (verint_rect *)palloc(sizeof(verint_rect));
    *unionL = *(verint_rect *)DatumGetPointer(entryvec->vector[seed1].key);
    *unionR = *(verint_rect *)DatumGetPointer(entryvec->vector[seed2].key);

    v->spl_left[v->spl_nleft++] = seed1;
    v->spl_right[v->spl_nright++] = seed2;

    for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
    {
        if (i == seed1 || i == seed2)
            continue;

        r = (verint_rect *)DatumGetPointer(entryvec->vector[i].key);

        tmpL = *unionL;
        tmpR = *unionR;
        get_union_rect(unionL, r, &tmpL);
        get_union_rect(unionR, r, &tmpR);

        enlargementL = get_area(&tmpL) - get_area(unionL);
        enlargementR = get_area(&tmpR) - get_area(unionR);

        if (enlargementL < enlargementR ||
            (enlargementL == enlargementR && get_area(unionL) < get_area(unionR)))
        {
            v->spl_left[v->spl_nleft++] = i;
            *unionL = tmpL;
        }
        else
        {
            v->spl_right[v->spl_nright++] = i;
            *unionR = tmpR;
        }
    }

    v->spl_ldatum = PointerGetDatum(unionL);
    v->spl_rdatum = PointerGetDatum(unionR);

    PG_RETURN_POINTER(v);
}

/*
 *
 * Some Btree index method functions so that versioned_int could use
 * ORDER BY, DISTINCT etc.
 *
 */
static int versioned_int_cmp_internal(VersionedInt *a, VersionedInt *b)
{
    int64 av = a->entries[a->count - 1].value;
    int64 bv = b->entries[b->count - 1].value;

    if (av < bv)
    {
        return -1;
    }
    if (av > bv)
    {
        return 1;
    }

    return 0;
}

Datum versioned_int_btree_cmp(PG_FUNCTION_ARGS)
{
    VersionedInt *a = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    VersionedInt *b = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

    PG_RETURN_INT32(versioned_int_cmp_internal(a, b));
}

static int32 get_ts_insert_location(VersionedIntEntry *entries, int32 count, TimestampTz time)
{
    int32 l = 0;
    int32 r = count;

    while (l < r)
    {
        int32 mid = l + (r - l) / 2;

        if (entries[mid].time < time)
        {
            l = mid + 1;
        }
        else
        {
            r = mid;
        }
    }

    return l;
}

static VersionedInt *enforce_N_retention(VersionedInt *versionedInt, int32 maxCap)
{
    VersionedInt *newVerint;
    int32 drop;
    if (versionedInt->cap <= maxCap && versionedInt->count <= maxCap)
    {
        return versionedInt;
    }

    newVerint = (VersionedInt *)palloc0(sizeof(VersionedInt) + maxCap * sizeof(VersionedIntEntry));
    SET_VARSIZE(newVerint, sizeof(VersionedInt) + maxCap * sizeof(VersionedIntEntry));
    newVerint->cap = maxCap;
    newVerint->count = Min(versionedInt->count, maxCap);
    drop = Max(0, versionedInt->count - maxCap);

    memcpy(newVerint->entries, &versionedInt->entries[drop], newVerint->count * sizeof(VersionedIntEntry));

    return newVerint;
}

static int32 first_time_greater_than_cutoff(VersionedIntEntry *entries, int32 count, TimestampTz cutoff)
{
    int32 l = 0;
    int32 r = count - 1;
    int32 result = count;

    while (l <= r)
    {
        int32 mid = l + (r - l) / 2;
        if (entries[mid].time > cutoff)
        {
            result = mid;
            r = mid - 1;
        }
        else
        {
            l = mid + 1;
        }
    }

    return result;
}

static VersionedInt *enforce_Time_retention(VersionedInt *versionedInt, int64 time)
{
    TimestampTz cutoffTime = GetCurrentTimestamp() - time;
    int32 idx = first_time_greater_than_cutoff(versionedInt->entries, versionedInt->count, cutoffTime);
    int32 newCount;
    VersionedInt *newVerint;

    if (idx == 0)
        return versionedInt;

    newCount = versionedInt->count - idx;

    newVerint = (VersionedInt *)palloc0(sizeof(VersionedInt) + newCount * sizeof(VersionedIntEntry));
    SET_VARSIZE(newVerint, sizeof(VersionedInt) + newCount * sizeof(VersionedIntEntry));
    newVerint->cap = newCount;
    newVerint->count = newCount;

    memcpy(newVerint->entries, &versionedInt->entries[idx], newCount * sizeof(VersionedIntEntry));

    return newVerint;
}

/*
 *
 * Helper function that given versioned_int and timestamp returns
 * versioned_ints value at that time or null if it didn't exist at
 * said time
 *
 */
static VersionedIntEntry *get_versioned_ints_value_at_time(VersionedInt *versionedInt, TimestampTz timestamp)
{
    VersionedIntEntry *entries = versionedInt->entries;
    int32 l = 0;
    int32 r = versionedInt->count - 1;
    int32 mid;

    if (versionedInt->count == 0)
    {
        return NULL;
    }
    if (timestamp >= entries[versionedInt->count - 1].time)
    {
        return &entries[versionedInt->count - 1];
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

static VerintMinMax get_versioned_ints_min_max(VersionedInt *verint)
{
    VerintMinMax ret = {INT64_MAX, INT64_MIN};
    int i;
    for (i = 0; i < verint->count; i++)
    {
        ret.verint_max = Max(verint->entries[i].value, ret.verint_max);
        ret.verint_min = Min(verint->entries[i].value, ret.verint_min);
    }

    return ret;
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
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    int64 bigInt = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(versionedInt->entries[versionedInt->count - 1].value == bigInt);
}

/* versioned_int <> bigint */
PG_FUNCTION_INFO_V1(versioned_int_neq_bigint);
Datum versioned_int_neq_bigint(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    int64 bigInt = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(versionedInt->entries[versionedInt->count - 1].value != bigInt);
}

/* versioned_int > bigint */
PG_FUNCTION_INFO_V1(versioned_int_gt_bigint);
Datum versioned_int_gt_bigint(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    int64 bigInt = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(versionedInt->entries[versionedInt->count - 1].value > bigInt);
}

/* versioned_int >= bigint */
PG_FUNCTION_INFO_V1(versioned_int_ge_bigint);
Datum versioned_int_ge_bigint(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    int64 bigInt = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(versionedInt->entries[versionedInt->count - 1].value >= bigInt);
}

/* versioned_int < bigint */
PG_FUNCTION_INFO_V1(versioned_int_lt_bigint);
Datum versioned_int_lt_bigint(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    int64 bigInt = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(versionedInt->entries[versionedInt->count - 1].value < bigInt);
}

/* versioned_int <= bigint */
PG_FUNCTION_INFO_V1(versioned_int_le_bigint);
Datum versioned_int_le_bigint(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    int64 bigInt = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(versionedInt->entries[versionedInt->count - 1].value <= bigInt);
}

/*
 *
 * COMPARISON OPERATORS FOR bigint and verint
 *
 */
/* bigint  =  versioned_int */
PG_FUNCTION_INFO_V1(bigint_eq_versioned_int);
Datum bigint_eq_versioned_int(PG_FUNCTION_ARGS)
{
    int64 bigInt = PG_GETARG_INT64(0);
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

    PG_RETURN_BOOL(bigInt == versionedInt->entries[versionedInt->count - 1].value);
}

/* bigint  <>  versioned_int */
PG_FUNCTION_INFO_V1(bigint_neq_versioned_int);
Datum bigint_neq_versioned_int(PG_FUNCTION_ARGS)
{
    int64 bigInt = PG_GETARG_INT64(0);
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

    PG_RETURN_BOOL(bigInt != versionedInt->entries[versionedInt->count - 1].value);
}

/* bigint  >  versioned_int */
PG_FUNCTION_INFO_V1(bigint_gt_versioned_int);
Datum bigint_gt_versioned_int(PG_FUNCTION_ARGS)
{
    int64 bigInt = PG_GETARG_INT64(0);
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

    PG_RETURN_BOOL(bigInt > versionedInt->entries[versionedInt->count - 1].value);
}

/* bigint  >=  versioned_int */
PG_FUNCTION_INFO_V1(bigint_ge_versioned_int);
Datum bigint_ge_versioned_int(PG_FUNCTION_ARGS)
{
    int64 bigInt = PG_GETARG_INT64(0);
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

    PG_RETURN_BOOL(bigInt >= versionedInt->entries[versionedInt->count - 1].value);
}

/* bigint  <  versioned_int */
PG_FUNCTION_INFO_V1(bigint_lt_versioned_int);
Datum bigint_lt_versioned_int(PG_FUNCTION_ARGS)
{
    int64 bigInt = PG_GETARG_INT64(0);
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

    PG_RETURN_BOOL(bigInt < versionedInt->entries[versionedInt->count - 1].value);
}

/* bigint  <=  versioned_int */
PG_FUNCTION_INFO_V1(bigint_le_versioned_int);
Datum bigint_le_versioned_int(PG_FUNCTION_ARGS)
{
    int64 bigInt = PG_GETARG_INT64(0);
    VersionedInt *versionedInt = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

    PG_RETURN_BOOL(bigInt <= versionedInt->entries[versionedInt->count - 1].value);
}

/*
 *
 * COMPARISON OPERATORS FOR verint and verint
 *
 */
/* verint = verint */
PG_FUNCTION_INFO_V1(versioned_int_eq_versioned_int);
Datum versioned_int_eq_versioned_int(PG_FUNCTION_ARGS)
{
    VersionedInt *a = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    VersionedInt *b = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

    PG_RETURN_BOOL(a->entries[a->count - 1].value ==
                   b->entries[b->count - 1].value);
}

/* verint <> verint */
PG_FUNCTION_INFO_V1(versioned_int_neq_versioned_int);
Datum versioned_int_neq_versioned_int(PG_FUNCTION_ARGS)
{
    VersionedInt *a = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    VersionedInt *b = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

    PG_RETURN_BOOL(a->entries[a->count - 1].value !=
                   b->entries[b->count - 1].value);
}

/* verint > verint */
PG_FUNCTION_INFO_V1(versioned_int_gt_versioned_int);
Datum versioned_int_gt_versioned_int(PG_FUNCTION_ARGS)
{
    VersionedInt *a = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    VersionedInt *b = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

    PG_RETURN_BOOL(a->entries[a->count - 1].value >
                   b->entries[b->count - 1].value);
}

/* verint >= verint */
PG_FUNCTION_INFO_V1(versioned_int_ge_versioned_int);
Datum versioned_int_ge_versioned_int(PG_FUNCTION_ARGS)
{
    VersionedInt *a = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    VersionedInt *b = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

    PG_RETURN_BOOL(a->entries[a->count - 1].value >=
                   b->entries[b->count - 1].value);
}

/* verint < verint */
PG_FUNCTION_INFO_V1(versioned_int_lt_versioned_int);
Datum versioned_int_lt_versioned_int(PG_FUNCTION_ARGS)
{
    VersionedInt *a = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    VersionedInt *b = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

    PG_RETURN_BOOL(a->entries[a->count - 1].value <
                   b->entries[b->count - 1].value);
}

/* verint <= verint */
PG_FUNCTION_INFO_V1(versioned_int_le_versioned_int);
Datum versioned_int_le_versioned_int(PG_FUNCTION_ARGS)
{
    VersionedInt *a = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    VersionedInt *b = (VersionedInt *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

    PG_RETURN_BOOL(a->entries[a->count - 1].value <=
                   b->entries[b->count - 1].value);
}
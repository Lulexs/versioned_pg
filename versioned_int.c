#include "postgres.h"
#include "fmgr.h"
#include "datatype/timestamp.h"
#include "utils/timestamp.h"

#include <stdio.h>
#include <stdlib.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* VersionedIntEntry struct declaraction
 *
 * VersionedIntEntry represents value of integer value at time time
 *
 * @author Luka
 */
typedef struct
{
    int value;
    TimestampTz time;
} VersionedIntEntry;

/* VersionedInt struct declaration
 *
 * VersionedInt is internal representation of versioned_int postgre type
 * length - required field that specifies struct length
 * count - current number of entries
 * entries - list of entries
 *
 * @author Luka
 */
typedef struct
{
    int32 length;
    int32 count;
    VersionedIntEntry entries[FLEXIBLE_ARRAY_MEMBER];
} VersionedInt;

/* versioned_int input function
 *
 * Input functions convert string representation to type's internal memory
 * representation. This feature is disabled because of update statement.
 * When user tries to do something like set versioned_int = '5', it would
 * be impossible to access old value. Only way to insert/update versioned ints
 * is to use make_versioned function.
 *
 * @author Luka
 */
PG_FUNCTION_INFO_V1(versioned_int_in);
Datum versioned_int_in(PG_FUNCTION_ARGS)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED)),
            errmsg("Conversion between text representation and versioned_int is not possible"));

    // char *str = PG_GETARG_CSTRING(0);
    // VersionedInt *result;
    // int value;

    // if (sscanf(str, "%d", &value) != 1)
    // {
    //     ereport(ERROR,
    //             (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)),
    //             errmsg("invalid text representation for type %s: \"%s\"", "versioned_int", str));
    // }

    // result = (VersionedInt *)palloc(sizeof(VersionedInt) + sizeof(VersionedIntEntry));
    // SET_VARSIZE(result, sizeof(VersionedInt) + sizeof(VersionedIntEntry));
    // result->count = 1;
    // result->entries[0].value = value;
    // result->entries[0].time = GetCurrentTimestamp();

    // PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(make_versioned);
Datum make_versioned(PG_FUNCTION_ARGS)
{
}

/* versioned_int output function
 *
 * Output functions converts type's internal memory representation to string
 * representation. In case of a versioned_int expected string representation
 * is "5".
 *
 * @author Luka
 */
PG_FUNCTION_INFO_V1(versioned_int_out);
Datum versioned_int_out(PG_FUNCTION_ARGS)
{
    VersionedInt *versionedInt = (VersionedInt *)PG_GETARG_POINTER(0);
    char *result;

    result = psprintf("%d", versionedInt->entries[0].value);
    PG_RETURN_CSTRING(result);
}

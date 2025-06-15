CREATE TYPE versioned_int;

CREATE FUNCTION versioned_int_in(cstring)
    RETURNS versioned_int
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION make_versioned(versioned_int, BIGINT)
    RETURNS versioned_int
    AS 'MODULE_PATHNAME'
    LANGUAGE C VOLATILE;

CREATE FUNCTION versioned_int_out(versioned_int)
    RETURNS cstring
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE __int_history AS (updated_at TIMESTAMP, value BIGINT);

CREATE FUNCTION get_history(versioned_int)
    RETURNS SETOF __int_history
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT VOLATILE;

CREATE TYPE versioned_int (
    internallength = VARIABLE,
    input = versioned_int_in,
    output = versioned_int_out,
    alignment = int4,
    storage = plain
);
CREATE TYPE versioned_int;

CREATE FUNCTION versioned_int_in(cstring)
    RETURNS versioned_int
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION versioned_int_out(versioned_int)
    RETURNS cstring
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE versioned_int (
    internallength = VARIABLE,
    input = versioned_int_in,
    output = versioned_int_out,
    alignment = int4
);
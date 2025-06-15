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

CREATE TYPE versioned_int (
    internallength = VARIABLE,
    input = versioned_int_in,
    output = versioned_int_out,
    alignment = int4,
    storage = plain
);

CREATE TYPE __int_history AS (updated_at TIMESTAMPTZ, value BIGINT);

CREATE FUNCTION get_history(versioned_int)
    RETURNS SETOF __int_history
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION versioned_int_at_time(versioned_int, TIMESTAMPTZ)
    RETURNS BIGINT
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION versioned_int_eq_bigint(versioned_int, BIGINT)
    RETURNS BOOLEAN
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION versioned_int_neq_bigint(versioned_int, BIGINT)
    RETURNS BOOLEAN
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION versioned_int_gt_bigint(versioned_int, BIGINT)
    RETURNS BOOLEAN
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION versioned_int_ge_bigint(versioned_int, BIGINT)
    RETURNS BOOLEAN
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;
    
CREATE FUNCTION versioned_int_lt_bigint(versioned_int, BIGINT)
    RETURNS BOOLEAN
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;
    
CREATE FUNCTION versioned_int_le_bigint(versioned_int, BIGINT)
    RETURNS BOOLEAN
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR @ (
    LEFTARG = versioned_int,
    RIGHTARG = TIMESTAMPTZ,
    PROCEDURE = versioned_int_at_time
);

CREATE OPERATOR = (
    LEFTARG = versioned_int,
    RIGHTARG = bigint,
    PROCEDURE = versioned_int_eq_bigint,
    COMMUTATOR = '=',
    NEGATOR = '<>',
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);

CREATE OPERATOR <> (
    LEFTARG = versioned_int,
    RIGHTARG = bigint,
    PROCEDURE = versioned_int_neq_bigint,
    COMMUTATOR = '<>',
    NEGATOR = '=',
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);

CREATE OPERATOR > (
    LEFTARG = versioned_int,
    RIGHTARG = bigint,
    PROCEDURE = versioned_int_gt_bigint,
    COMMUTATOR = <,
    NEGATOR = <=,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);

CREATE OPERATOR >= (
    LEFTARG = versioned_int,
    RIGHTARG = bigint,
    PROCEDURE = versioned_int_ge_bigint,
    COMMUTATOR = <=,
    NEGATOR = <,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);

CREATE OPERATOR < (
    LEFTARG = versioned_int,
    RIGHTARG = bigint,
    PROCEDURE = versioned_int_lt_bigint,
    COMMUTATOR = >,
    NEGATOR = >=,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG = versioned_int,
    RIGHTARG = bigint,
    PROCEDURE = versioned_int_le_bigint,
    COMMUTATOR = >=,
    NEGATOR = >,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);

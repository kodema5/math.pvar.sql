-- a correlation measure between 2 sets

-- state type
drop type if exists math.pcorr_t cascade;
create type math.pcorr_t as (
    n int,                  -- count of non-null items
    xy double precision,    -- sum of x*y
    xx double precision,    -- sum of x*x
    yy double precision,    -- sum of y*y
    sx double precision,    -- sum of x
    sy double precision     -- sum of y
);

-- zeros
create or replace function math.pcorr_t ()
    returns math.pcorr_t
    language sql
    immutable
as $$
    select (
        0,
        0.0, 0.0, 0.0,
        0.0, 0.0
    )::math.pcorr_t
$$;

------------------------------------------------------------------------------
-- correlation value
-- pcorr_t::double precision
--
create or replace function math.corr(
    a math.pcorr_t
)
    returns double precision
    language plpgsql
    strict
as $$
declare
    d numeric;
begin
    d = (a.n*a.xx-a.sx*a.sx)*(a.n*a.yy-a.sy*a.sy);

    if d = 0 then
        return null;
    end if;

    return ((a.n*a.xy)-(a.sx*a.sy))/sqrt(d);
end;
$$;

drop cast if exists ( math.pcorr_t as double precision);
create cast ( math.pcorr_t as double precision )
    with function math.corr(math.pcorr_t)
    as assignment;


------------------------------------------------------------------------------
-- pcorr_t + (double precision, double precision)
--
create or replace function math.pcorr (
    a math.pcorr_t,
    x double precision,
    y double precision
)
    returns math.pcorr_t
    language plpgsql
    immutable
as $$
begin
    if x is null or y is null then
        return a;
    end if;
    a = coalesce(a, math.pcorr_t());

    a.n = a.n + 1;
    a.xy = a.xy + (x * y);
    a.xx = a.xx + (x * x);
    a.yy = a.yy + (y * y);
    a.sx = a.sx + x;
    a.sy = a.sy + y;

    return a;
end;
$$;

create or replace aggregate math.pcorr (
    double precision,
    double precision
) (
    sfunc = math.pcorr,
    stype = math.pcorr_t
);


------------------------------------------------------------------------------
-- xy_t
--
drop type if exists math.xy_t cascade;
create type math.xy_t as (
    x double precision,
    y double precision
);

create or replace function math.xy_t (
    x double precision default 0.0,
    y double precision default 0.0
)
    returns math.xy_t
    language sql
    immutable
    security definer
as $$
    select (x,y)::math.xy_t
$$;

------------------------------------------------------------------------------
-- pcorr_t = pcorr_t + xy_t

create or replace function math.pcorr (
    a math.pcorr_t,
    xy math.xy_t default math.xy_t()
)
    returns math.pcorr_t
    language sql
    immutable
as $$
    select math.pcorr(
        coalesce(a, math.pcorr_t()),
        xy.x,
        xy.y
    )
$$;

drop operator if exists + (math.pcorr_t, math.xy_t);
create operator + (
    leftarg = math.pcorr_t,
    rightarg = math.xy_t,
    procedure = math.pcorr,
    commutator = +
);

create or replace aggregate math.pcorr (
    math.xy_t
) (
    sfunc = math.pcorr,
    stype = math.pcorr_t
);

\if :test
    create function tests.test_pcorr_vs_corr() returns setof text language plpgsql as $$
    declare
        xs double precision[] = array[1,2,.3,1,.2,.3];
        ys double precision[] = array[0,1,.5,0,1,.5];
        a double precision;
        b double precision;
    begin
        -- aggregate calculation
        --
        select corr(x,y), math.pcorr(x,y)::double precision
        into a, b
        from ( select unnest(xs) x, unnest(ys) y) t;
        return next ok(
            math.equ(a,b),
            'pcorr_t ~ corr'
        );

        -- iterative calculation (for online?)
        --
        declare
            i int;
            c math.pcorr_t;
        begin
            for i in 1..cardinality(xs)
            loop
                c = c + (xs[i], ys[i])::math.xy_t;
            end loop;
            return next ok(
                math.equ(a,c::double precision),
                'pcorr_t + xy_t'
            );
        end;
    end;
    $$;
\endif


------------------------------------------------------------------------------
-- pcorr_t + pcorr_t = pcorr_t
--
create or replace function math.pcorr (
    a math.pcorr_t,
    b math.pcorr_t
)
    returns math.pcorr_t
    language sql
    strict
as $$
    select (
        a.n + b.n,
        a.xy + b.xy,
        a.xx + b.xx,
        a.yy + b.yy,
        a.sx + b.sx,
        a.sy + b.sy
    )::math.pcorr_t
$$;

drop operator if exists + (math.pcorr_t, math.pcorr_t);
create operator + (
    leftarg = math.pcorr_t,
    rightarg = math.pcorr_t,
    procedure = math.pcorr,
    commutator = +
);

create or replace aggregate sum (
    math.pcorr_t
)(
    sfunc = math.pcorr,
    stype = math.pcorr_t
);


\if :test
    create function tests.test_pcorr_parallel() returns setof text language plpgsql as $$
    declare
        xs1 double precision[] = array[1,2,.3];
        xs2 double precision[] = array[1,.2,.3];
        xs3 double precision[] = xs1 || xs2;

        ys1 double precision[] = array[0,1,.5];
        ys2 double precision[] = array[0,1,.5];
        ys3 double precision[] = ys1 || ys2;

        a1 math.pcorr_t;
        a2 math.pcorr_t;
        a3 math.pcorr_t;
    begin
        select (math.pcorr(x,y)).* into a1 from (select unnest(xs1) x, unnest(ys1) y) t;
        select (math.pcorr(x,y)).* into a2 from (select unnest(xs2) x, unnest(ys2) y) t;
        select (math.pcorr(x,y)).* into a3 from (select unnest(xs3) x, unnest(ys3) y) t;
        return next ok(
            math.equ(
                (a1 + a2)::double precision,
                a3::double precision
            ),
            'pcorr_t + pcorr_t'
        );

        return next ok(
            (select math.equ(sum(x)::double precision, a3::double precision)
            from (values (a1), (a2)) t (x)),
            'sum(pcorr_t)');

    end;
    $$;
\endif


-- ref:
-- https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
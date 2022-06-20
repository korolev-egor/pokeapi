-- GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE sysadmin;
use role SYSADMIN;
drop database if exists Egor_Korolev;
create database Egor_Korolev;
use Egor_Korolev;
----------------
-- warehouses --
----------------
create warehouse if not exists deployment_wh
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    MAX_CLUSTER_COUNT = 1;

create warehouse if not exists tasks_wh
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    MAX_CLUSTER_COUNT = 1;

use warehouse deployment_wh;
create schema staging;
create schema storage;
create schema data_marts;
use schema Egor_Korolev.storage;

-------------------------
-- storage tables DDLs --
-------------------------

create table pokemon(
    id int NOT NULL,
    name varchar NOT NULL
);
create table type(
    id int NOT NULL,
    name varchar NOT NULL
);
create table stat(
    id int NOT NULL,
    name varchar NOT NULL
);
create table generation(
    id int NOT NULL,
    name varchar NOT NULL
);
create table move(
    id int NOT NULL,
    name varchar NOT NULL
);
create table pokemon_generation(
    id int IDENTITY,
    pokemon_id int NOT NULL,
    generation_id int NOT NULL
);
create table pokemon_type(
    id int IDENTITY,
    pokemon_id int NOT NULL,
    type_id int NOT NULL
);
create table pokemon_move(
    id int IDENTITY,
    pokemon_id int NOT NULL,
    move_id int NOT NULL
);
create table pokemon_stat(
    id int IDENTITY,
    pokemon_id int NOT NULL,
    stat_id int NOT NULL,
    base_stat int NOT NULL
);

-------------
-- staging --
-------------

use schema staging;
create or replace stage aws_stage
    URL = 's3://de-school-snowflake/snowpipe/Korolev/'
    file_format = (TYPE = json STRIP_OUTER_ARRAY = TRUE)
    credentials = (
        AWS_KEY_ID = blank
        AWS_SECRET_KEY = blank
    )
;

-------------------------
-- staging tables DDLs --
-------------------------

create table stg_pokemon(
    data variant,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime default current_timestamp()
);

create table stg_type(
    data variant,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime default current_timestamp()
);

create table stg_stat(
    data variant,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime default current_timestamp()
);

create table stg_generation(
    data variant,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime default current_timestamp()
);

create table stg_species(
    data variant,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime default current_timestamp()
);

create table stg_move(
    data variant,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime default current_timestamp()
);

-------------
-- streams --
-------------

create stream str_move on table stg_move;
create stream str_generation on table stg_generation;
create stream str_stat on table stg_stat;
create stream str_type on table stg_type;
create stream str_pokemon on table stg_pokemon;

create stream str_pokemon_move on table stg_move;
create stream str_pokemon_stat on table stg_pokemon;
create stream str_pokemon_type on table stg_type;
create stream str_pokemon_generation on table stg_species;

---------------
-- snowpipes --
---------------

create or replace pipe pipe_move
    auto_ingest=true
    as
    copy into stg_move(data, filename, amnd_user)
    from (select t.$1, METADATA$FILENAME, current_user() from @aws_stage t)
    pattern = '.*move[.]json';

create or replace pipe pipe_species
    auto_ingest=true
    as
    copy into stg_species(data, filename, amnd_user)
    from (select t.$1, METADATA$FILENAME, current_user() from @aws_stage t)
    pattern = '.*pokemon-species[.]json';

create or replace pipe pipe_generation
    auto_ingest=true
    as
    copy into stg_generation(data, filename, amnd_user)
    from (select t.$1, METADATA$FILENAME, current_user() from @aws_stage t)
    pattern = '.*generation[.]json';

create or replace pipe pipe_stat
    auto_ingest=true
    as
    copy into stg_stat(data, filename, amnd_user)
    from (select t.$1, METADATA$FILENAME, current_user() from @aws_stage t)
    pattern = '.*stat[.]json';
create or replace pipe pipe_type
    auto_ingest=true
    as
    copy into stg_type(data, filename, amnd_user)
    from (select t.$1, METADATA$FILENAME, current_user() from @aws_stage t)
    pattern = '.*type[.]json';
create or replace pipe pipe_pokemon
    auto_ingest=true
    as
    copy into stg_pokemon(data, filename, amnd_user)
    from (select t.$1, METADATA$FILENAME, current_user() from @aws_stage t)
    pattern = '.*pokemon[.]json';

-------------------
-- refresh pipes --
-------------------

alter pipe pipe_move refresh;
alter pipe pipe_species refresh;
alter pipe pipe_generation refresh;
alter pipe pipe_stat refresh;
alter pipe pipe_type refresh;
alter pipe pipe_pokemon refresh;

---------
-- UDF --
---------

-- if use urls instead of id need this function
-- create or replace function url_to_id(input_string varchar)
--     returns int
--     as 
--     $$
--     select REGEXP_SUBSTR(input_string, '\\d\\d\*', 1, 2)::int
--     $$
-- ;

-----------
-- tasks --
-----------

create or replace task moving_stg_stat
    warehouse = tasks_wh
    schedule = '5 minute'
    when system$stream_has_data('str_stat')
    as
    insert into storage.stat(id, name)
        select
            $1:id::int,
            $1:name::string
        from str_stat
        where metadata$action = 'INSERT';

create or replace task moving_stg_generation
    warehouse = tasks_wh
    schedule = '5 minute'
    when system$stream_has_data('str_generation')
    as
    insert into storage.generation(id, name)
        select
            $1:id::int,
            $1:name::string
        from str_generation
        where metadata$action = 'INSERT';

create or replace task moving_stg_type
    warehouse = tasks_wh
    schedule = '5 minute'
    when system$stream_has_data('str_type')
    as
    insert into storage.type(id, name)
        select
            $1:id::int,
            $1:name::string
        from str_type
        where metadata$action = 'INSERT';

create or replace task moving_stg_pokemon
    warehouse = tasks_wh
    schedule = '5 minute'
    when system$stream_has_data('str_pokemon')
    as
    insert into storage.pokemon(id, name)
        select
            $1:id::int,
            $1:name::string
        from str_pokemon
        where metadata$action = 'INSERT';

create or replace task moving_stg_move
    warehouse = tasks_wh
    schedule = '5 minute'
    when system$stream_has_data('str_move')
    as
    insert into storage.move(id, name)
        select
            $1:id::int,
            $1:name::string
        from str_move
        where metadata$action = 'INSERT';

create or replace task moving_stg_pokemon_move
    warehouse = tasks_wh
    schedule = '5 minute'
    when system$stream_has_data('str_pokemon_move')
    as
    insert into storage.pokemon_move(pokemon_id, move_id)
        select
            flat.value:id::int,
            raw.$1:id::int
        from str_pokemon_move raw, table(flatten($1:learned_by_pokemon)) flat
        where metadata$action = 'INSERT';

create or replace task moving_stg_pokemon_type
    warehouse = tasks_wh
    schedule = '5 minute'
    when system$stream_has_data('str_pokemon_type')
    as
    insert into storage.pokemon_type(pokemon_id, type_id)
        select
            flat.value:pokemon:id::int,
            raw.$1:id::int
        from str_pokemon_type raw, table(flatten($1:pokemon)) flat
        where metadata$action = 'INSERT';

create or replace task moving_stg_pokemon_stat
    warehouse = tasks_wh
    schedule = '5 minute'
    when system$stream_has_data('str_pokemon_stat')
    as
    insert into storage.pokemon_stat(pokemon_id, stat_id, base_stat)
        select
            raw.$1:id::int,
            flat.value:stat:id::int,
            flat.value:base_stat::int
        from str_pokemon_stat raw, table(flatten($1:stats)) flat
        where metadata$action = 'INSERT';

create or replace task moving_stg_pokemon_generation
    warehouse = tasks_wh
    schedule = '5 minute'
    when system$stream_has_data('str_pokemon_generation')
    as
    insert into storage.pokemon_generation(pokemon_id, generation_id)
        select
            flat.value:pokemon:id::int,
            raw.$1:generation:id::int
        from str_pokemon_generation raw, table(flatten($1:varieties)) flat
        where metadata$action = 'INSERT';

----------------
-- data_marts --
----------------

create or replace view data_marts.question_a
as
    select 
        name, 
        cnt as pokemon_count,
        cnt - lag(cnt) over (order by cnt) as difference_with_previous,
        lead(cnt) over (order by cnt) - cnt as difference_with_next
    from (
        select name, count(storage.pokemon_type.pokemon_id) as cnt
        from storage.pokemon_type right join storage.type on storage.type.id = storage.pokemon_type.type_id
        group by name
    )
;

create or replace view data_marts.question_b 
as
    select 
        name as move_name,
        cnt as pokemon_count,
        cnt - lag(cnt) over (order by cnt) as difference_with_previous,
        lead(cnt) over (order by cnt) - cnt as difference_with_next
    from (
        select name, count(pokemon_id) as cnt
        from storage.pokemon_move pm join storage.move m on pm.move_id = m.id
        group by name
    )
;

create or replace view data_marts.question_c
as
    select p.name, sum(ps.base_stat) as stat_sum
    from storage.pokemon_stat ps join storage.pokemon p on ps.pokemon_id=p.id
    group by p.name
    order by stat_sum desc
;

create or replace view data_marts.question_d
as
    select type, generation, max(pokemon_count) as pokemon_count
    from (
        select t.name as type, g.name as generation, count(p.id) as pokemon_count
        from storage.type t 
            join storage.pokemon_type pt on t.id = pt.type_id 
            join storage.pokemon p on pt.pokemon_id = p.id 
            join storage.pokemon_generation pg on p.id = pg.pokemon_id 
            join storage.generation g on pg.generation_id = g.id
        group by t.name, g.name
        union
        select t.name as type, g.name as generation, 0 as pokemon_count
        from storage.type t 
            cross join storage.generation g
        )
    group by type, generation
    order by type, generation
;
        
------------------
-- resume tasks --
------------------

alter task moving_stg_type resume;
alter task moving_stg_pokemon resume;
alter task moving_stg_generation resume;
alter task moving_stg_stat resume;
alter task moving_stg_move resume;
alter task moving_stg_pokemon_move resume;
alter task moving_stg_pokemon_type resume;
alter task moving_stg_pokemon_stat resume;
alter task moving_stg_pokemon_generation resume;

-- ----------------
-- execute tasks --
-- ----------------

-- execute task moving_stg_type;
-- execute task moving_stg_pokemon;
-- execute task moving_stg_generation;
-- execute task moving_stg_stat;
-- execute task moving_stg_move;
-- execute task moving_stg_pokemon_move;
-- execute task moving_stg_pokemon_type;
-- execute task moving_stg_pokemon_stat;
-- execute task moving_stg_pokemon_generation;

----------------------
-- check data marts --
----------------------

select * from data_marts.question_a;
select * from data_marts.question_b;
select * from data_marts.question_c;
select * from data_marts.question_d;

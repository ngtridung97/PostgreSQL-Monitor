/* Assume that we have a PostgreSQL connection and want to monitor some useful information

Below queries are an overview for that request. */

-- 1. Data type for each column

select

	a.table_catalog, a.table_schema, a.table_name, a.column_name, a.is_nullable, a.data_type, a.udt_catalog, a.udt_schema, a.udt_name, a.is_generated, a.is_updatable,
	
	b.n_tup_ins, b.n_live_tup, b.last_autoanalyze

from information_schema.columns a

inner join pg_stat_user_tables b

on a.table_schema = b.schemaname and a.table_name = b.relname;


-- 2. Running queries

select

	a.pid, age(clock_timestamp(), a.query_start), a.usename, a.query,

	b.mode, b.locktype, b.granted
	
from pg_stat_activity a

inner join pg_locks b

on a.pid = b.pid 

where a.query != '<IDLE>' and a.query not like '%pg_stat_activity%'

order by b.granted, b.pid desc;


-- 3. Database name and size

select
  
	datname as db,
	
	pg_size_pretty(pg_database_size(datname)) as db_size
	
from pg_database

order by pg_database_size(datname) desc;


-- 4. Table, index, schema name and size

select

	concat(nspname, '.', relname) as relation_name,
	
	pg_size_pretty(pg_total_relation_size(a.oid)) as relation_size,

	pg_size_pretty(sum(pg_total_relation_size(a.oid)) over(partition by nspname)) as schema_size

from pg_class a

left join pg_namespace b

on a.relnamespace = b.oid

where b.nspname not in ('pg_catalog', 'information_schema') and b.nspname !~ '^pg_toast'

order by pg_total_relation_size(a.oid) desc;


-- 5. Unused indexes
 
select

	relname as table_name,
	
	indexrelname as index_name,
	
	idx_scan,
	
	idx_tup_read,
	
	idx_tup_fetch,
	
	pg_size_pretty(pg_relation_size(indexrelname::regclass))
	
from pg_stat_all_indexes

where schemaname not in ('pg_catalog', 'pg_toast') and idx_scan = 0 and idx_tup_read = 0 and idx_tup_fetch = 0

order by pg_relation_size(indexrelname::regclass) desc;


-- 6. Table bloats - When you see a table with high bloats, then consider running VACUUM ANALYZE on it.

with

	constants as (select
	
			(select current_setting('block_size')::numeric) as bs,
			
			case when substring(split_part(version(), ' ', 2) from '#"[0-9]+.[0-9]+#"%' for '#') in ('8.0', '8.1', '8.2') then 27 else 23 end as hdr,
			
			case when trim(split_part(version(), ',', 3)) in ('mingw32', '64-bit') then 8 else 4 end as ma
	
		from version()),
	
	foo as (select
	
			schemaname, tablename, hdr, ma, bs,
			
			sum((1 - null_frac) * avg_width) as datawidth,
			
			max(null_frac) as maxfracsum,
			
			hdr + (select 1 + count(*) / 8
			
				from pg_stats s2
				
				where null_frac != 0 and s2.schemaname = s.schemaname and s2.tablename = s.tablename) as nullhdr
      
		from pg_stats s, constants
		
		group by schemaname, tablename, hdr, ma, bs),
		
	rs as (select
	
			ma, bs, schemaname,tablename,
			
			(datawidth + (hdr + ma - (case when hdr%ma = 0 then ma else hdr%ma end)))::numeric as datahdr,
			
			(maxfracsum * (nullhdr + ma - (case when nullhdr%ma = 0 then ma else nullhdr%ma end))) as nullhdr2
		
		from foo),
		
	sml as (select
	
			schemaname, tablename, cc.reltuples, cc.relpages, bs,
			
			ceil((cc.reltuples * ((datahdr+ma - (case when datahdr%ma = 0 then ma else datahdr%ma end)) + nullhdr2 + 4)) / (bs - 20::float)) as otta,
			
			coalesce(c2.relname, '?') as iname,
			
			coalesce(c2.reltuples, 0) as ituples,
			
			coalesce(c2.relpages, 0) as ipages,
			
			coalesce(ceil((c2.reltuples * (datahdr - 12)) / (bs - 20::float)), 0) as iotta -- very rough approximation, assumes all cols
		
		from rs

        inner join pg_class cc
        
        on cc.relname = rs.tablename

        inner join pg_namespace nn
        
        on cc.relnamespace = nn.oid and nn.nspname = rs.schemaname

        left join pg_index i
        
        on indrelid = cc.oid

        left join pg_class c2
        
        on c2.oid = i.indexrelid)
		
select

	current_database(), schemaname, tablename,
	
	round(case when otta = 0 or sml.relpages = 0 or sml.relpages = otta then 0.0 else sml.relpages / otta::numeric end, 1) as tbloat,
	
	case when relpages < otta then 0 else bs*(sml.relpages-otta)::bigint end as wastedbytes,
	
	iname,
	
	round(case when iotta = 0 or ipages = 0 or ipages = iotta then 0.0 else ipages / iotta::numeric end, 1) as ibloat,
	
	case when ipages < iotta then 0 else bs*(ipages-iotta) end as wastedibytes
		
from sml

where schemaname not in ('pg_catalog', 'information_schema') and schemaname !~ '^pg_toast';

-- 7. Index tuples

with

	foo as (select
	
			c.relname as ctablename, ipg.relname as indexname,
			
			x.indnatts as number_of_columns, idx_scan, idx_tup_read, idx_tup_fetch, indexrelname, indisunique
			
			from pg_index x
			
			inner join pg_class c
			
			on c.oid = x.indrelid
			
			inner join pg_class ipg
			
			on ipg.oid = x.indexrelid
			
			inner join pg_stat_all_indexes psai
			
			on x.indexrelid = psai.indexrelid)

select

    t.tablename, foo.indexname,
    
    c.reltuples as row_Count,
    
    pg_size_pretty(pg_relation_size(quote_ident(t.tablename)::text)) as table_size,
    
    pg_size_pretty(pg_relation_size(quote_ident(indexrelname)::text)) as index_size,
    
    idx_scan as number_of_scans, idx_tup_read as tuples_read, idx_tup_fetch as tuples_fetched

from pg_tables t

left join pg_class c

on t.tablename = c.relname

left join foo

on t.tablename = foo.ctablename

where t.schemaname = 'public'

order by t.tablename, indexname;


-- 8. Tables cache

select

	current_database(), schemaname, relname as tablename,
	
	heap_blks_read as heap_read,
	
	heap_blks_hit as heap_hit,
	
	((heap_blks_hit * 100) / nullif((heap_blks_hit + heap_blks_read), 0)) as ratio
	
from pg_statio_user_tables;
do $$ declare
tables record;
begin
for tables in (select tablename from pg_tables where schemaname=current_schema())loop
    execute 'drop table ' ||quote_ident(tables.tablename);
end loop;
end $$;

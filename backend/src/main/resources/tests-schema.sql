create EXTENSION IF not EXISTS ltree SCHEMA public;

;drop table IF EXISTS tree; drop type IF EXISTS OBJECT_TYPE;

;create type OBJECT_TYPE as enum('ROOT','DIR', 'FILE');

;CREATE TABLE IF NOT EXISTS public.tree (
	object_id varchar(36) PRIMARY KEY,
	object_type object_type,
	PATH ltree UNIQUE
);

create or replace function insCheck() RETURNS trigger AS $$
begin
	if NEW.object_type = 'ROOT' then
		if (nlevel(NEW.PATH) != 1) then
			raise exception 'Table constraint violation exception';
		ELSE
			RETURN NEW;
		END IF;
	ELSE
		IF nlevel(NEW.PATH) < 2
			OR 1 > (
				SELECT count(object_id)
				FROM tree
				WHERE subpath(NEW.PATH, 0, -1) = path
			) THEN
				RAISE EXCEPTION 'Table constraint violation exception';
		ELSE
			RETURN NEW;
		END IF;
	END IF;
END;
$$ language plpgsql;

create or replace trigger ins before insert on tree
for each row
EXECUTE procedure insCheck();

create or replace function leavesNoOrphans() RETURNS trigger AS $$
begin
	if exists (
		-- removes edge case where an object was modified or removed and also re-added in  the same statement
		with old_paths as (
			select old_table.PATH
			from old_table
			where old_table.object_type <> 'FILE'
				and old_table.PATH not in (select new_table.PATH from new_table)
		)
		select tree.PATH
		from tree join old_paths
		on old_paths.path @> tree.path
	) THEN raise exception 'This operation creates an orphan, an object references a moved object';
	END IF;
	RETURN NULL;
END;
$$ LANGUAGE plpgsql;

create or replace trigger leaves_no_orphans
	after update on tree
	referencing new TABLE AS new_table
		OLD TABLE AS old_table
	for each STATEMENT EXECUTE function leavesNoOrphans();




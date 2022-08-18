;DROP VIEW IF EXISTS public.file_view;
;DROP TABLE IF EXISTS public.tree_join_file;
;DROP TABLE IF EXISTS public.file;
;DROP TABLE IF EXISTS tree CASCADE;
;DROP TYPE IF EXISTS OBJECT_TYPE;

;CREATE EXTENSION IF not EXISTS ltree SCHEMA public;
;CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

;CREATE TYPE OBJECT_TYPE AS enum('ROOT','DIR', 'FILE');
;CREATE TABLE IF NOT EXISTS public.tree (
	object_id uuid NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
	object_type object_type NOT NULL,
	PATH ltree NOT NULL,
	user_id uuid NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	unique(user_id, path)
);
;CREATE INDEX IF NOT EXISTS tree_path_idx on tree using gist (path);

--create or replace function insCheck() RETURNS trigger AS $$
--begin
--	if NEW.object_type = 'ROOT' then
--		if (nlevel(NEW.PATH) != 1) then
--			raise exception 'Table constraint violation exception';
--		ELSE
--			RETURN NEW;
--		END IF;
--	ELSE
--		IF nlevel(NEW.PATH) < 2
--			OR 1 > (
--				SELECT count(object_id)
--				FROM tree
--				WHERE subpath(NEW.PATH, 0, -1) = path
--			) THEN
--				RAISE EXCEPTION 'Table constraint violation exception';
--		ELSE
--			RETURN NEW;
--		END IF;
--	END IF;
--END;
--$$ language plpgsql;
--
--create or replace trigger ins before insert on tree
--for each row
--EXECUTE procedure insCheck();
--
--create or replace function leavesNoOrphans() RETURNS trigger AS $$
--begin
--	if exists (
--		-- removes edge case where an object was modified or removed and also re-added in  the same statement
--		with old_paths as (
--			select old_table.PATH
--			from old_table
--			where old_table.object_type <> 'FILE'
--				and old_table.PATH not in (select new_table.PATH from new_table)
--		)
--		select tree.PATH
--		from tree join old_paths
--		on old_paths.path @> tree.path
--	) THEN raise exception 'This operation creates an orphan, an object references a moved object';
--	END IF;
--	RETURN NULL;
--END;
--$$ LANGUAGE plpgsql;
--
--create or replace trigger leaves_no_orphans
--	after update on tree
--	referencing new TABLE AS new_table
--		OLD TABLE AS old_table
--	for each STATEMENT EXECUTE function leavesNoOrphans();

;DROP VIEW IF EXISTS public.file_view;
;DROP TABLE IF EXISTS public.tree_join_file;
;DROP TABLE IF EXISTS public.file;

;CREATE TABLE IF NOT EXISTS public.file (
	file_id uuid PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
	checksum varchar(64),
	SIZE bigint,
	user_id UUID NOT NULL DEFAULT uuid_generate_v4(),
	uploaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

;CREATE TABLE IF NOT EXISTS public.tree_join_file (
	object_id uuid NOT NULL REFERENCES public.tree DEFERRABLE INITIALLY DEFERRED,
	file_id uuid NOT NULL REFERENCES public.file DEFERRABLE INITIALLY DEFERRED,
	linked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (object_id, file_id)
);

;CREATE OR REPLACE VIEW public.file_view AS
	SELECT tree.object_id,
		file.file_id,
		file.user_id,
		file.uploaded_at,
		tree_join_file.linked_at,
		file.checksum,
		file.size
	FROM public.file
	LEFT JOIN public.tree_join_file
	ON tree_join_file.file_id = file.file_id
	LEFT JOIN public.tree
	ON tree.object_id = tree_join_file.object_id;

CREATE OR REPLACE FUNCTION file_view_ins() RETURNS TRIGGER AS $$
BEGIN
	IF 0 < num_nulls(NEW.object_id, NEW.user_id) OR
		(SELECT tree.user_id FROM tree WHERE NEW.object_id = tree.object_id AND NEW.user_id = tree.user_id) IS NULL THEN
		RAISE EXCEPTION 'User does not have access to file, or file object does not exist';
	ELSEIF 0 = num_nulls(NEW.file_id, NEW.linked_at) AND 0 = num_nonnulls(NEW.uploaded_at, NEW.checksum, NEW.size) THEN
		INSERT INTO public.tree_join_file (object_id, file_id, linked_at)
			values(NEW.object_id, NEW.file_id, NEW.linked_at);
	ELSEIF 0 = num_nulls(NEW.file_id, NEW.linked_at, NEW.uploaded_at, NEW.checksum, NEW.size) THEN
			INSERT INTO public.tree_join_file (object_id, file_id, linked_at)
				values(NEW.object_id, NEW.file_id, NEW.linked_at);
			INSERT INTO public.file (file_id, checksum, SIZE, user_id, uploaded_at)
				values(NEW.file_id, NEW.checksum, NEW.SIZE, NEW.user_id, NEW.uploaded_at);
	ELSE
		RAISE EXCEPTION 'improper arguments: missing required fields or provided too many fields';
	END IF;
	RETURN NEW;
END;
$$ language plpgsql;

CREATE OR REPLACE TRIGGER file_view_ins_trigger INSTEAD OF INSERT ON public.file_view
	FOR EACH ROW EXECUTE PROCEDURE file_view_ins();

-- Returns affected file UUIDs and if the file resource is an orphan
CREATE OR REPLACE FUNCTION file_view_del(in_object_id UUID, in_file_id UUID, in_user_id UUID)
RETURNS bool
LANGUAGE plpgsql
AS $$
DECLARE
	orphan bool := FALSE;
BEGIN
--	 fetch the input record to determine it is accurate & exists
	CREATE TEMP TABLE IF NOT EXISTS del_links AS
		(SELECT * FROM file_view
		WHERE file_view.object_id = in_object_id
			AND file_view.file_id = in_file_id
				AND file_view.user_id = in_user_id);
--	 force rollback on any improper or old record
	IF NOT EXISTS(SELECT * FROM del_links)
		THEN DROP TABLE del_links;
			RAISE EXCEPTION 'record not found';
	END IF;
--	 delete edge
	DELETE FROM public.tree_join_file
	WHERE tree_join_file.file_id = in_file_id AND tree_join_file.object_id = in_object_id;
--	 determine if need to delete file and populate return field
	SELECT NOT EXISTS(
		SELECT * FROM tree_join_file
		WHERE tree_join_file.file_id = in_file_id)
	INTO orphan;
--	 delete file if required
	IF orphan THEN
		DELETE FROM public.file
		WHERE file.file_id IN (SELECT del_links.file_id FROM del_links);
	END IF;
	DROP TABLE del_links;
	RETURN orphan;
END;$$





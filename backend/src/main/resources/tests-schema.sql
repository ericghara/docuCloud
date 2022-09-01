;DROP VIEW IF EXISTS public.file_view;
;DROP TABLE IF EXISTS public.tree_join_file;
;DROP TABLE IF EXISTS public.file;
;DROP TABLE IF EXISTS tree CASCADE;
;DROP TYPE IF EXISTS OBJECT_TYPE;

;CREATE EXTENSION IF not EXISTS ltree SCHEMA public;
;CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

;DROP TABLE IF EXISTS tree CASCADE; DROP TYPE IF EXISTS OBJECT_TYPE;
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

CREATE OR REPLACE FUNCTION insCheck() RETURNS TRIGGER AS $$
BEGIN
	IF NEW.object_type = 'ROOT' THEN
		IF (nlevel(NEW.PATH) != 0) THEN
			RAISE EXCEPTION 'Table constraint violation exception - ROOT';
		ELSE
			RETURN NEW;
		END IF;
	ELSE
		IF nlevel(NEW.PATH) < 1
			OR 1 > (
				SELECT count(object_id)
				FROM tree
				WHERE subpath(NEW.PATH, 0, -1) = PATH AND NEW.user_id = user_id
			) THEN
				RAISE EXCEPTION 'Table constraint violation exception - creates orphan';
		ELSE
			RETURN NEW;
		END IF;
	END IF;
END;
$$ language plpgsql;

-- separate out first part with on update of object_type
--CREATE OR REPLACE FUNCTION updCheck() RETURNS TRIGGER AS $$
--BEGIN
--	IF NEW.object_type <> OLD.object_type THEN
--		RAISE EXCEPTION 'Table constraint violation - cannot modify object type';
--	ELSEIF OLD.object_type = 'ROOT' THEN
--		IF nlevel(NEW.PATH) != 1 THEN
--			RAISE EXCEPTION 'Table constraint violation - ROOT nlevel must = 1';
--		END IF;
--	ELSE THEN
--		IF nlevel(NEW.PATH) < 2 THEN
--			RAISE EXCEPTION 'Table constraint violation - DIR or FILE nlevel must be >= 2';
--		-- new path must have parent
--		ELSEIF NOT EXISTS (
--			SELECT 1
--			FROM tree
--			WHERE subpath(NEW.PATH, 0, -1) = PATH
--			) THEN
--			RAISE EXCEPTION 'Table constraint violation - DIR or FILE must have a parent';
--		-- do move
--		ELSEIF object_type = 'DIR' AND EXISTS (
--			SELECT 1
--			FROM tree
--			WHERE OLD.PATH @> PATH) THEN
--			RAISE EXCEPTION 'Table constraint violation - '
--		THEN
--		END IF;
--	END IF;
--	RETURN NEW;
--END;
--$$ language plpgsql;

--CREATE OR REPLACE FUNCTION leavesNoOrphans() RETURNS TRIGGER AS $$
--BEGIN
--	IF EXISTS (
--		-- removes edge case where an object was modified or removed and also re-added in  the same statement
--		WITH old_paths AS (
--			SELECT old_table.PATH
--			FROM old_table
--			WHERE old_table.object_type <> 'FILE'
--				AND old_table.PATH NOT IN (SELECT new_table.PATH FROM new_table)
--		)
--		SELECT tree.PATH
--		FROM tree JOIN old_paths
--		ON old_paths.path @> tree.PATH
--	) THEN RAISE EXCEPTION 'This operation creates an orphan, an object references a moved object';
--	END IF;
--	RETURN NULL;
--END;
--$$ LANGUAGE plpgsql;


-- ins trigger
CREATE OR REPLACE TRIGGER ins BEFORE INSERT ON tree
FOR EACH ROW
EXECUTE PROCEDURE insCheck();


-- upd triggers
--CREATE OR REPLACE TRIGGER leaves_no_orphans
--	AFTER UPDATE ON tree
--	REFERENCING NEW TABLE AS new_table
--		OLD TABLE AS old_table
--	FOR EACH STATEMENT EXECUTE FUNCTION leavesNoOrphans();

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
	SELECT tree_join_file.object_id,
		file.file_id,
		file.user_id,
		file.uploaded_at,
		tree_join_file.linked_at,
		file.checksum,
		file.size
	FROM public.file
	LEFT JOIN public.tree_join_file
	ON tree_join_file.file_id = file.file_id;

CREATE OR REPLACE FUNCTION file_view_ins() RETURNS TRIGGER AS $$
DECLARE
	fileObj tree;
BEGIN
	IF 0 <> num_nulls(NEW.object_id, NEW.user_id) THEN
		RAISE EXCEPTION 'user_id and object_id must be specified';
	END IF;
	SELECT * FROM tree INTO fileObj WHERE NEW.object_id = tree.object_id;
	IF fileObj IS NULL
		THEN RAISE EXCEPTION 'Record with provided object_id not found in tree';
	ELSEIF fileObj.user_id <> NEW.user_id
		THEN RAISE EXCEPTION 'The provided user_id does not match the record user_id';
	ELSEIF fileObj.object_type <> 'FILE'
		THEN RAISE EXCEPTION 'The object specified is not a file';
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





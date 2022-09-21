## docuCloud
A versioned cloud filestore for personal documents

![er](https://raw.githubusercontent.com/ericghara/docuCloud/main/backend/misc/abbreviated_er_diagram.png)
*Abbreviated Entity Relationship Diagram:* The `tree` table models the hierarchical structure of a filesystem (or file**tree**).  Each tree object may map to multiple files. Each `file` represents a distinct version of one or many objects in the `tree` table.  If a tree object is copied, edges in `tree_join_file` are copied, avoiding duplication of identical files.  The `file` table maps directly to S3 object identifiers.

### Tech Used
- Postgres + Ltree Module
- Spring Reactive Stack
- jOOQ
- AWS S3
- Testing: Minio, TestContainers, JUnit5, Mockito

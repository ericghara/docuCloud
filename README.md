## DocuCloud
A cloud filesystem that implements file versioning through copy on write.  The project is similar in idea to Google Drive or DropBox but is not intended to be a clone of either.  DocuCloud instead is heavily focused on file versioning (optimized for binary blobs) and avoiding data duplication on file copy operations, through a technique similar to symbolic linking in UNIX filesystems.
<p>While S3 is used in the project, it is only serves as a highly reliable and available data store; the (limited) ability for S3 to represent file hierarchies and file versions is not used.  The benefits of this choice are:

- Reduced cloud storage space requirements through techniques that minimize data duplication
- Looser coupling to the cloud provider, using only PUT and DELETE operations
- Reduced number of requests to S3
- Greater flexibility for search operations

![er](https://raw.githubusercontent.com/ericghara/DocuCloud/main/backend/misc/abbreviated_er_diagram.png)
*Abbreviated Entity Relationship Diagram:* The `tree` table models the hierarchical structure of a filesystem (or file**tree**).  Each tree object may map to multiple files. Each `file` represents a distinct version of one or many objects in the `tree` table.  If a tree object is copied, edges in `tree_join_file` are copied, avoiding duplication of identical files.  The `file` table maps directly to S3 object identifiers.

### Tech Used
- Postgres + Ltree Module
- Spring Reactive Stack
- jOOQ
- AWS S3
- Testing: MinIO, TestContainers, JUnit5, Mockito

### Notable Challenges and Solutions
- Representing hierarchical data with SQL:
    - Postgres `ltree` module
    - Trigger functions to enforce certain invariants (file must have a parent folder etc.)
- Distributed transactions across multiple DB tables and S3
    - Used only atomic S3 operations (i.e. batch delete) which triggered database rollbacks on failure
- High memory usage of multiple concurrent file uploads and downloads
    - All uploads and downloads are performed using buffered streams (i.e. `Flux<ByteBuffer>`) allowing memory usage per request to have a constant maximum, independent of file size
- Inability for ltree paths to natively represent non-alphanumeric characters:
    -   Developed a custom encoding for special characters allowing stored path strings to remain reasonably human-readable, but represented alphanumerically
- Inability of ltree to use custom path separators:
    -   Encoded occurrences of the default path separator (`.`) as alphanumeric characters and the custom path separator (`/`) as the default (`.`)
- File Integrity
    - A SHA1 checksum is required for all uploads
    - The checksum is provided to S3 in the PUT request which S3 validates
    - The checksum is stored in the `file` table.  The client can fetch the hash on a file GET request and verify.

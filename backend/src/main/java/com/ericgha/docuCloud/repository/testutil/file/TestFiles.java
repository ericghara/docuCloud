package com.ericgha.docuCloud.repository.testutil.file;

import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.jooq.tables.records.FileRecord;
import com.ericgha.docuCloud.repository.testutil.tree.TestFileTree;
import lombok.NonNull;

import java.util.Comparator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestFiles {

    private final TestFileTree tree;
    private final FileTestQueries fileQueries;
    // this isn't really used but the size field of each new file is an autoincrementing long
    private final Comparator<TreeDto> treeDtoComp;
    private final Comparator<FileViewDto> fileViewDtoComp;
    private long fileSize = 0;

    // keys are file checksums
    private final NavigableMap<String, NavigableSet<FileViewDto>> fileViewDtoByObjectPath;
    private final NavigableMap<String, NavigableSet<TreeDto>> treeDtoByFileChecksum;

    public TestFiles(TestFileTree tree, FileTestQueries fileQueries,
                     Comparator<TreeDto> treeDtoComp, Comparator<FileViewDto> fileViewDtoComp) {
        this.tree = tree;
        this.fileQueries = fileQueries;
        this.treeDtoComp = treeDtoComp;
        this.fileViewDtoComp = fileViewDtoComp;
        fileViewDtoByObjectPath = new TreeMap<>();
        treeDtoByFileChecksum = new TreeMap<>();
    }

    // If file doesn't exist it is created, if file does exist a link is made
    // In the csv the checksum field in file_view is used as a human friendly file key
    public NavigableSet<FileViewDto> createFileViewDtos(String csv) {
        return TestFilesCsvParser.parse( csv )
                .map(csvRecord ->  this.createFileViewDto( csvRecord.treePath(), csvRecord.fileChecksum() ) )
                .collect( Collectors.toCollection(
                        () -> new TreeSet<FileViewDto>( fileViewDtoComp ) )
                );
    }

    public FileViewDto createFileViewDto(String pathStr, String checksum) {
         ObjectFileLink link = this.insertFileViewDto( pathStr, checksum );
         this.recordNewFileViewDto( link );
         return link.fileViewDto();
    }

    // throws if no files are linked to given path
    public NavigableSet<FileViewDto> getCreatedFileViewDtosForObject(String pathStr ) throws IllegalArgumentException {
        var records = fileViewDtoByObjectPath.get(pathStr);
        if (Objects.isNull(records) ) {
            throw new IllegalArgumentException("No objects at specified path are linked to any objects created by this instance");
        }
        return records;
    }

    public NavigableSet<TreeDto> getCreatedObjectsForFile(String fileChecksum ) throws IllegalArgumentException {
        var records = treeDtoByFileChecksum.get(fileChecksum);
        if (Objects.isNull(records) ) {
            throw new IllegalArgumentException("No files with the provided checksum are linked to any objects created by this instance");
        }
        return records;
    }

//    public NavigableMap<String, NavigableSet<FileViewDto>> getCreatedFileViewDtosGroupedByObjectPath() {
//        NavigableMap<String, NavigableSet<FileViewDto>> copy = new TreeMap<>();
//        fileViewDtoByObjectPath.forEach( (key, set) -> {
//            set.stream().map( fvr -> fvr.into(FileViewDto.class) )
//        } );
//
//        return
//
//    }

    // map where keys are checksums and values are sets of TreeDtos currently linked to (file with) checksum
    // if multiple fileIds have the same checksum this will break, but very unlikely as fileId (UUIDs) are being autogenerated
    public NavigableMap<String, NavigableSet<TreeDto>> fetchTreeDtosGroupedByLinkedFileChecksum() {
        NavigableMap<String, NavigableSet<TreeDto>> current = new TreeMap<>();
        fetchUserFileViewDtos().forEach( fvr -> {
            String checksum = fvr.getChecksum();
            TreeDto linkedObj = tree.fetchCurRecord( fvr.getObjectId() );
            NavigableSet<TreeDto> checksumTreeDtos = current.computeIfAbsent(
                    checksum, k -> new TreeSet<TreeDto>( treeDtoComp ) );
            checksumTreeDtos.add( linkedObj );
        } );
        return current;
    }


    // records sorted by fileViewDtoComp provided in constructor
    public NavigableSet<FileViewDto> fetchUserFileViewDtos() {
        UUID userId = tree.getUserId();
        return fileQueries.fetchRecordsByUserId( userId, fileViewDtoComp )
                .collect( () -> new TreeSet<FileViewDto>( fileViewDtoComp ), TreeSet::add )
                .block();
    }

    // records sorted by treeDtoComp provided in constructor
    public NavigableSet<TreeDto> fetchObjectsLinkedTo(String checksum) {
        return fileQueries.fetchRecordsByChecksum( checksum, tree.getUserId() )
                .map( fvr -> Objects.requireNonNull( tree.fetchCurRecord( fvr.getObjectId() ), "Failure fetching tree record" ) )
                .sort( treeDtoComp )
                .collect( () -> new TreeSet<TreeDto>( treeDtoComp ), TreeSet::add )
                .block();
    }

    // records sorted by treeDtoComp provided in constructor
    public NavigableSet<TreeDto> fetchObjectsLinkedTo(UUID fileId) {
        return fileQueries.fetchRecordsByFileId( fileId, tree.getUserId() )
                .map( fvr -> Objects.requireNonNull( tree.fetchCurRecord( fvr.getObjectId() ), "Failure fetching tree record" ) )
                .collect( () -> new TreeSet<TreeDto>( treeDtoComp ), TreeSet::add )
                .block();
    }

    public NavigableSet<FileViewDto> fetchFileViewDtosLinkedTo(String treePathStr) {
        TreeDto treeDto = tree.fetchByObjectPath( treePathStr );
        return fetchFileViewDtosLinkedTo( treeDto );

    }

    // uses objectId from treeDto, does not consider path, userId pulled from tree provided to constructor
    public NavigableSet<FileViewDto> fetchFileViewDtosLinkedTo(TreeDto treeDto) {
        return fileQueries.fetchRecordsByObjectId( treeDto.getObjectId() )
                .collect( () -> new TreeSet<FileViewDto>( fileViewDtoComp ), TreeSet::add )
                .block();
    }

    // only parses fileId from fileRecord, ie does not use checksum
    public NavigableSet<TreeDto> fetchObjectsLinkedTo(FileRecord fileViewDto) {
        return fetchObjectsLinkedTo( fileViewDto.getFileId() );
    }

    ObjectFileLink insertFileViewDto(String treePath, String checksum) {
        TreeDto treeDto = Objects.requireNonNull( tree.getOrigRecord( treePath ), "treePath not found" );
        UUID objectId = treeDto.getObjectId();
        UUID userId = treeDto.getUserId();
        FileViewDto.FileViewDtoBuilder toCreate = FileViewDto.builder().objectId( objectId )
                .fileId( UUID.randomUUID() )
                .userId( userId );
        FileViewDto createdRecord;
        if (treeDtoByFileChecksum.containsKey( checksum )) {
            // if duplicate objectPath <-> checksum link, table constraints prevent insertion
            // (unique composite key constraint on tree_join_file)
            createdRecord = fileQueries.createLink( toCreate.build() )
                    .flatMap( rec -> fileQueries.fetchFileViewDto( rec.getObjectId(), rec.getFileId() ) )
                    .block();
        } else {
            // create new file with link
            toCreate.checksum( checksum )
                    .size( fileSize++ );
            createdRecord = fileQueries.createFileWithLinks( toCreate.build() ).block();
        }
        return new ObjectFileLink( treeDto, createdRecord );
    }

    // fileViewDto => newly created file or link
    // treeDto => object being linked to file
    void recordNewFileViewDto(ObjectFileLink link) {
        FileViewDto fileViewDto = link.fileViewDto();
        TreeDto treeDto = link.treeDto();
        String treePath = treeDto.getPath().data();
        String checksum = fileViewDto.getChecksum();
        NavigableSet<FileViewDto> fileRecSet = fileViewDtoByObjectPath.computeIfAbsent(
                treePath, k -> new TreeSet<FileViewDto>( fileViewDtoComp ) );
        NavigableSet<TreeDto> treeRecSet = treeDtoByFileChecksum.computeIfAbsent(
                checksum, k -> new TreeSet<TreeDto>( treeDtoComp ) );
        fileRecSet.add( fileViewDto );
        treeRecSet.add( treeDto );
    }

    ObjectFileLink createLinkOrFile(CsvRecord csvRecord) {
        String treePath = csvRecord.treePath();
        String checksum = csvRecord.fileChecksum();
        return this.insertFileViewDto( treePath, checksum );
    }

    // using ObjectId in fileViewDto fetches obj path
    String fetchObjectPath(FileViewDto fileViewDto) {
        return Objects.requireNonNull( tree.fetchCurRecord( fileViewDto.getObjectId() ),
                        "TreeDto does not exist for objectId" )
                .getPath()
                .data();
    }

    private record ObjectFileLink(TreeDto treeDto, FileViewDto fileViewDto) {
    }

    private record CsvRecord(String treePath, String fileChecksum) {
    }

    static class TestFilesCsvParser {

        private TestFilesCsvParser() throws IllegalAccessException {
            throw new IllegalAccessException("Do not instantiate -- utility class");

        }
        static private final String COMMENT_REGEX = "^\s*#.*";
        static private final String DELIM_REGEX = "\s*,\s*";

        static Stream<CsvRecord> parse(@NonNull String csv) {
            return csv.lines().map( TestFilesCsvParser::splitLine )
                    .filter( split -> split.length == 2 )
                    .map( TestFilesCsvParser::toCsvRecord );
        }

        // returns String[0] for comments else String[2]
        private static String[] splitLine(String line) throws IllegalStateException {
            if (line.matches( COMMENT_REGEX ) || line.isBlank()) {
                // a comment;
                return new String[0];
            }
            String[] split = line.strip()
                    .split( DELIM_REGEX );
            if (split.length != 2) {
                throw new IllegalArgumentException( "Unable to parse line: " + line );
            }
            return split;
        }

        private static CsvRecord toCsvRecord(String[] split) {
            return new CsvRecord( split[0], split[1] );
        }
    }
}

package com.ericgha.docuCloud.repository.testtool.file;

import com.ericgha.docuCloud.dto.FileDto;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.jooq.tables.records.FileRecord;
import com.ericgha.docuCloud.repository.testtool.file.ObjectResourceAdjacencyParser.ObjectResourceAdjacency;
import com.ericgha.docuCloud.repository.testtool.tree.TestFileTree;
import org.springframework.core.convert.converter.Converter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

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
    private final Converter<FileViewDto, FileDto> fileViewToFile;
    private long fileSize = 0;

    // keys are file checksums
    private final NavigableMap<String, NavigableSet<FileViewDto>> fileViewDtoByObjectPath;
    private final NavigableMap<String, NavigableSet<TreeDto>> treeDtoByChecksum;
    private final NavigableMap<String, FileDto> fileDtoByChecksum;

    private final Scheduler blockingSched = Schedulers.newBoundedElastic( 1, 100_000, "blocking-task-scheduler" );

    public TestFiles(TestFileTree tree, FileTestQueries fileQueries, Converter<FileViewDto, FileDto> fileViewToFile) {
        this.tree = tree;
        this.fileQueries = fileQueries;
        this.fileViewToFile = fileViewToFile;
        fileViewDtoByObjectPath = new TreeMap<>();
        treeDtoByChecksum = new TreeMap<>();
        fileDtoByChecksum = new TreeMap<>();
    }

    // If file doesn't exist it is created, if file does exist a link is made
    // In the csv the checksum field in file_view is used as a human friendly file key
    public NavigableSet<FileViewDto> insertFileViewRecords(String csv) {
        return ObjectResourceAdjacencyParser.parse( csv )
                .map( csvRecord -> this.insertFileViewRecord( csvRecord.treePath(), csvRecord.fileChecksum() ) )
                .collect( Collectors.toCollection(
                        TreeSet::new )
                );
    }

    public FileViewDto insertFileViewRecord(String pathStr, String checksum) {
        ObjectFileLink link = this.insertFileView( pathStr, checksum );
        this.recordNewFileViewDto( link );
        return link.fileViewDto();
    }

    // throws if no files are linked to given path
    public NavigableSet<FileViewDto> getOrigFileViewsFor(String pathStr) throws IllegalArgumentException {
        var records = fileViewDtoByObjectPath.get( pathStr );
        if (Objects.isNull( records )) {
            throw new IllegalArgumentException( "No objects at specified path are linked to any objects created by this instance" );
        }
        return new TreeSet<>( records );
    }

    public NavigableSet<TreeDto> getOrigTreeObjectsFor(String fileChecksum) throws IllegalArgumentException {
        var records = treeDtoByChecksum.get( fileChecksum );
        if (Objects.isNull( records )) {
            throw new IllegalArgumentException( "No files with the provided checksum are linked to any objects created by this instance" );
        }
        return new TreeSet<>( records );
    }

    public FileDto getOrigFileFor(String checksum) {
        var dto = fileDtoByChecksum.get( checksum );
        if (Objects.isNull( dto )) {
            throw new IllegalArgumentException( String.format( "No record was created with the checksum: %s", checksum ) );
        }
        return dto;
    }

    // throws if treePathStr not found or no edge between treeObj and fileRes exists
    public FileViewDto getOrigFileViewFor(String treePathStr, String fileResChecksum ) throws IllegalArgumentException {
        return this.getOrigFileViewsFor( treePathStr )
                .stream()
                .filter(fileViewDto -> fileViewDto.getChecksum().equals( fileResChecksum ) )
                .findFirst()
                .orElseThrow( () -> new IllegalArgumentException("Unable to find edge between treeObj and fileRes") );
    }

    public NavigableMap<String, NavigableSet<FileViewDto>> getOrigFileViewsGroupedByPathStr() {
        NavigableMap<String, NavigableSet<FileViewDto>> copy = new TreeMap<>();
        fileViewDtoByObjectPath.keySet().forEach( pathStr ->
                copy.put( pathStr, this.getOrigFileViewsFor( pathStr ) )
        );
        return copy;
    }

    public NavigableMap<String, NavigableSet<TreeDto>> getTreeDtosByLinkedFileChecksum() {
        NavigableMap<String, NavigableSet<TreeDto>> copy = new TreeMap<>();
        treeDtoByChecksum.keySet().forEach( checksum ->
                copy.put( checksum, this.getOrigTreeObjectsFor( checksum ) )
        );
        return copy;
    }

    // map where keys are checksums and values are sets of TreeDtos currently linked to (file with) checksum
    // if multiple fileIds have the same checksum this will break, but very unlikely as fileId (UUIDs) are being autogenerated
    public NavigableMap<String, NavigableSet<TreeDto>> fetchTreeDtosByLinkedFileChecksum() {
        NavigableMap<String, NavigableSet<TreeDto>> current = new TreeMap<>();
        fetchUserFileViewDtos().forEach( fvr -> {
            String checksum = fvr.getChecksum();
            TreeDto linkedObj = tree.fetchCurRecord( fvr.getObjectId() );
            NavigableSet<TreeDto> checksumTreeDtos = current.computeIfAbsent(
                    checksum, k -> new TreeSet<TreeDto>() );
            checksumTreeDtos.add( linkedObj );
        } );
        return current;
    }

    public FileViewDto fetchFileViewDto(String pathStr, String checksum) {
        UUID objectId = tree.fetchByObjectPath( pathStr ).getObjectId();
        return fileQueries.fetchRecordByObjectIdChecksum( objectId, checksum, tree.getUserId() )
                .block();
    }

    public NavigableMap<String, NavigableSet<FileViewDto>> fetchFileViewDtosGroupedByObjectPathStr() {
        return this.groupByPathStr(
                fileQueries.fetchRecordsByUserId( tree.getUserId() )
                       .publishOn( blockingSched ) );
    }

    public NavigableMap<String, NavigableSet<FileViewDto>> fetchFileViewDtosGroupedByObjectPathStr(
            Stream<ObjectResourceAdjacency> adjacencies) {
        Flux<FileViewDto> fileFlux = Flux.fromStream( adjacencies )
                .publishOn( blockingSched )
                .map( adj -> this.fetchFileViewDto( adj.treePath(), adj.fileChecksum() ) )
                .publishOn( blockingSched );
        return this.groupByPathStr( fileFlux );
    }

    private NavigableMap<String, NavigableSet<FileViewDto>> groupByPathStr(Flux<FileViewDto> fileViews) {
        return (NavigableMap<String, NavigableSet<FileViewDto>>) fileViews.groupBy( fileViewDto -> tree.fetchCurRecord( fileViewDto.getObjectId() ).getPathStr(),
                        fileViewDto -> fileViewDto )
                .publishOn( blockingSched )
                .collectMap( GroupedFlux::key,
                        gf -> gf.collect( () -> (NavigableSet<FileViewDto>) new TreeSet<FileViewDto>(),
                                (set, dto) -> set.add( dto ) ).block(),
                        TreeMap::new )
                .block();

    }


    // records sorted by fileViewDtoComp provided in constructor
    public NavigableSet<FileViewDto> fetchUserFileViewDtos() {
        UUID userId = tree.getUserId();
        return fileQueries.fetchRecordsByUserId( userId )
                .collect( () -> new TreeSet<FileViewDto>(), TreeSet::add )
                .block();
    }

    // records sorted by treeDtoComp provided in constructor
    public NavigableSet<TreeDto> fetchObjectsLinkedTo(String checksum) {
        return fileQueries.fetchRecordsByChecksum( checksum, tree.getUserId() )
                .publishOn( blockingSched )
                .map( fvr -> Objects.requireNonNull( tree.fetchCurRecord( fvr.getObjectId() ), "Failure fetching tree record" ) )
                .sort()
                .collect( () -> new TreeSet<TreeDto>(), TreeSet::add )
                .block();
    }

    // records sorted by treeDtoComp provided in constructor
    public NavigableSet<TreeDto> fetchObjectsLinkedTo(UUID fileId) {
        return fileQueries.fetchRecordsByFileId( fileId, tree.getUserId() )
                .publishOn( blockingSched )
                .map( fvr -> Objects.requireNonNull( tree.fetchCurRecord( fvr.getObjectId() ), "Failure fetching tree record" ) )
                .collect( () -> new TreeSet<TreeDto>(), TreeSet::add )
                .block();
    }

    public NavigableSet<FileDto> fetchFileDtosLinkedTo(String treePathStr) {
        TreeDto treeDto = tree.fetchByObjectPath( treePathStr );
        return fetchFileDtosLinkedTo( treeDto );

    }

    // uses objectId from treeDto, does not consider path, userId pulled from tree provided to constructor
    public NavigableSet<FileDto> fetchFileDtosLinkedTo(TreeDto treeDto) {
        return fileQueries.fetchRecordsByObjectId( treeDto.getObjectId() )
                .map( fileViewToFile::convert )
                .collect( () -> new TreeSet<FileDto>(), TreeSet::add )
                .block();
    }

    // only parses fileId from fileRecord, ie does not use checksum
    public NavigableSet<TreeDto> fetchObjectsLinkedTo(FileRecord fileViewDto) {
        return fetchObjectsLinkedTo( fileViewDto.getFileId() );
    }

    ObjectFileLink insertFileView(String treePath, String checksum) {
        TreeDto treeDto = Objects.requireNonNull( tree.getOrigRecord( treePath ), "treePath not found" );
        UUID objectId = treeDto.getObjectId();
        UUID userId = treeDto.getUserId();
        FileViewDto.FileViewDtoBuilder toCreate = FileViewDto.builder().objectId( objectId )
                .userId( userId );
        FileViewDto createdRecord;
        if (treeDtoByChecksum.containsKey( checksum )) {
            // if duplicate objectPath <-> checksum link, table constraints prevent insertion
            // (unique composite key constraint on tree_join_file)
            UUID fileId = this.getOrigFileFor( checksum ).getFileId();
            createdRecord = fileQueries.createLink( toCreate.fileId( fileId ).build() )
                    .flatMap( rec -> fileQueries.fetchFileViewDto( rec.getObjectId(), rec.getFileId() ) )
                    .block();
        } else {
            // create new file with link
            toCreate.checksum( checksum )
                    .fileId( UUID.randomUUID() )
                    .size( fileSize++ );
            createdRecord = fileQueries.createFileWithLink( toCreate.build() ).block();
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
                treePath, k -> new TreeSet<FileViewDto>() );
        NavigableSet<TreeDto> treeRecSet = treeDtoByChecksum.computeIfAbsent(
                checksum, k -> new TreeSet<TreeDto>() );
        fileRecSet.add( fileViewDto );
        treeRecSet.add( treeDto );
        FileDto fileDto = fileViewToFile.convert( fileViewDto );
        if (!fileDto.equals( fileDtoByChecksum.computeIfAbsent( checksum, k -> fileDto ) )) {
            throw new IllegalStateException( "Multiple inserts for same file checksum produced different file records" );
        }
    }

    record ObjectFileLink(TreeDto treeDto, FileViewDto fileViewDto) {
    }

}

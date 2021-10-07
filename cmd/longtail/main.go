package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

var numWorkerCount = runtime.NumCPU()

func normalizePath(path string) string {
	doubleForwardRemoved := strings.Replace(path, "//", "/", -1)
	doubleBackwardRemoved := strings.Replace(doubleForwardRemoved, "\\\\", "/", -1)
	backwardRemoved := strings.Replace(doubleBackwardRemoved, "\\", "/", -1)
	return backwardRemoved
}

func createBlockStoreForURI(uri string, optionalStoreIndexPath string, jobAPI longtaillib.Longtail_JobAPI, targetBlockSize uint32, maxChunksPerBlock uint32, accessType longtailstorelib.AccessType) (longtaillib.Longtail_BlockStoreAPI, error) {
	blobStoreURL, err := url.Parse(uri)
	if err == nil {
		switch blobStoreURL.Scheme {
		case "gs":
			gcsBlobStore, err := longtailstorelib.NewGCSBlobStore(blobStoreURL, false)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, err
			}
			gcsBlockStore, err := longtailstorelib.NewRemoteBlockStore(
				jobAPI,
				gcsBlobStore,
				optionalStoreIndexPath,
				numWorkerCount,
				accessType)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, err
			}
			return longtaillib.CreateBlockStoreAPI(gcsBlockStore), nil
		case "s3":
			s3BlobStore, err := longtailstorelib.NewS3BlobStore(blobStoreURL)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, err
			}
			s3BlockStore, err := longtailstorelib.NewRemoteBlockStore(
				jobAPI,
				s3BlobStore,
				optionalStoreIndexPath,
				numWorkerCount,
				accessType)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, err
			}
			return longtaillib.CreateBlockStoreAPI(s3BlockStore), nil
		case "abfs":
			return longtaillib.Longtail_BlockStoreAPI{}, fmt.Errorf("azure Gen1 storage not yet implemented")
		case "abfss":
			return longtaillib.Longtail_BlockStoreAPI{}, fmt.Errorf("azure Gen2 storage not yet implemented")
		case "file":
			return longtaillib.CreateFSBlockStore(jobAPI, longtaillib.CreateFSStorageAPI(), blobStoreURL.Path[1:]), nil
		}
	}
	return longtaillib.CreateFSBlockStore(jobAPI, longtaillib.CreateFSStorageAPI(), uri), nil
}

func getCompressionTypesForFiles(fileInfos longtaillib.Longtail_FileInfos, compressionType uint32) []uint32 {
	pathCount := fileInfos.GetFileCount()
	compressionTypes := make([]uint32, pathCount)
	for i := uint32(0); i < pathCount; i++ {
		compressionTypes[i] = compressionType
	}
	return compressionTypes
}

var (
	compressionTypeMap = map[string]uint32{
		"none":            noCompressionType,
		"brotli":          longtaillib.GetBrotliGenericDefaultCompressionType(),
		"brotli_min":      longtaillib.GetBrotliGenericMinCompressionType(),
		"brotli_max":      longtaillib.GetBrotliGenericMaxCompressionType(),
		"brotli_text":     longtaillib.GetBrotliTextDefaultCompressionType(),
		"brotli_text_min": longtaillib.GetBrotliTextMinCompressionType(),
		"brotli_text_max": longtaillib.GetBrotliTextMaxCompressionType(),
		"lz4":             longtaillib.GetLZ4DefaultCompressionType(),
		"zstd":            longtaillib.GetZStdDefaultCompressionType(),
		"zstd_min":        longtaillib.GetZStdMinCompressionType(),
		"zstd_max":        longtaillib.GetZStdMaxCompressionType(),
	}

	hashIdentifierMap = map[string]uint32{
		"meow":   longtaillib.GetMeowHashIdentifier(),
		"blake2": longtaillib.GetBlake2HashIdentifier(),
		"blake3": longtaillib.GetBlake3HashIdentifier(),
	}
)

const noCompressionType = uint32(0)

func getCompressionType(compressionAlgorithm string) (uint32, error) {
	if compressionType, exists := compressionTypeMap[compressionAlgorithm]; exists {
		return compressionType, nil
	}
	return 0, fmt.Errorf("unsupported compression algorithm: `%s`", compressionAlgorithm)
}

func getHashIdentifier(hashAlgorithm string) (uint32, error) {
	if identifier, exists := hashIdentifierMap[hashAlgorithm]; exists {
		return identifier, nil
	}
	return 0, fmt.Errorf("not a supported hash api: `%s`", hashAlgorithm)
}

type asyncFolderScanner struct {
	wg        sync.WaitGroup
	fileInfos longtaillib.Longtail_FileInfos
	elapsed   time.Duration
	err       error
}

func (scanner *asyncFolderScanner) scan(
	sourceFolderPath string,
	pathFilter longtaillib.Longtail_PathFilterAPI,
	fs longtaillib.Longtail_StorageAPI) {

	scanner.wg.Add(1)
	go func() {
		startTime := time.Now()
		fileInfos, errno := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			normalizePath(sourceFolderPath))
		if errno != 0 {
			scanner.err = errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtaillib.GetFilesRecursively(%s) failed", sourceFolderPath)
		}
		scanner.fileInfos = fileInfos
		scanner.elapsed = time.Since(startTime)
		scanner.wg.Done()
	}()
}

func (scanner *asyncFolderScanner) get() (longtaillib.Longtail_FileInfos, time.Duration, error) {
	scanner.wg.Wait()
	return scanner.fileInfos, scanner.elapsed, scanner.err
}

func getFolderIndex(
	sourceFolderPath string,
	sourceIndexPath string,
	targetChunkSize uint32,
	compressionType uint32,
	hashIdentifier uint32,
	pathFilter longtaillib.Longtail_PathFilterAPI,
	fs longtaillib.Longtail_StorageAPI,
	jobs longtaillib.Longtail_JobAPI,
	hashRegistry longtaillib.Longtail_HashRegistryAPI,
	scanner *asyncFolderScanner) (longtaillib.Longtail_VersionIndex, longtaillib.Longtail_HashAPI, time.Duration, error) {
	if sourceIndexPath == "" {
		fileInfos, scanTime, err := scanner.get()
		if err != nil {
			return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, scanTime, err
		}
		defer fileInfos.Dispose()

		startTime := time.Now()

		compressionTypes := getCompressionTypesForFiles(fileInfos, compressionType)

		hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
		if errno != 0 {
			return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, scanTime + time.Since(startTime), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "hashRegistry.GetHashAPI(%d) failed", hashIdentifier)
		}

		chunker := longtaillib.CreateHPCDCChunkerAPI()
		defer chunker.Dispose()

		createVersionIndexProgress := longtailutils.CreateProgress("Indexing version")
		defer createVersionIndexProgress.Dispose()
		vindex, errno := longtaillib.CreateVersionIndex(
			fs,
			hash,
			chunker,
			jobs,
			&createVersionIndexProgress,
			normalizePath(sourceFolderPath),
			fileInfos,
			compressionTypes,
			targetChunkSize)
		if errno != 0 {
			return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, scanTime + time.Since(startTime), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtaillib.CreateVersionIndex(%s)", sourceFolderPath)
		}

		return vindex, hash, scanTime + time.Since(startTime), nil
	}
	startTime := time.Now()

	vbuffer, err := longtailstorelib.ReadFromURI(sourceIndexPath)
	if err != nil {
		return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, time.Since(startTime), err
	}
	var errno int
	vindex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, time.Since(startTime), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtaillib.ReadVersionIndexFromBuffer(%s) failed", sourceIndexPath)
	}

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, time.Since(startTime), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "hashRegistry.GetHashAPI(%d) failed", hashIdentifier)
	}

	return vindex, hash, time.Since(startTime), nil
}

type asyncVersionIndexReader struct {
	wg           sync.WaitGroup
	versionIndex longtaillib.Longtail_VersionIndex
	hashAPI      longtaillib.Longtail_HashAPI
	elapsedTime  time.Duration
	err          error
}

func (indexReader *asyncVersionIndexReader) read(
	sourceFolderPath string,
	sourceIndexPath string,
	targetChunkSize uint32,
	compressionType uint32,
	hashIdentifier uint32,
	pathFilter longtaillib.Longtail_PathFilterAPI,
	fs longtaillib.Longtail_StorageAPI,
	jobs longtaillib.Longtail_JobAPI,
	hashRegistry longtaillib.Longtail_HashRegistryAPI,
	scanner *asyncFolderScanner) {
	indexReader.wg.Add(1)
	go func() {
		indexReader.versionIndex, indexReader.hashAPI, indexReader.elapsedTime, indexReader.err = getFolderIndex(
			sourceFolderPath,
			sourceIndexPath,
			targetChunkSize,
			compressionType,
			hashIdentifier,
			pathFilter,
			fs,
			jobs,
			hashRegistry,
			scanner)
		indexReader.wg.Done()
	}()
}

func (indexReader *asyncVersionIndexReader) get() (longtaillib.Longtail_VersionIndex, longtaillib.Longtail_HashAPI, time.Duration, error) {
	indexReader.wg.Wait()
	return indexReader.versionIndex, indexReader.hashAPI, indexReader.elapsedTime, indexReader.err
}

func upSyncVersion(
	blobStoreURI string,
	sourceFolderPath string,
	sourceIndexPath string,
	targetFilePath string,
	targetChunkSize uint32,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	compressionAlgorithm string,
	hashAlgorithm string,
	includeFilterRegEx string,
	excludeFilterRegEx string,
	minBlockUsagePercent uint32,
	versionLocalStoreIndexPath string,
	getConfigPath string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()
	pathFilter, err := longtailutils.MakeRegexPathFilter(includeFilterRegEx, excludeFilterRegEx)
	if err != nil {
		return storeStats, timeStats, err
	}

	fs := longtaillib.CreateFSStorageAPI()
	defer fs.Dispose()

	sourceFolderScanner := asyncFolderScanner{}
	if sourceIndexPath == "" {
		sourceFolderScanner.scan(sourceFolderPath, pathFilter, fs)
	}

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()
	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	compressionType, err := getCompressionType(compressionAlgorithm)
	if err != nil {
		return storeStats, timeStats, err
	}
	hashIdentifier, err := getHashIdentifier(hashAlgorithm)
	if err != nil {
		return storeStats, timeStats, err
	}

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	sourceIndexReader := asyncVersionIndexReader{}
	sourceIndexReader.read(sourceFolderPath,
		sourceIndexPath,
		targetChunkSize,
		compressionType,
		hashIdentifier,
		pathFilter,
		fs,
		jobs,
		hashRegistry,
		&sourceFolderScanner)

	remoteStore, err := createBlockStoreForURI(blobStoreURI, "", jobs, targetBlockSize, maxChunksPerBlock, longtailstorelib.ReadWrite)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer remoteStore.Dispose()

	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()

	indexStore := longtaillib.CreateCompressBlockStore(remoteStore, creg)
	defer indexStore.Dispose()

	vindex, hash, readSourceIndexTime, err := sourceIndexReader.get()
	if err != nil {
		return storeStats, timeStats, err
	}
	defer vindex.Dispose()
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceIndexTime})

	getMissingContentStartTime := time.Now()
	existingRemoteStoreIndex, errno := longtailutils.GetExistingStoreIndexSync(indexStore, vindex.GetChunkHashes(), minBlockUsagePercent)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtailutils.GetExistingStoreIndexSync(%s) failed", blobStoreURI)
	}
	defer existingRemoteStoreIndex.Dispose()

	versionMissingStoreIndex, errno := longtaillib.CreateMissingContent(
		hash,
		existingRemoteStoreIndex,
		vindex,
		targetBlockSize,
		maxChunksPerBlock)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtaillib.CreateMissingContent(%s) failed", sourceFolderPath)
	}
	defer versionMissingStoreIndex.Dispose()

	getMissingContentTime := time.Since(getMissingContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get content index", getMissingContentTime})

	writeContentStartTime := time.Now()
	if versionMissingStoreIndex.GetBlockCount() > 0 {
		writeContentProgress := longtailutils.CreateProgress("Writing content blocks")
		defer writeContentProgress.Dispose()

		errno = longtaillib.WriteContent(
			fs,
			indexStore,
			jobs,
			&writeContentProgress,
			versionMissingStoreIndex,
			vindex,
			normalizePath(sourceFolderPath))
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtaillib.WriteContent(%s) failed", sourceFolderPath)
		}
	}
	writeContentTime := time.Since(writeContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Write version content", writeContentTime})

	flushStartTime := time.Now()

	stores := []longtaillib.Longtail_BlockStoreAPI{
		indexStore,
		remoteStore,
	}
	errno = longtailutils.FlushStoresSync(stores)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtailutils.FlushStoresSync: Failed for `%v`", stores)
	}

	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Flush", flushTime})

	indexStoreStats, errno := indexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Compress", indexStoreStats})
	}
	remoteStoreStats, errno := remoteStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Remote", remoteStoreStats})
	}

	writeVersionIndexStartTime := time.Now()
	vbuffer, errno := longtaillib.WriteVersionIndexToBuffer(vindex)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtaillib.WriteVersionIndexToBuffer() failed")
	}

	err = longtailstorelib.WriteToURI(targetFilePath, vbuffer)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, "upSyncVersion: longtaillib.longtailstorelib.WriteToURL() failed")
	}
	writeVersionIndexTime := time.Since(writeVersionIndexStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Write version index", writeVersionIndexTime})

	if versionLocalStoreIndexPath != "" {
		writeVersionLocalStoreIndexStartTime := time.Now()
		versionLocalStoreIndex, errno := longtaillib.MergeStoreIndex(existingRemoteStoreIndex, versionMissingStoreIndex)
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "upSyncVersion: longtaillib.MergeStoreIndex() failed")
		}
		defer versionLocalStoreIndex.Dispose()
		versionLocalStoreIndexBuffer, errno := longtaillib.WriteStoreIndexToBuffer(versionLocalStoreIndex)
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "upSyncVersion: longtaillib.WriteStoreIndexToBuffer() failed")
		}
		err = longtailstorelib.WriteToURI(versionLocalStoreIndexPath, versionLocalStoreIndexBuffer)
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, "upSyncVersion: longtailstorelib.WriteToURL() failed")
		}
		writeVersionLocalStoreIndexTime := time.Since(writeVersionLocalStoreIndexStartTime)
		timeStats = append(timeStats, longtailutils.TimeStat{"Write version store index", writeVersionLocalStoreIndexTime})
	}

	if getConfigPath != "" {
		writeGetConfigStartTime := time.Now()

		v := viper.New()
		v.SetConfigType("json")
		v.Set("storage-uri", blobStoreURI)
		v.Set("source-path", targetFilePath)
		if versionLocalStoreIndexPath != "" {
			v.Set("version-local-store-index-path", versionLocalStoreIndexPath)
		}
		tmpFile, err := ioutil.TempFile(os.TempDir(), "longtail-")
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, "upSyncVersion: ioutil.TempFile() failed")
		}
		tmpFilePath := tmpFile.Name()
		tmpFile.Close()
		fmt.Printf("tmp file: %s", tmpFilePath)
		err = v.WriteConfigAs(tmpFilePath)
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, "upSyncVersion: v.WriteConfigAs() failed")
		}

		bytes, err := ioutil.ReadFile(tmpFilePath)
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, "upSyncVersion: ioutil.ReadFile(%s) failed", tmpFilePath)
		}
		os.Remove(tmpFilePath)

		err = longtailstorelib.WriteToURI(getConfigPath, bytes)
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, "upSyncVersion: longtailstorelib.WriteToURI(%s) failed", getConfigPath)
		}

		writeGetConfigTime := time.Since(writeGetConfigStartTime)
		timeStats = append(timeStats, longtailutils.TimeStat{"Write get config", writeGetConfigTime})
	}

	return storeStats, timeStats, nil
}

func getVersion(
	getConfigPath string,
	targetFolderPath string,
	targetIndexPath string,
	localCachePath string,
	retainPermissions bool,
	validate bool,
	includeFilterRegEx string,
	excludeFilterRegEx string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	readGetConfigStartTime := time.Now()

	vbuffer, err := longtailstorelib.ReadFromURI(getConfigPath)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, "getVersion: longtailstorelib.ReadFromURI() failed")
	}

	v := viper.New()
	v.SetConfigType("json")
	err = v.ReadConfig(bytes.NewBuffer(vbuffer))
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, "getVersion: v.ReadConfig() failed")
	}

	blobStoreURI := v.GetString("storage-uri")
	if blobStoreURI == "" {
		return storeStats, timeStats, fmt.Errorf("getVersion: missing storage-uri in get-config")
	}
	sourceFilePath := v.GetString("source-path")
	if sourceFilePath == "" {
		return storeStats, timeStats, fmt.Errorf("getVersion: missing source-path in get-config")
	}
	var versionLocalStoreIndexPath string
	if v.IsSet("version-local-store-index-path") {
		versionLocalStoreIndexPath = v.GetString("version-local-store-index-path")
	}

	readGetConfigTime := time.Since(readGetConfigStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read get config", readGetConfigTime})

	downSyncStoreStats, downSyncTimeStats, err := downSyncVersion(
		blobStoreURI,
		sourceFilePath,
		targetFolderPath,
		targetIndexPath,
		localCachePath,
		retainPermissions,
		validate,
		versionLocalStoreIndexPath,
		includeFilterRegEx,
		excludeFilterRegEx)

	storeStats = append(storeStats, downSyncStoreStats...)
	timeStats = append(timeStats, downSyncTimeStats...)

	return storeStats, timeStats, err
}

func downSyncVersion(
	blobStoreURI string,
	sourceFilePath string,
	targetFolderPath string,
	targetIndexPath string,
	localCachePath string,
	retainPermissions bool,
	validate bool,
	versionLocalStoreIndexPath string,
	includeFilterRegEx string,
	excludeFilterRegEx string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	pathFilter, err := longtailutils.MakeRegexPathFilter(includeFilterRegEx, excludeFilterRegEx)
	if err != nil {
		return storeStats, timeStats, err
	}

	resolvedTargetFolderPath := ""
	if targetFolderPath == "" {
		urlSplit := strings.Split(normalizePath(sourceFilePath), "/")
		sourceName := urlSplit[len(urlSplit)-1]
		sourceNameSplit := strings.Split(sourceName, ".")
		resolvedTargetFolderPath = sourceNameSplit[0]
		if resolvedTargetFolderPath == "" {
			return storeStats, timeStats, fmt.Errorf("downSyncVersion: unable to resolve target path using `%s` as base", sourceFilePath)
		}
	} else {
		resolvedTargetFolderPath = targetFolderPath
	}

	fs := longtaillib.CreateFSStorageAPI()
	defer fs.Dispose()

	targetFolderScanner := asyncFolderScanner{}
	if targetIndexPath == "" {
		targetFolderScanner.scan(resolvedTargetFolderPath, pathFilter, fs)
	}

	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	readSourceStartTime := time.Now()

	vbuffer, err := longtailstorelib.ReadFromURI(sourceFilePath)
	if err != nil {
		return storeStats, timeStats, err
	}
	sourceVersionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer sourceVersionIndex.Dispose()

	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	hashIdentifier := sourceVersionIndex.GetHashIdentifier()
	targetChunkSize := sourceVersionIndex.GetTargetChunkSize()

	targetIndexReader := asyncVersionIndexReader{}
	targetIndexReader.read(resolvedTargetFolderPath,
		targetIndexPath,
		targetChunkSize,
		noCompressionType,
		hashIdentifier,
		pathFilter,
		fs,
		jobs,
		hashRegistry,
		&targetFolderScanner)

	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()

	localFS := longtaillib.CreateFSStorageAPI()
	defer localFS.Dispose()

	// MaxBlockSize and MaxChunksPerBlock are just temporary values until we get the remote index settings
	remoteIndexStore, err := createBlockStoreForURI(blobStoreURI, versionLocalStoreIndexPath, jobs, 8388608, 1024, longtailstorelib.ReadOnly)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer remoteIndexStore.Dispose()

	var localIndexStore longtaillib.Longtail_BlockStoreAPI
	var cacheBlockStore longtaillib.Longtail_BlockStoreAPI
	var compressBlockStore longtaillib.Longtail_BlockStoreAPI

	if localCachePath == "" {
		compressBlockStore = longtaillib.CreateCompressBlockStore(remoteIndexStore, creg)
	} else {
		localIndexStore = longtaillib.CreateFSBlockStore(jobs, localFS, normalizePath(localCachePath))

		cacheBlockStore = longtaillib.CreateCacheBlockStore(jobs, localIndexStore, remoteIndexStore)

		compressBlockStore = longtaillib.CreateCompressBlockStore(cacheBlockStore, creg)
	}

	defer cacheBlockStore.Dispose()
	defer localIndexStore.Dispose()
	defer compressBlockStore.Dispose()

	lruBlockStore := longtaillib.CreateLRUBlockStoreAPI(compressBlockStore, 32)
	defer lruBlockStore.Dispose()
	indexStore := longtaillib.CreateShareBlockStore(lruBlockStore)
	defer indexStore.Dispose()

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.GetHashAPI() failed")
	}

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	targetVersionIndex, hash, readTargetIndexTime, err := targetIndexReader.get()
	if err != nil {
		return storeStats, timeStats, err
	}
	defer targetVersionIndex.Dispose()
	timeStats = append(timeStats, longtailutils.TimeStat{"Read target index", readTargetIndexTime})

	getExistingContentStartTime := time.Now()
	versionDiff, errno := longtaillib.CreateVersionDiff(
		hash,
		targetVersionIndex,
		sourceVersionIndex)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.CreateVersionDiff() failed")
	}
	defer versionDiff.Dispose()

	chunkHashes, errno := longtaillib.GetRequiredChunkHashes(
		sourceVersionIndex,
		versionDiff)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.GetRequiredChunkHashes() failed")
	}

	retargettedVersionStoreIndex, errno := longtailutils.GetExistingStoreIndexSync(indexStore, chunkHashes, 0)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtailutils.GetExistingStoreIndexSync(indexStore, chunkHashes) failed")
	}
	defer retargettedVersionStoreIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get content index", getExistingContentTime})

	changeVersionStartTime := time.Now()
	changeVersionProgress := longtailutils.CreateProgress("Updating version")
	defer changeVersionProgress.Dispose()
	errno = longtaillib.ChangeVersion(
		indexStore,
		fs,
		hash,
		jobs,
		&changeVersionProgress,
		retargettedVersionStoreIndex,
		targetVersionIndex,
		sourceVersionIndex,
		versionDiff,
		normalizePath(resolvedTargetFolderPath),
		retainPermissions)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.ChangeVersion() failed")
	}

	changeVersionTime := time.Since(changeVersionStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Change version", changeVersionTime})

	flushStartTime := time.Now()

	stores := []longtaillib.Longtail_BlockStoreAPI{
		indexStore,
		lruBlockStore,
		compressBlockStore,
		cacheBlockStore,
		localIndexStore,
		remoteIndexStore,
	}
	errno = longtailutils.FlushStoresSync(stores)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtailutils.FlushStoresSync: Failed for `%v`", stores)
	}

	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Flush", flushTime})

	shareStoreStats, errno := indexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Share", shareStoreStats})
	}
	lruStoreStats, errno := lruBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"LRU", lruStoreStats})
	}
	compressStoreStats, errno := compressBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Compress", compressStoreStats})
	}
	cacheStoreStats, errno := cacheBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Cache", cacheStoreStats})
	}
	localStoreStats, errno := localIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Local", localStoreStats})
	}
	remoteStoreStats, errno := remoteIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Remote", remoteStoreStats})
	}

	if validate {
		validateStartTime := time.Now()
		validateFileInfos, errno := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			normalizePath(resolvedTargetFolderPath))
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.GetFilesRecursively() failed")
		}
		defer validateFileInfos.Dispose()

		chunker := longtaillib.CreateHPCDCChunkerAPI()
		defer chunker.Dispose()

		createVersionIndexProgress := longtailutils.CreateProgress("Validating version")
		defer createVersionIndexProgress.Dispose()
		validateVersionIndex, errno := longtaillib.CreateVersionIndex(
			fs,
			hash,
			chunker,
			jobs,
			&createVersionIndexProgress,
			normalizePath(resolvedTargetFolderPath),
			validateFileInfos,
			nil,
			targetChunkSize)
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.CreateVersionIndex() failed")
		}
		defer validateVersionIndex.Dispose()
		if validateVersionIndex.GetAssetCount() != sourceVersionIndex.GetAssetCount() {
			return storeStats, timeStats, fmt.Errorf("downSyncVersion: failed validation: asset count mismatch")
		}
		validateAssetSizes := validateVersionIndex.GetAssetSizes()
		validateAssetHashes := validateVersionIndex.GetAssetHashes()

		sourceAssetSizes := sourceVersionIndex.GetAssetSizes()
		sourceAssetHashes := sourceVersionIndex.GetAssetHashes()

		assetSizeLookup := map[string]uint64{}
		assetHashLookup := map[string]uint64{}
		assetPermissionLookup := map[string]uint16{}

		for i, s := range sourceAssetSizes {
			path := sourceVersionIndex.GetAssetPath(uint32(i))
			assetSizeLookup[path] = s
			assetHashLookup[path] = sourceAssetHashes[i]
			assetPermissionLookup[path] = sourceVersionIndex.GetAssetPermissions(uint32(i))
		}
		for i, validateSize := range validateAssetSizes {
			validatePath := validateVersionIndex.GetAssetPath(uint32(i))
			validateHash := validateAssetHashes[i]
			size, exists := assetSizeLookup[validatePath]
			hash := assetHashLookup[validatePath]
			if !exists {
				return storeStats, timeStats, fmt.Errorf("downSyncVersion: failed validation: invalid path %s", validatePath)
			}
			if size != validateSize {
				return storeStats, timeStats, fmt.Errorf("downSyncVersion: failed validation: asset %d size mismatch", i)
			}
			if hash != validateHash {
				return storeStats, timeStats, fmt.Errorf("downSyncVersion: failed validation: asset %d hash mismatch", i)
			}
			if retainPermissions {
				validatePermissions := validateVersionIndex.GetAssetPermissions(uint32(i))
				permissions := assetPermissionLookup[validatePath]
				if permissions != validatePermissions {
					return storeStats, timeStats, fmt.Errorf("downSyncVersion: failed validation: asset %d permission mismatch", i)
				}
			}
		}
		validateTime := time.Since(validateStartTime)
		timeStats = append(timeStats, longtailutils.TimeStat{"Validate", validateTime})
	}

	return storeStats, timeStats, nil
}

func hashIdentifierToString(hashIdentifier uint32) string {
	if hashIdentifier == longtaillib.GetBlake2HashIdentifier() {
		return "blake2"
	}
	if hashIdentifier == longtaillib.GetBlake3HashIdentifier() {
		return "blake3"
	}
	if hashIdentifier == longtaillib.GetMeowHashIdentifier() {
		return "meow"
	}
	return fmt.Sprintf("%d", hashIdentifier)
}

func validateVersion(
	blobStoreURI string,
	versionIndexPath string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	// MaxBlockSize and MaxChunksPerBlock are just temporary values until we get the remote index settings
	indexStore, err := createBlockStoreForURI(blobStoreURI, "", jobs, 8388608, 1024, longtailstorelib.ReadOnly)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer indexStore.Dispose()
	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	readSourceStartTime := time.Now()
	vbuffer, err := longtailstorelib.ReadFromURI(versionIndexPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	getExistingContentStartTime := time.Now()
	remoteStoreIndex, errno := longtailutils.GetExistingStoreIndexSync(indexStore, versionIndex.GetChunkHashes(), 0)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: longtailutils.GetExistingStoreIndexSync(indexStore, versionIndex.GetChunkHashes(): Failed for `%s` failed", blobStoreURI)
	}
	defer remoteStoreIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get content index", getExistingContentTime})

	validateStartTime := time.Now()
	errno = longtaillib.ValidateStore(remoteStoreIndex, versionIndex)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: longtaillib.ValidateContent() failed")
	}
	validateTime := time.Since(validateStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Validate", validateTime})

	return storeStats, timeStats, nil
}

func showVersionIndex(versionIndexPath string, compact bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	readSourceStartTime := time.Now()

	vbuffer, err := longtailstorelib.ReadFromURI(versionIndexPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	var smallestChunkSize uint32
	var largestChunkSize uint32
	var averageChunkSize uint32
	var totalAssetSize uint64
	var totalChunkSize uint64
	totalAssetSize = 0
	totalChunkSize = 0
	chunkSizes := versionIndex.GetChunkSizes()
	if len(chunkSizes) > 0 {
		smallestChunkSize = uint32(chunkSizes[0])
		largestChunkSize = uint32(chunkSizes[0])
	} else {
		smallestChunkSize = 0
		largestChunkSize = 0
	}
	for i := uint32(0); i < uint32(len(chunkSizes)); i++ {
		chunkSize := uint32(chunkSizes[i])
		if chunkSize < smallestChunkSize {
			smallestChunkSize = chunkSize
		}
		if chunkSize > largestChunkSize {
			largestChunkSize = chunkSize
		}
		totalChunkSize = totalChunkSize + uint64(chunkSize)
	}
	if len(chunkSizes) > 0 {
		averageChunkSize = uint32(totalChunkSize / uint64(len(chunkSizes)))
	} else {
		averageChunkSize = 0
	}
	assetSizes := versionIndex.GetAssetSizes()
	for i := uint32(0); i < uint32(len(assetSizes)); i++ {
		assetSize := uint64(assetSizes[i])
		totalAssetSize = totalAssetSize + uint64(assetSize)
	}

	if compact {
		fmt.Printf("%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
			versionIndexPath,
			versionIndex.GetVersion(),
			hashIdentifierToString(versionIndex.GetHashIdentifier()),
			versionIndex.GetTargetChunkSize(),
			versionIndex.GetAssetCount(),
			totalAssetSize,
			versionIndex.GetChunkCount(),
			totalChunkSize,
			averageChunkSize,
			smallestChunkSize,
			largestChunkSize)
	} else {
		fmt.Printf("Version:             %d\n", versionIndex.GetVersion())
		fmt.Printf("Hash Identifier:     %s\n", hashIdentifierToString(versionIndex.GetHashIdentifier()))
		fmt.Printf("Target Chunk Size:   %d\n", versionIndex.GetTargetChunkSize())
		fmt.Printf("Asset Count:         %d   (%s)\n", versionIndex.GetAssetCount(), longtailutils.ByteCountDecimal(uint64(versionIndex.GetAssetCount())))
		fmt.Printf("Asset Total Size:    %d   (%s)\n", totalAssetSize, longtailutils.ByteCountBinary(totalAssetSize))
		fmt.Printf("Chunk Count:         %d   (%s)\n", versionIndex.GetChunkCount(), longtailutils.ByteCountDecimal(uint64(versionIndex.GetChunkCount())))
		fmt.Printf("Chunk Total Size:    %d   (%s)\n", totalChunkSize, longtailutils.ByteCountBinary(totalChunkSize))
		fmt.Printf("Average Chunk Size:  %d   (%s)\n", averageChunkSize, longtailutils.ByteCountBinary(uint64(averageChunkSize)))
		fmt.Printf("Smallest Chunk Size: %d   (%s)\n", smallestChunkSize, longtailutils.ByteCountBinary(uint64(smallestChunkSize)))
		fmt.Printf("Largest Chunk Size:  %d   (%s)\n", largestChunkSize, longtailutils.ByteCountBinary(uint64(largestChunkSize)))
	}

	return storeStats, timeStats, nil
}

func showStoreIndex(storeIndexPath string, compact bool, details bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	readStoreIndexStartTime := time.Now()

	vbuffer, err := longtailstorelib.ReadFromURI(storeIndexPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	storeIndex, errno := longtaillib.ReadStoreIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "showStoreIndex: longtaillib.ReadStoreIndexFromBuffer() failed")
	}
	defer storeIndex.Dispose()
	readStoreIndexTime := time.Since(readStoreIndexStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read store index", readStoreIndexTime})

	storedChunksSizes := uint64(0)
	uniqueStoredChunksSizes := uint64(0)
	if details {
		getChunkSizesStartTime := time.Now()
		uniqueChunks := make(map[uint64]uint32)
		chunkHashes := storeIndex.GetChunkHashes()
		chunkSizes := storeIndex.GetChunkSizes()
		for i, chunkHash := range chunkHashes {
			uniqueChunks[chunkHash] = chunkSizes[i]
			storedChunksSizes += uint64(chunkSizes[i])
		}
		for _, size := range uniqueChunks {
			uniqueStoredChunksSizes += uint64(size)
		}
		getChunkSizesTime := time.Since(getChunkSizesStartTime)
		timeStats = append(timeStats, longtailutils.TimeStat{"Get chunk sizes", getChunkSizesTime})
	}

	if compact {
		fmt.Printf("%s\t%d\t%s\t%d\t%d",
			storeIndexPath,
			storeIndex.GetVersion(),
			hashIdentifierToString(storeIndex.GetHashIdentifier()),
			storeIndex.GetBlockCount(),
			storeIndex.GetChunkCount())
		if details {
			fmt.Printf("\t%d\t%d",
				storedChunksSizes,
				uniqueStoredChunksSizes)
		}
		fmt.Printf("\n")
	} else {
		fmt.Printf("Version:             %d\n", storeIndex.GetVersion())
		fmt.Printf("Hash Identifier:     %s\n", hashIdentifierToString(storeIndex.GetHashIdentifier()))
		fmt.Printf("Block Count:         %d   (%s)\n", storeIndex.GetBlockCount(), longtailutils.ByteCountDecimal(uint64(storeIndex.GetBlockCount())))
		fmt.Printf("Chunk Count:         %d   (%s)\n", storeIndex.GetChunkCount(), longtailutils.ByteCountDecimal(uint64(storeIndex.GetChunkCount())))
		if details {
			fmt.Printf("Data size:           %d   (%s)\n", storedChunksSizes, longtailutils.ByteCountBinary(storedChunksSizes))
			fmt.Printf("Unique Data size:    %d   (%s)\n", uniqueStoredChunksSizes, longtailutils.ByteCountBinary(uniqueStoredChunksSizes))
		}
	}

	return storeStats, timeStats, nil
}

func dumpVersionIndex(versionIndexPath string, showDetails bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	readSourceStartTime := time.Now()
	vbuffer, err := longtailstorelib.ReadFromURI(versionIndexPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	assetCount := versionIndex.GetAssetCount()

	var biggestAsset uint64
	biggestAsset = 0
	for i := uint32(0); i < assetCount; i++ {
		assetSize := versionIndex.GetAssetSize(i)
		if assetSize > biggestAsset {
			biggestAsset = assetSize
		}
	}

	sizePadding := len(fmt.Sprintf("%d", biggestAsset))

	for i := uint32(0); i < assetCount; i++ {
		path := versionIndex.GetAssetPath(i)
		if showDetails {
			isDir := strings.HasSuffix(path, "/")
			assetSize := versionIndex.GetAssetSize(i)
			permissions := versionIndex.GetAssetPermissions(i)
			detailsString := longtailutils.GetDetailsString(path, assetSize, permissions, isDir, sizePadding)
			fmt.Printf("%s\n", detailsString)
		} else {
			fmt.Printf("%s\n", path)
		}
	}

	return storeStats, timeStats, nil
}

func cpVersionIndex(
	blobStoreURI string,
	versionIndexPath string,
	localCachePath string,
	sourcePath string,
	targetPath string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()
	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()
	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	// MaxBlockSize and MaxChunksPerBlock are just temporary values until we get the remote index settings
	remoteIndexStore, err := createBlockStoreForURI(blobStoreURI, "", jobs, 8388608, 1024, longtailstorelib.ReadOnly)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer remoteIndexStore.Dispose()

	localFS := longtaillib.CreateFSStorageAPI()
	defer localFS.Dispose()

	var localIndexStore longtaillib.Longtail_BlockStoreAPI
	var cacheBlockStore longtaillib.Longtail_BlockStoreAPI
	var compressBlockStore longtaillib.Longtail_BlockStoreAPI

	if localCachePath == "" {
		compressBlockStore = longtaillib.CreateCompressBlockStore(remoteIndexStore, creg)
	} else {
		localIndexStore = longtaillib.CreateFSBlockStore(jobs, localFS, normalizePath(localCachePath))

		cacheBlockStore = longtaillib.CreateCacheBlockStore(jobs, localIndexStore, remoteIndexStore)

		compressBlockStore = longtaillib.CreateCompressBlockStore(cacheBlockStore, creg)
	}

	defer cacheBlockStore.Dispose()
	defer localIndexStore.Dispose()
	defer compressBlockStore.Dispose()

	lruBlockStore := longtaillib.CreateLRUBlockStoreAPI(compressBlockStore, 32)
	defer lruBlockStore.Dispose()
	indexStore := longtaillib.CreateShareBlockStore(lruBlockStore)
	defer indexStore.Dispose()

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	readSourceStartTime := time.Now()
	vbuffer, err := longtailstorelib.ReadFromURI(versionIndexPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	hashIdentifier := versionIndex.GetHashIdentifier()

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: hashRegistry.GetHashAPI() failed")
	}

	getExistingContentStartTime := time.Now()
	storeIndex, errno := longtailutils.GetExistingStoreIndexSync(indexStore, versionIndex.GetChunkHashes(), 0)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: longtailutils.GetExistingStoreIndexSync(indexStore, versionIndex.GetChunkHashes(): Failed for `%s` failed", blobStoreURI)
	}
	defer storeIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get store index", getExistingContentTime})

	createBlockStoreFSStartTime := time.Now()
	blockStoreFS := longtaillib.CreateBlockStoreStorageAPI(
		hash,
		jobs,
		indexStore,
		storeIndex,
		versionIndex)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: hashRegistry.CreateBlockStoreStorageAPI() failed")
	}
	defer blockStoreFS.Dispose()
	createBlockStoreFSTime := time.Since(createBlockStoreFSStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Create Blockstore FS", createBlockStoreFSTime})

	copyFileStartTime := time.Now()
	// Only support writing to regular file path for now
	outFile, err := os.Create(targetPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer outFile.Close()

	inFile, errno := blockStoreFS.OpenReadFile(sourcePath)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: hashRegistry.OpenReadFile() failed")
	}
	defer blockStoreFS.CloseFile(inFile)

	size, errno := blockStoreFS.GetSize(inFile)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: blockStoreFS.GetSize() failed")
	}

	offset := uint64(0)
	for offset < size {
		left := size - offset
		if left > 128*1024*1024 {
			left = 128 * 1024 * 1024
		}
		data, errno := blockStoreFS.Read(inFile, offset, left)
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: hashRegistry.Read() failed")
		}
		outFile.Write(data)
		offset += left
	}
	copyFileTime := time.Since(copyFileStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Copy file", copyFileTime})

	flushStartTime := time.Now()

	stores := []longtaillib.Longtail_BlockStoreAPI{
		indexStore,
		lruBlockStore,
		compressBlockStore,
		cacheBlockStore,
		localIndexStore,
		remoteIndexStore,
	}
	errno = longtailutils.FlushStoresSync(stores)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtailutils.FlushStoresSync: Failed for `%v`", stores)
	}

	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Flush", flushTime})

	shareStoreStats, errno := indexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Share", shareStoreStats})
	}
	lruStoreStats, errno := lruBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"LRU", lruStoreStats})
	}
	compressStoreStats, errno := compressBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Compress", compressStoreStats})
	}
	cacheStoreStats, errno := cacheBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Cache", cacheStoreStats})
	}
	localStoreStats, errno := localIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Local", localStoreStats})
	}
	remoteStoreStats, errno := remoteIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Remote", remoteStoreStats})
	}

	return storeStats, timeStats, nil
}

func initRemoteStore(
	blobStoreURI string,
	hashAlgorithm string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	remoteIndexStore, err := createBlockStoreForURI(blobStoreURI, "", jobs, 8388608, 1024, longtailstorelib.Init)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer remoteIndexStore.Dispose()
	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	getExistingContentStartTime := time.Now()
	retargetStoreIndex, errno := longtailutils.GetExistingStoreIndexSync(remoteIndexStore, []uint64{}, 0)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "initRemoteStore: longtailutils.GetExistingStoreIndexSync(indexStore, versionIndex.GetChunkHashes(): Failed for `%s` failed", blobStoreURI)
	}
	defer retargetStoreIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get store index", getExistingContentTime})

	flushStartTime := time.Now()

	f, errno := longtailutils.FlushStore(&remoteIndexStore)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtailutils.FlushStore: Failed for `%v`", blobStoreURI)
	}
	errno = f.Wait()
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtailutils.FlushStore: Failed for `%v`", blobStoreURI)
	}

	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Flush", flushTime})

	remoteStoreStats, errno := remoteIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Remote", remoteStoreStats})
	}

	return storeStats, timeStats, nil
}

func lsVersionIndex(
	versionIndexPath string,
	commandLSVersionDir string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()
	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	readSourceStartTime := time.Now()
	vbuffer, err := longtailstorelib.ReadFromURI(versionIndexPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "lsVersionIndex: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	setupStartTime := time.Now()
	hashIdentifier := versionIndex.GetHashIdentifier()

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "lsVersionIndex: hashRegistry.GetHashAPI() failed")
	}

	fakeBlockStoreFS := longtaillib.CreateInMemStorageAPI()
	defer fakeBlockStoreFS.Dispose()

	fakeBlockStore := longtaillib.CreateFSBlockStore(jobs, fakeBlockStoreFS, "store")
	defer fakeBlockStoreFS.Dispose()

	storeIndex, errno := longtaillib.CreateStoreIndex(
		hash,
		versionIndex,
		1024*1024*1024,
		1024)

	blockStoreFS := longtaillib.CreateBlockStoreStorageAPI(
		hash,
		jobs,
		fakeBlockStore,
		storeIndex,
		versionIndex)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "lsVersionIndex: hashRegistry.CreateBlockStoreStorageAPI() failed")
	}
	defer blockStoreFS.Dispose()

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	searchDir := ""
	if commandLSVersionDir != "." {
		searchDir = commandLSVersionDir
	}

	iterator, errno := blockStoreFS.StartFind(searchDir)
	if errno == longtaillib.ENOENT {
		return storeStats, timeStats, nil
	}
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "lsVersionIndex: hashRegistry.StartFind() failed")
	}
	defer blockStoreFS.CloseFind(iterator)
	for {
		properties, errno := blockStoreFS.GetEntryProperties(iterator)
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "lsVersionIndex: GetEntryProperties.GetEntryProperties() failed")
		}
		detailsString := longtailutils.GetDetailsString(properties.Name, properties.Size, properties.Permissions, properties.IsDir, 16)
		fmt.Printf("%s\n", detailsString)

		errno = blockStoreFS.FindNext(iterator)
		if errno == longtaillib.ENOENT {
			break
		}
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: GetEntryProperties.FindNext() failed")
		}
	}
	return storeStats, timeStats, nil
}

func stats(
	blobStoreURI string,
	versionIndexPath string,
	localCachePath string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	var indexStore longtaillib.Longtail_BlockStoreAPI

	remoteIndexStore, err := createBlockStoreForURI(blobStoreURI, "", jobs, 8388608, 1024, longtailstorelib.ReadOnly)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer remoteIndexStore.Dispose()

	var localFS longtaillib.Longtail_StorageAPI

	var localIndexStore longtaillib.Longtail_BlockStoreAPI
	var cacheBlockStore longtaillib.Longtail_BlockStoreAPI

	if localCachePath == "" {
		indexStore = remoteIndexStore
	} else {
		localFS = longtaillib.CreateFSStorageAPI()
		localIndexStore = longtaillib.CreateFSBlockStore(jobs, localFS, normalizePath(localCachePath))

		cacheBlockStore = longtaillib.CreateCacheBlockStore(jobs, localIndexStore, remoteIndexStore)

		indexStore = cacheBlockStore
	}

	defer cacheBlockStore.Dispose()
	defer localIndexStore.Dispose()
	defer localFS.Dispose()

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	readSourceStartTime := time.Now()
	vbuffer, err := longtailstorelib.ReadFromURI(versionIndexPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "stats: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	getExistingContentStartTime := time.Now()
	existingStoreIndex, errno := longtailutils.GetExistingStoreIndexSync(indexStore, versionIndex.GetChunkHashes(), 0)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "stats: longtailutils.GetExistingStoreIndexSync() failed")
	}
	defer existingStoreIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get store index", getExistingContentTime})

	blockLookup := make(map[uint64]uint64)

	blockChunkCount := uint32(0)

	fetchingBlocksStartTime := time.Now()

	progress := longtailutils.CreateProgress("Fetching blocks")
	defer progress.Dispose()

	blockHashes := existingStoreIndex.GetBlockHashes()
	maxBatchSize := int(numWorkerCount)
	for i := 0; i < len(blockHashes); {
		batchSize := len(blockHashes) - i
		if batchSize > maxBatchSize {
			batchSize = maxBatchSize
		}
		completions := make([]longtailutils.GetStoredBlockCompletionAPI, batchSize)
		for offset := 0; offset < batchSize; offset++ {
			completions[offset].Wg.Add(1)
			go func(startIndex int, offset int) {
				blockHash := blockHashes[startIndex+offset]
				indexStore.GetStoredBlock(blockHash, longtaillib.CreateAsyncGetStoredBlockAPI(&completions[offset]))
			}(i, offset)
		}

		for offset := 0; offset < batchSize; offset++ {
			completions[offset].Wg.Wait()
			if completions[offset].Err != 0 {
				return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "stats: remoteStoreIndex.GetStoredBlock() failed")
			}
			blockIndex := completions[offset].StoredBlock.GetBlockIndex()
			for _, chunkHash := range blockIndex.GetChunkHashes() {
				blockLookup[chunkHash] = blockHashes[i+offset]
			}
			blockChunkCount += uint32(len(blockIndex.GetChunkHashes()))
		}

		i += batchSize
		progress.OnProgress(uint32(len(blockHashes)), uint32(i))
	}

	fetchingBlocksTime := time.Since(fetchingBlocksStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Fetching blocks", fetchingBlocksTime})

	blockUsage := uint32(100)
	if blockChunkCount > 0 {
		blockUsage = uint32((100 * existingStoreIndex.GetChunkCount()) / blockChunkCount)
	}

	var assetFragmentCount uint64
	chunkHashes := versionIndex.GetChunkHashes()
	assetChunkCounts := versionIndex.GetAssetChunkCounts()
	assetChunkIndexStarts := versionIndex.GetAssetChunkIndexStarts()
	assetChunkIndexes := versionIndex.GetAssetChunkIndexes()
	for a := uint32(0); a < versionIndex.GetAssetCount(); a++ {
		uniqueBlockCount := uint64(0)
		chunkCount := assetChunkCounts[a]
		chunkIndexOffset := assetChunkIndexStarts[a]
		lastBlockIndex := ^uint64(0)
		for c := chunkIndexOffset; c < chunkIndexOffset+chunkCount; c++ {
			chunkIndex := assetChunkIndexes[c]
			chunkHash := chunkHashes[chunkIndex]
			blockIndex := blockLookup[chunkHash]
			if blockIndex != lastBlockIndex {
				uniqueBlockCount++
				lastBlockIndex = blockIndex
				assetFragmentCount++
			}
		}
	}
	assetFragmentation := uint32(0)
	if versionIndex.GetAssetCount() > 0 {
		assetFragmentation = uint32((100*(assetFragmentCount))/uint64(versionIndex.GetAssetCount()) - 100)
	}

	fmt.Printf("Block Usage:          %d%%\n", blockUsage)
	fmt.Printf("Asset Fragmentation:  %d%%\n", assetFragmentation)

	flushStartTime := time.Now()

	stores := []longtaillib.Longtail_BlockStoreAPI{
		cacheBlockStore,
		localIndexStore,
		remoteIndexStore,
	}
	errno = longtailutils.FlushStoresSync(stores)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtailutils.FlushStoresSync: Failed for `%v`", stores)
	}

	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Flush", flushTime})

	cacheStoreStats, errno := cacheBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Cache", cacheStoreStats})
	}
	localStoreStats, errno := localIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Local", localStoreStats})
	}
	remoteStoreStats, errno := remoteIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Remote", remoteStoreStats})
	}
	return storeStats, timeStats, nil
}

func createVersionStoreIndex(
	blobStoreURI string,
	sourceFilePath string,
	versionLocalStoreIndexPath string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	indexStore, err := createBlockStoreForURI(blobStoreURI, "", jobs, 8388608, 1024, longtailstorelib.ReadOnly)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer indexStore.Dispose()

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	readSourceStartTime := time.Now()
	vbuffer, err := longtailstorelib.ReadFromURI(sourceFilePath)
	if err != nil {
		return storeStats, timeStats, err
	}
	sourceVersionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer sourceVersionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	getExistingContentStartTime := time.Now()
	chunkHashes := sourceVersionIndex.GetChunkHashes()

	retargettedVersionStoreIndex, errno := longtailutils.GetExistingStoreIndexSync(indexStore, chunkHashes, 0)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtailutils.GetExistingStoreIndexSync(indexStore, chunkHashes) failed")
	}
	defer retargettedVersionStoreIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get content index", getExistingContentTime})

	writeVersionLocalStoreIndexStartTime := time.Now()
	versionLocalStoreIndexBuffer, errno := longtaillib.WriteStoreIndexToBuffer(retargettedVersionStoreIndex)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "upSyncVersion: longtaillib.WriteStoreIndexToBuffer() failed")
	}
	err = longtailstorelib.WriteToURI(versionLocalStoreIndexPath, versionLocalStoreIndexBuffer)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, "upSyncVersion: longtaillib.longtailstorelib.WriteToURL() failed")
	}
	writeVersionLocalStoreIndexTime := time.Since(writeVersionLocalStoreIndexStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Write version store index", writeVersionLocalStoreIndexTime})

	return storeStats, timeStats, nil
}

func validateOneVersion(
	targetStore longtaillib.Longtail_BlockStoreAPI,
	targetFilePath string,
	skipValidate bool) error {
	tbuffer, err := longtailstorelib.ReadFromURI(targetFilePath)
	if err != nil {
		return err
	}
	if skipValidate {
		fmt.Printf("Skipping `%s`\n", targetFilePath)
		return nil
	}
	fmt.Printf("Validating `%s`\n", targetFilePath)
	targetVersionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(tbuffer)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateOneVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer targetVersionIndex.Dispose()

	targetStoreIndex, errno := longtailutils.GetExistingStoreIndexSync(targetStore, targetVersionIndex.GetChunkHashes(), 0)
	defer targetStoreIndex.Dispose()
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateOneVersion: longtailutils.GetExistingStoreIndexSync() failed")
	}
	defer targetStoreIndex.Dispose()

	errno = longtaillib.ValidateStore(targetStoreIndex, targetVersionIndex)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateOneVersion: longtaillib.ValidateStore() failed")
	}
	return nil
}

func Clone(v longtaillib.Longtail_VersionIndex) longtaillib.Longtail_VersionIndex {
	if !v.IsValid() {
		return longtaillib.Longtail_VersionIndex{}
	}
	vbuffer, errno := longtaillib.WriteVersionIndexToBuffer(v)
	if errno != 0 {
		return longtaillib.Longtail_VersionIndex{}
	}
	copy, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return longtaillib.Longtail_VersionIndex{}
	}
	return copy
}

func cloneOneVersion(
	targetPath string,
	jobs longtaillib.Longtail_JobAPI,
	hashRegistry longtaillib.Longtail_HashRegistryAPI,
	fs longtaillib.Longtail_StorageAPI,
	pathFilter longtaillib.Longtail_PathFilterAPI,
	retainPermissions bool,
	createVersionLocalStoreIndex bool,
	skipValidate bool,
	minBlockUsagePercent uint32,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	sourceStore longtaillib.Longtail_BlockStoreAPI,
	targetStore longtaillib.Longtail_BlockStoreAPI,
	sourceRemoteIndexStore longtaillib.Longtail_BlockStoreAPI,
	targetRemoteStore longtaillib.Longtail_BlockStoreAPI,
	targetFilePath string,
	sourceFilePath string,
	sourceFileZipPath string,
	currentVersionIndex longtaillib.Longtail_VersionIndex) (longtaillib.Longtail_VersionIndex, error) {

	targetFolderScanner := asyncFolderScanner{}
	targetFolderScanner.scan(targetPath, pathFilter, fs)

	err := validateOneVersion(targetStore, targetFilePath, skipValidate)
	if err == nil {
		return Clone(currentVersionIndex), nil
	}

	fmt.Printf("`%s` -> `%s`\n", sourceFilePath, targetFilePath)

	vbuffer, err := longtailstorelib.ReadFromURI(sourceFilePath)
	if err != nil {
		fileInfos, _, _ := targetFolderScanner.get()
		fileInfos.Dispose()
		return Clone(currentVersionIndex), err
	}
	sourceVersionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		fileInfos, _, _ := targetFolderScanner.get()
		fileInfos.Dispose()
		return Clone(currentVersionIndex), err
	}

	hashIdentifier := sourceVersionIndex.GetHashIdentifier()
	targetChunkSize := sourceVersionIndex.GetTargetChunkSize()

	var hash longtaillib.Longtail_HashAPI
	var targetVersionIndex longtaillib.Longtail_VersionIndex

	if currentVersionIndex.IsValid() {
		hash, errno = hashRegistry.GetHashAPI(hashIdentifier)
		if errno != 0 {
			return Clone(currentVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "hashRegistry.GetHashAPI(%d) failed", hashIdentifier)
		}
		targetVersionIndex = Clone(currentVersionIndex)
	} else {
		targetIndexReader := asyncVersionIndexReader{}
		targetIndexReader.read(targetPath,
			"",
			targetChunkSize,
			noCompressionType,
			hashIdentifier,
			pathFilter,
			fs,
			jobs,
			hashRegistry,
			&targetFolderScanner)

		targetVersionIndex, hash, _, err = targetIndexReader.get()
		if err != nil {
			return Clone(currentVersionIndex), err
		}
	}
	defer targetVersionIndex.Dispose()

	versionDiff, errno := longtaillib.CreateVersionDiff(
		hash,
		targetVersionIndex,
		sourceVersionIndex)
	if errno != 0 {
		return Clone(currentVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.CreateVersionDiff() failed")
	}
	defer versionDiff.Dispose()

	chunkHashes, errno := longtaillib.GetRequiredChunkHashes(
		sourceVersionIndex,
		versionDiff)
	if errno != 0 {
		return Clone(currentVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.GetRequiredChunkHashes() failed")
	}

	existingStoreIndex, errno := longtailutils.GetExistingStoreIndexSync(sourceStore, chunkHashes, 0)
	if errno != 0 {
		return Clone(currentVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtailutils.GetExistingStoreIndexSync() failed")
	}
	defer existingStoreIndex.Dispose()

	changeVersionProgress := longtailutils.CreateProgress("Updating version")
	defer changeVersionProgress.Dispose()

	errno = longtaillib.ChangeVersion(
		sourceStore,
		fs,
		hash,
		jobs,
		&changeVersionProgress,
		existingStoreIndex,
		targetVersionIndex,
		sourceVersionIndex,
		versionDiff,
		normalizePath(targetPath),
		retainPermissions)

	var newVersionIndex longtaillib.Longtail_VersionIndex

	if errno == 0 {
		newVersionIndex = Clone(sourceVersionIndex)
	} else {
		if sourceFileZipPath == "" {
			fmt.Printf("Skipping `%s` - unable to download from longtail: %s\n", sourceFilePath, longtaillib.ErrnoToError(errno, longtaillib.ErrEIO).Error())
			return longtaillib.Longtail_VersionIndex{}, err
		}
		fmt.Printf("Falling back to reading ZIP source from `%s`\n", sourceFileZipPath)
		zipBytes, err := longtailstorelib.ReadFromURI(sourceFileZipPath)
		if err != nil {
			return longtaillib.Longtail_VersionIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtailstorelib.ReadFromURI() failed")
		}

		zipReader := bytes.NewReader(zipBytes)

		r, err := zip.NewReader(zipReader, int64(len(zipBytes)))
		if err != nil {
			return longtaillib.Longtail_VersionIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: zip.OpenReader()  failed")
		}
		os.RemoveAll(targetPath)
		os.MkdirAll(targetPath, 0755)
		// Closure to address file descriptors issue with all the deferred .Close() methods
		extractAndWriteFile := func(f *zip.File) error {
			rc, err := f.Open()
			if err != nil {
				return err
			}
			defer func() {
				if err := rc.Close(); err != nil {
					panic(err)
				}
			}()

			path := filepath.Join(targetPath, f.Name)
			fmt.Printf("Unzipping `%s`\n", path)

			// Check for ZipSlip (Directory traversal)
			if !strings.HasPrefix(path, filepath.Clean(targetPath)+string(os.PathSeparator)) {
				return fmt.Errorf("illegal file path: %s", path)
			}

			if f.FileInfo().IsDir() {
				os.MkdirAll(path, f.Mode())
			} else {
				os.MkdirAll(filepath.Dir(path), 0777)
				f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
				if err != nil {
					return err
				}
				defer func() {
					if err := f.Close(); err != nil {
						panic(err)
					}
				}()

				_, err = io.Copy(f, rc)
				if err != nil {
					return err
				}
			}
			return nil
		}

		for _, f := range r.File {
			err := extractAndWriteFile(f)
			if err != nil {
				return longtaillib.Longtail_VersionIndex{}, err
			}
		}

		fileInfos, errno := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			normalizePath(targetPath))
		if errno != 0 {
			return longtaillib.Longtail_VersionIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.GetFilesRecursively() failed")
		}
		defer fileInfos.Dispose()

		compressionTypes := getCompressionTypesForFiles(fileInfos, noCompressionType)

		hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
		if errno != 0 {
			return longtaillib.Longtail_VersionIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: hashRegistry.GetHashAPI() failed")
		}

		chunker := longtaillib.CreateHPCDCChunkerAPI()
		defer chunker.Dispose()

		createVersionIndexProgress := longtailutils.CreateProgress("Indexing version")
		defer createVersionIndexProgress.Dispose()
		// Make sure to create an index of what we actually have on disk after update
		newVersionIndex, errno = longtaillib.CreateVersionIndex(
			fs,
			hash,
			chunker,
			jobs,
			&createVersionIndexProgress,
			normalizePath(targetPath),
			fileInfos,
			compressionTypes,
			targetChunkSize)
		if errno != 0 {
			return longtaillib.Longtail_VersionIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.CreateVersionIndex() failed")
		}

		chunkHashes = newVersionIndex.GetChunkHashes()

		// Make sure to update version binary for what we actually have on disk
		vbuffer, errno = longtaillib.WriteVersionIndexToBuffer(newVersionIndex)
		if errno != 0 {
			newVersionIndex.Dispose()
			return longtaillib.Longtail_VersionIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.WriteVersionIndexToBuffer() failed")
		}
	}
	defer newVersionIndex.Dispose()

	newExistingStoreIndex, errno := longtailutils.GetExistingStoreIndexSync(targetStore, newVersionIndex.GetChunkHashes(), minBlockUsagePercent)
	if errno != 0 {
		return Clone(sourceVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtailutils.GetExistingStoreIndexSync() failed")
	}
	defer newExistingStoreIndex.Dispose()

	versionMissingStoreIndex, errno := longtaillib.CreateMissingContent(
		hash,
		newExistingStoreIndex,
		newVersionIndex,
		targetBlockSize,
		maxChunksPerBlock)
	if errno != 0 {
		return Clone(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: CreateMissingContent() failed")
	}
	defer versionMissingStoreIndex.Dispose()

	if versionMissingStoreIndex.GetBlockCount() > 0 {
		writeContentProgress := longtailutils.CreateProgress("Writing content blocks")

		errno = longtaillib.WriteContent(
			fs,
			targetStore,
			jobs,
			&writeContentProgress,
			versionMissingStoreIndex,
			newVersionIndex,
			normalizePath(targetPath))
		writeContentProgress.Dispose()
		if errno != 0 {
			return Clone(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.WriteContent() failed")
		}
	}

	stores := []longtaillib.Longtail_BlockStoreAPI{
		targetRemoteStore,
		sourceRemoteIndexStore,
	}
	f, errno := longtailutils.FlushStores(stores)
	if errno != 0 {
		return Clone(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtailutils.FlushStores: Failed for `%v`", stores)
	}

	err = longtailstorelib.WriteToURI(targetFilePath, vbuffer)
	if err != nil {
		return Clone(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtailstorelib.WriteToURI() failed")
	}

	if createVersionLocalStoreIndex {
		versionLocalStoreIndex, errno := longtaillib.MergeStoreIndex(newExistingStoreIndex, versionMissingStoreIndex)
		if errno != 0 {
			return Clone(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.MergeStoreIndex() failed")
		}
		versionLocalStoreIndexBuffer, errno := longtaillib.WriteStoreIndexToBuffer(versionLocalStoreIndex)
		versionLocalStoreIndex.Dispose()
		if errno != 0 {
			return Clone(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.WriteStoreIndexToBuffer() failed")
		}
		versionLocalStoreIndexPath := strings.Replace(targetFilePath, ".lvi", ".lsi", -1) // TODO: This should use a file with path names instead of this rename hack!
		err = longtailstorelib.WriteToURI(versionLocalStoreIndexPath, versionLocalStoreIndexBuffer)
		if err != nil {
			return Clone(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtailstorelib.WriteToURI() failed")
		}
	}

	errno = f.Wait()
	if errno != 0 {
		return Clone(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtailutils.FlushStores: Failed for `%v`", stores)
	}

	return Clone(newVersionIndex), nil
}

func cloneStore(
	sourceStoreURI string,
	targetStoreURI string,
	localCachePath string,
	targetPath string,
	sourcePaths string,
	sourceZipPaths string,
	targetPaths string,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	retainPermissions bool,
	createVersionLocalStoreIndex bool,
	hashing string,
	compression string,
	minBlockUsagePercent uint32,
	skipValidate bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	fs := longtaillib.CreateFSStorageAPI()
	defer fs.Dispose()

	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()

	localFS := longtaillib.CreateFSStorageAPI()
	defer localFS.Dispose()

	sourceRemoteIndexStore, err := createBlockStoreForURI(sourceStoreURI, "", jobs, 8388608, 1024, longtailstorelib.ReadOnly)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer sourceRemoteIndexStore.Dispose()
	var localIndexStore longtaillib.Longtail_BlockStoreAPI
	var cacheBlockStore longtaillib.Longtail_BlockStoreAPI
	var sourceCompressBlockStore longtaillib.Longtail_BlockStoreAPI

	if len(localCachePath) > 0 {
		localIndexStore = longtaillib.CreateFSBlockStore(jobs, localFS, normalizePath(localCachePath))

		cacheBlockStore = longtaillib.CreateCacheBlockStore(jobs, localIndexStore, sourceRemoteIndexStore)

		sourceCompressBlockStore = longtaillib.CreateCompressBlockStore(cacheBlockStore, creg)
	} else {
		sourceCompressBlockStore = longtaillib.CreateCompressBlockStore(sourceRemoteIndexStore, creg)
	}

	defer localIndexStore.Dispose()
	defer cacheBlockStore.Dispose()
	defer sourceCompressBlockStore.Dispose()

	sourceLRUBlockStore := longtaillib.CreateLRUBlockStoreAPI(sourceCompressBlockStore, 32)
	defer sourceLRUBlockStore.Dispose()
	sourceStore := longtaillib.CreateShareBlockStore(sourceLRUBlockStore)
	defer sourceStore.Dispose()

	targetRemoteStore, err := createBlockStoreForURI(targetStoreURI, "", jobs, targetBlockSize, maxChunksPerBlock, longtailstorelib.ReadWrite)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer targetRemoteStore.Dispose()
	targetStore := longtaillib.CreateCompressBlockStore(targetRemoteStore, creg)
	defer targetStore.Dispose()

	sourcesFile, err := os.Open(sourcePaths)
	if err != nil {
		log.Fatal(err)
	}
	defer sourcesFile.Close()

	var sourcesZipScanner *bufio.Scanner
	if sourceZipPaths != "" {
		sourcesZipFile, err := os.Open(sourceZipPaths)
		if err != nil {
			log.Fatal(err)
		}
		sourcesZipScanner = bufio.NewScanner(sourcesZipFile)
		defer sourcesZipFile.Close()
	}

	targetsFile, err := os.Open(targetPaths)
	if err != nil {
		log.Fatal(err)
	}
	defer targetsFile.Close()

	sourcesScanner := bufio.NewScanner(sourcesFile)
	targetsScanner := bufio.NewScanner(targetsFile)

	var pathFilter longtaillib.Longtail_PathFilterAPI
	var currentVersionIndex longtaillib.Longtail_VersionIndex
	defer currentVersionIndex.Dispose()

	for sourcesScanner.Scan() {
		if !targetsScanner.Scan() {
			break
		}
		sourceFileZipPath := ""
		if sourcesZipScanner != nil {
			if !sourcesZipScanner.Scan() {
				break
			}
			sourceFileZipPath = sourcesZipScanner.Text()
		}

		sourceFilePath := sourcesScanner.Text()
		targetFilePath := targetsScanner.Text()

		newCurrentVersionIndex, err := cloneOneVersion(
			targetPath,
			jobs,
			hashRegistry,
			fs,
			pathFilter,
			retainPermissions,
			createVersionLocalStoreIndex,
			skipValidate,
			minBlockUsagePercent,
			targetBlockSize,
			maxChunksPerBlock,
			sourceStore,
			targetStore,
			sourceRemoteIndexStore,
			targetRemoteStore,
			targetFilePath,
			sourceFilePath,
			sourceFileZipPath,
			currentVersionIndex)
		currentVersionIndex.Dispose()
		currentVersionIndex = newCurrentVersionIndex

		if err != nil {
			return storeStats, timeStats, err
		}
	}

	if err := sourcesScanner.Err(); err != nil {
		log.Fatal(err)
	}
	if err := sourcesZipScanner.Err(); err != nil {
		log.Fatal(err)
	}
	if err := targetsScanner.Err(); err != nil {
		log.Fatal(err)
	}

	return storeStats, timeStats, nil
}

func pruneStore(
	storageURI string,
	sourcePaths string,
	versionLocalStoreIndexesPath string,
	writeVersionLocalStoreIndex bool,
	dryRun bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {

	setupStartTime := time.Now()
	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	storeMode := longtailstorelib.ReadOnly
	if !dryRun {
		storeMode = longtailstorelib.ReadWrite
	}

	remoteStore, err := createBlockStoreForURI(storageURI, "", jobs, 8388608, 1024, storeMode)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer remoteStore.Dispose()

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	sourceFilePathsStartTime := time.Now()

	sourcesFile, err := os.Open(sourcePaths)
	if err != nil {
		log.Fatal(err)
	}
	defer sourcesFile.Close()

	sourceFilePaths := make([]string, 0)
	sourcesScanner := bufio.NewScanner(sourcesFile)
	for sourcesScanner.Scan() {
		sourceFilePath := sourcesScanner.Text()
		sourceFilePaths = append(sourceFilePaths, sourceFilePath)
	}
	sourceFilePathsTime := time.Since(sourceFilePathsStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source file list", sourceFilePathsTime})

	versionLocalStoreIndexFilePaths := make([]string, 0)
	if strings.TrimSpace(versionLocalStoreIndexesPath) != "" {
		versionLocalStoreIndexesPathsStartTime := time.Now()
		versionLocalStoreIndexFile, err := os.Open(versionLocalStoreIndexesPath)
		if err != nil {
			log.Fatal(err)
		}
		defer versionLocalStoreIndexFile.Close()

		versionLocalStoreIndexesScanner := bufio.NewScanner(versionLocalStoreIndexFile)
		for versionLocalStoreIndexesScanner.Scan() {
			versionLocalStoreIndexFilePath := versionLocalStoreIndexesScanner.Text()
			versionLocalStoreIndexFilePaths = append(versionLocalStoreIndexFilePaths, versionLocalStoreIndexFilePath)
		}

		versionLocalStoreIndexesPathsTime := time.Since(versionLocalStoreIndexesPathsStartTime)
		timeStats = append(timeStats, longtailutils.TimeStat{"Read version local store index file list", versionLocalStoreIndexesPathsTime})

		if len(sourceFilePaths) != len(versionLocalStoreIndexFilePaths) {
			return storeStats, timeStats, fmt.Errorf("Number of files in `%s` does not match number of files in `%s`", sourcesFile, versionLocalStoreIndexesPath)
		}
	}

	usedBlocks := make(map[uint64]uint32)

	batchCount := numWorkerCount
	if batchCount > len(sourceFilePaths) {
		batchCount = len(sourceFilePaths)
	}
	batchStart := 0

	scanningForBlocksStartTime := time.Now()

	batchErrors := make(chan error, batchCount)
	progress := longtailutils.CreateProgress("Processing versions")
	defer progress.Dispose()
	for batchStart < len(sourceFilePaths) {
		batchLength := batchCount
		if batchStart+batchLength > len(sourceFilePaths) {
			batchLength = len(sourceFilePaths) - batchStart
		}
		blockHashesPerBatch := make([][]uint64, batchLength)
		for batchPos := 0; batchPos < batchLength; batchPos++ {
			i := batchStart + batchPos
			sourceFilePath := sourceFilePaths[i]
			versionLocalStoreIndexFilePath := ""
			if len(versionLocalStoreIndexFilePaths) > 0 {
				versionLocalStoreIndexFilePath = versionLocalStoreIndexFilePaths[i]
			}
			go func(batchPos int, sourceFilePath string, versionLocalStoreIndexFilePath string) {

				vbuffer, err := longtailstorelib.ReadFromURI(sourceFilePath)
				if err != nil {
					batchErrors <- err
					return
				}
				sourceVersionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
				if errno != 0 {
					batchErrors <- err
					return
				}

				var existingStoreIndex longtaillib.Longtail_StoreIndex
				if versionLocalStoreIndexFilePath != "" && !writeVersionLocalStoreIndex {
					sbuffer, err := longtailstorelib.ReadFromURI(versionLocalStoreIndexFilePath)
					if err == nil {
						existingStoreIndex, errno = longtaillib.ReadStoreIndexFromBuffer(sbuffer)
						if errno != 0 {
							batchErrors <- err
							return
						}
						errno = longtaillib.ValidateStore(existingStoreIndex, sourceVersionIndex)
						if errno != 0 {
							existingStoreIndex.Dispose()
						}
					}
				}
				if !existingStoreIndex.IsValid() {
					existingStoreIndex, errno = longtailutils.GetExistingStoreIndexSync(remoteStore, sourceVersionIndex.GetChunkHashes(), 0)
					if errno != 0 {
						sourceVersionIndex.Dispose()
						batchErrors <- err
						return
					}
					errno = longtaillib.ValidateStore(existingStoreIndex, sourceVersionIndex)
					if errno != 0 {
						existingStoreIndex.Dispose()
						sourceVersionIndex.Dispose()
						if dryRun {
							log.Printf("WARNING: Data is missing in store `%s` for version `%s`", storageURI, sourceFilePath)
							batchErrors <- nil
						} else {
							batchErrors <- errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "Data is missing in store `%s` for version `%s`", storageURI, sourceFilePath)
						}
						return
					}
				}

				blockHashesPerBatch[batchPos] = append(blockHashesPerBatch[batchPos], existingStoreIndex.GetBlockHashes()...)

				if versionLocalStoreIndexFilePath != "" && writeVersionLocalStoreIndex && !dryRun {
					sbuffer, errno := longtaillib.WriteStoreIndexToBuffer(existingStoreIndex)
					if errno != 0 {
						existingStoreIndex.Dispose()
						sourceVersionIndex.Dispose()
						batchErrors <- err
						return
					}
					err = longtailstorelib.WriteToURI(versionLocalStoreIndexFilePath, sbuffer)
					if err != nil {
						existingStoreIndex.Dispose()
						sourceVersionIndex.Dispose()
						batchErrors <- err
						return
					}
				}
				existingStoreIndex.Dispose()
				sourceVersionIndex.Dispose()

				batchErrors <- nil
			}(batchPos, sourceFilePath, versionLocalStoreIndexFilePath)
		}

		for batchPos := 0; batchPos < batchLength; batchPos++ {
			batchError := <-batchErrors
			if batchError != nil {
				return storeStats, timeStats, batchError
			}
			progress.OnProgress(uint32(len(sourceFilePaths)), uint32(batchStart+batchPos))
		}
		for batchPos := 0; batchPos < batchLength; batchPos++ {
			for _, h := range blockHashesPerBatch[batchPos] {
				usedBlocks[h] += 1
			}
		}

		batchStart += batchLength
	}
	progress.OnProgress(uint32(len(sourceFilePaths)), uint32(len(sourceFilePaths)))

	scanningForBlocksTime := time.Since(scanningForBlocksStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Scanning", scanningForBlocksTime})

	if dryRun {
		fmt.Printf("Prune would keep %d blocks\n", len(usedBlocks))
		return storeStats, timeStats, nil
	}

	pruneStartTime := time.Now()

	blockHashes := make([]uint64, len(usedBlocks))
	i := 0
	for k := range usedBlocks {
		blockHashes[i] = k
		i++
	}

	prunedBlockCount, errno := longtailutils.PruneBlocksSync(remoteStore, blockHashes)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "store.PruneBlocks() failed")
	}
	pruneTime := time.Since(pruneStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Prune", pruneTime})

	fmt.Printf("Pruned %d blocks\n", prunedBlockCount)

	flushStartTime := time.Now()

	errno = longtailutils.FlushStoreSync(&remoteStore)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtailutils.FlushStore: Failed for `%s`", remoteStore)
	}

	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Flush", flushTime})

	remoteStoreStats, errno := remoteStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Remote", remoteStoreStats})
	}

	return storeStats, timeStats, nil
}

type Context struct {
	StoreStats []longtailutils.StoreStat
	TimeStats  []longtailutils.TimeStat
}

type CompressionOption struct {
	Compression string `name:"compression-algorithm" help:"Compression algorithm [none brotli brotli_min brotli_max brotli_text brotli_text_min brotli_text_max lz4 zstd zstd_min zstd_max]" enum:"none,brotli,brotli_min,brotli_max,brotli_text,brotli_text_min,brotli_text_max,lz4,zstd,zstd_min,zstd_max" default:"zstd"`
}

type HashingOption struct {
	Hashing string `name:"hash-algorithm" help:"Hash algorithm [meow blake2 blake3]" enum:"meow,blake2,blake3" default:"blake3"`
}

type UpsyncIncludeRegExOption struct {
	IncludeFilterRegEx string `name:"include-filter-regex" help:"Optional include regex filter for assets in --source-path. Separate regexes with **"`
}

type DownsyncIncludeRegExOption struct {
	IncludeFilterRegEx string `name:"include-filter-regex" help:"Optional include regex filter for assets in --target-path on downsync. Separate regexes with **"`
}

type UpsyncExcludeRegExOption struct {
	ExcludeFilterRegEx string `name:"exclude-filter-regex" help:"Optional exclude regex filter for assets in --source-path on upsync. Separate regexes with **"`
}

type DownsyncExcludeRegExOption struct {
	ExcludeFilterRegEx string `name:"exclude-filter-regex" help:"Optional exclude regex filter for assets in --target-path on downsync. Separate regexes with **"`
}

type StorageURIOption struct {
	StorageURI string `name:"storage-uri" help"Storage URI (local file system, GCS and S3 bucket URI supported)" required:""`
}

type CachePathOption struct {
	CachePath string `name:"cache-path" help:"Location for cached blocks"`
}

type RetainPermissionsOption struct {
	RetainPermissions bool `name:"retain-permissions" negatable:"" help:"Set permission on file/directories from source" default:"true"`
}

type TargetPathOption struct {
	TargetPath string `name:"target-path" help:"Target folder path"`
}

type TargetIndexUriOption struct {
	TargetIndexPath string `name:"target-index-path" help:"Optional pre-computed index of target-path"`
}

type SourceUriOption struct {
	SourcePath string `name:"source-path" help:"Source file uri" required:""`
}

type ValidateTargetOption struct {
	Validate bool `name:"validate" help:"Validate target path once completed"`
}

type VersionLocalStoreIndexPathOption struct {
	VersionLocalStoreIndexPath string `name:"version-local-store-index-path" help:"Path to an optimized store index for this particular version. If the file can't be read it will fall back to the master store index"`
}

type VersionIndexPathOption struct {
	VersionIndexPath string `name:"version-index-path" help:"URI to version index (local file system, GCS and S3 bucket URI supported)"`
}

type CompactOption struct {
	Compact bool `name:"compact" help:"Show info in compact layout"`
}

type StoreIndexPathOption struct {
	StoreIndexPath string `name:"store-index-path" help:"URI to store index (local file system, GCS and S3 bucket URI supported)"`
}

type MinBlockUsagePercentOption struct {
	MinBlockUsagePercent uint32 `name:"min-block-usage-percent" help:"Minimum percent of block content than must match for it to be considered \"existing\". Default is zero = use all" default:"0"`
}

type TargetChunkSizeOption struct {
	TargetChunkSize uint32 `name:"target-chunk-size" help:"Target chunk size" default:"32768"`
}

type MaxChunksPerBlockOption struct {
	MaxChunksPerBlock uint32 `name:"max-chunks-per-block" help:"Max chunks per block" default:"1024"`
}

type TargetBlockSizeOption struct {
	TargetBlockSize uint32 `name:"target-block-size" help:"Target block size" default:"8388608"`
}

type UpsyncCmd struct {
	SourcePath                 string `name:"source-path" help:"Source folder path" required:""`
	SourceIndexPath            string `name:"source-index-path" help:"Optional pre-computed index of source-path"`
	TargetPath                 string `name:"target-path" help:"Target file uri" required:""`
	VersionLocalStoreIndexPath string `name:"version-local-store-index-path" help:"Target file uri for a store index optimized for this particular version"`
	GetConfigPath              string `name:"get-config-path" help:"Target file uri for json formatted get-config file"`
	TargetChunkSizeOption
	MaxChunksPerBlockOption
	TargetBlockSizeOption
	MinBlockUsagePercentOption
	StorageURIOption
	CompressionOption
	HashingOption
	UpsyncIncludeRegExOption
	UpsyncExcludeRegExOption
}

func (r *UpsyncCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := upSyncVersion(
		r.StorageURI,
		r.SourcePath,
		r.SourceIndexPath,
		r.TargetPath,
		r.TargetChunkSize,
		r.TargetBlockSize,
		r.MaxChunksPerBlock,
		r.Compression,
		r.Hashing,
		r.IncludeFilterRegEx,
		r.ExcludeFilterRegEx,
		r.MinBlockUsagePercent,
		r.VersionLocalStoreIndexPath,
		r.GetConfigPath)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}

type DownsyncCmd struct {
	StorageURIOption
	SourceUriOption
	TargetPathOption
	TargetIndexUriOption
	CachePathOption
	RetainPermissionsOption
	ValidateTargetOption
	VersionLocalStoreIndexPathOption
	DownsyncIncludeRegExOption
	DownsyncExcludeRegExOption
}

func (r *DownsyncCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := downSyncVersion(
		r.StorageURI,
		r.SourcePath,
		r.TargetPath,
		r.TargetIndexPath,
		r.CachePath,
		r.RetainPermissions,
		r.Validate,
		r.VersionLocalStoreIndexPath,
		r.IncludeFilterRegEx,
		r.ExcludeFilterRegEx)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}

type GetCmd struct {
	GetConfigURI string `name:"get-config-path" help:"File uri for json formatted get-config file" required:""`
	TargetPathOption
	TargetIndexUriOption
	ValidateTargetOption
	VersionLocalStoreIndexPathOption
	StorageURIOption
	CachePathOption
	RetainPermissionsOption
	DownsyncIncludeRegExOption
	DownsyncExcludeRegExOption
}

func (r *GetCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := getVersion(
		r.GetConfigURI,
		r.TargetPath,
		r.TargetIndexPath,
		r.CachePath,
		r.RetainPermissions,
		r.Validate,
		r.IncludeFilterRegEx,
		r.ExcludeFilterRegEx)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}

type ValidateCmd struct {
	StorageURIOption
	VersionIndexPathOption
}

func (r *ValidateCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := validateVersion(
		r.StorageURI,
		r.VersionIndexPath)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}

type PrintVersionIndexCmd struct {
	VersionIndexPathOption
	CompactOption
}

func (r *PrintVersionIndexCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := showVersionIndex(
		r.VersionIndexPath,
		r.Compact)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}

type PrintStoreIndexCmd struct {
	StoreIndexPathOption
	CompactOption
	Details bool `name:"details" help:"Show details about data sizes"`
}

func (r *PrintStoreIndexCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := showStoreIndex(
		r.StoreIndexPath,
		r.Compact,
		r.Details)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}

type DumpCmd struct {
	VersionIndexPathOption
	Details bool `name:"details" help:"Show details about assets"`
}

func (r *DumpCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := dumpVersionIndex(
		r.VersionIndexPath,
		r.Details)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}

type LsCmd struct {
	VersionIndexPathOption
	Path string `name:"path" arg:"" optional:"" help:"Path inside the version index to list"`
}

func (r *LsCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := lsVersionIndex(
		r.VersionIndexPath,
		r.Path)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}

type CpCmd struct {
	StorageURIOption
	VersionIndexPathOption
	CachePathOption
	SourcePath string `name:"source path" arg:"" help:"Source path inside the version index to copy"`
	TargetPath string `name:"target path" arg:"" help:"Target uri path"`
}

func (r *CpCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := cpVersionIndex(
		r.StorageURI,
		r.VersionIndexPath,
		r.CachePath,
		r.SourcePath,
		r.TargetPath)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}

type InitRemoteStoreCmd struct {
	StorageURIOption
	HashingOption
}

func (r *InitRemoteStoreCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := initRemoteStore(
		r.StorageURI,
		r.Hashing)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}

type StatsCmd struct {
	StorageURIOption
	VersionIndexPathOption
	CachePathOption
}

func (r *StatsCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := stats(
		r.StorageURI,
		r.VersionIndexPath,
		r.CachePath)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}

type CreateVersionStoreIndexCmd struct {
	StorageURIOption
	SourceUriOption
	VersionLocalStoreIndexPathOption
}

func (r *CreateVersionStoreIndexCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := createVersionStoreIndex(
		r.StorageURI,
		r.SourcePath,
		r.VersionLocalStoreIndexPath)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}

type CloneStoreCmd struct {
	SourceStorageURI             string `name:"source-storage-uri" help:"Source storage URI (local file system, GCS and S3 bucket URI supported)" required:""`
	TargetStorageURI             string `name:"target-storage-uri" help:"Target storage URI (local file system, GCS and S3 bucket URI supported)" required:""`
	TargetPath                   string `name:"target-path" help:"Target folder path" required:""`
	SourcePaths                  string `name:"source-paths" help:"File containing list of source longtail uris" required:""`
	SourceZipPaths               string `name:"source-zip-paths" help:"File containing list of source zip uris"`
	TargetPaths                  string `name:"target-paths" help:"File containing list of target longtail uris" required:""`
	CreateVersionLocalStoreIndex bool   `name:"create-version-local-store-index" help:"Generate an store index optimized for the versions"`
	SkipValidate                 bool   `name"skip-validate" help:"Skip validation of already cloned versions"`
	CachePathOption
	RetainPermissionsOption
	MaxChunksPerBlockOption
	TargetBlockSizeOption
	HashingOption
	CompressionOption
	MinBlockUsagePercentOption
}

func (r *CloneStoreCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := cloneStore(
		r.SourceStorageURI,
		r.TargetStorageURI,
		r.CachePath,
		r.TargetPath,
		r.SourcePaths,
		r.SourceZipPaths,
		r.TargetPaths,
		r.TargetBlockSize,
		r.MaxChunksPerBlock,
		r.RetainPermissions,
		r.CreateVersionLocalStoreIndex,
		r.Hashing,
		r.Compression,
		r.MinBlockUsagePercent,
		r.SkipValidate)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}

type PruneStoreCmd struct {
	StorageURIOption
	SourcePaths                 string `name:"source-paths" help:"File containing list of source longtail uris" required:""`
	VersionLocalStoreIndexPaths string `name:"version-local-store-index-paths" help:"File containing list of version local store index longtail uris"`
	DryRun                      bool   `name:"dry-run" help:"Don't prune, just show how many blocks would be kept if prune was run"`
	WriteVersionLocalStoreIndex bool   `name:"write-version-local-store-index" help:"Write a new version local store index for each version. This requires a valid version-local-store-index-paths input parameter"`
}

func (r *PruneStoreCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := pruneStore(
		r.StorageURI,
		r.SourcePaths,
		r.VersionLocalStoreIndexPaths,
		r.WriteVersionLocalStoreIndex,
		r.DryRun)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}

var cli struct {
	LogLevel                string                     `name:"log-level" help:"Log level [debug, info, warn, error]" enum:"debug, info, warn, error" default:"warn" `
	ShowStats               bool                       `name:"show-stats" help:"Output brief stats summary"`
	ShowStoreStats          bool                       `name:"show-store-stats" help:"Output detailed stats for block stores"`
	MemTrace                bool                       `name:"mem-trace" help:"Output summary memory statistics from longtail"`
	MemTraceDetailed        bool                       `name:"mem-trace-detailed" help:"Output detailed memory statistics from longtail"`
	MemTraceCSV             string                     `name:"mem-trace-csv" help:"Output path for detailed memory statistics from longtail in csv format"`
	WorkerCount             int                        `name:"worker-count" help:"Limit number of workers created, defaults to match number of logical CPUs (zero for default count)" default:"0"`
	Upsync                  UpsyncCmd                  `cmd:"" name:"upsync" help:"Upload a folder"`
	Downsync                DownsyncCmd                `cmd:"" name:"downsync" help:"Download a folder"`
	Get                     GetCmd                     `cmd:"" name:"get" help:"Download a folder using a get-config"`
	Validate                ValidateCmd                `cmd:"" name:"validate" help:"Validate a version index against a content store"`
	PrintVersionIndex       PrintVersionIndexCmd       `cmd:"" name:"printVersionIndex" help:"Print info about a version index"`
	PrintStoreIndex         PrintStoreIndexCmd         `cmd:"" name:"printStoreIndex" help:"Print info about a store index"`
	Dump                    DumpCmd                    `cmd:"" name:"dump" help:"Dump the asset paths inside a version index"`
	Ls                      LsCmd                      `cmd:"" name:"ls" help:"List the content of a path inside a version index"`
	Cp                      CpCmd                      `cmd:"" name:"cp" help:"Copies a file from inside a version index"`
	InitRemoteStore         InitRemoteStoreCmd         `cmd:"" name:"init" help:"Open/create a remote store and force rebuild the store index"`
	Stats                   StatsCmd                   `cmd:"" name:"stats" help:"Show fragmenation stats about a version index"`
	CreateVersionStoreIndex CreateVersionStoreIndexCmd `cmd:"" name:"createVersionStoreIndex" help:"Create a store index optimized for a version index"`
	CloneStore              CloneStoreCmd              `cmd:"" name:"cloneStore" help:"Clone all the data needed to cover a set of versions from one store into a new store"`
	PruneStore              PruneStoreCmd              `cmd:"" name:"pruneStore" help:"Prune blocks in a store which are not used by the files in the input list. CAUTION! Running uploads to a store that is being pruned may cause loss of the uploaded data"`
}

func main() {
	executionStartTime := time.Now()
	initStartTime := executionStartTime

	context := &Context{}

	defer func() {
		executionTime := time.Since(executionStartTime)
		context.TimeStats = append(context.TimeStats, longtailutils.TimeStat{"Execution", executionTime})

		if cli.ShowStoreStats {
			for _, s := range context.StoreStats {
				longtailutils.PrintStats(s.Name, s.Stats)
			}
		}

		if cli.ShowStats {
			maxLen := 0
			for _, s := range context.TimeStats {
				if len(s.Name) > maxLen {
					maxLen = len(s.Name)
				}
			}
			for _, s := range context.TimeStats {
				name := fmt.Sprintf("%s:", s.Name)
				log.Printf("%-*s %s", maxLen+1, name, s.Dur)
			}
		}
	}()

	ctx := kong.Parse(&cli)

	longtailLogLevel, err := longtailutils.ParseLevel(cli.LogLevel)
	if err != nil {
		log.Fatal(err)
	}

	longtaillib.SetLogger(&longtailutils.LoggerData{})
	defer longtaillib.SetLogger(nil)
	longtaillib.SetLogLevel(longtailLogLevel)

	longtaillib.SetAssert(&longtailutils.AssertData{})
	defer longtaillib.SetAssert(nil)

	if cli.WorkerCount != 0 {
		numWorkerCount = cli.WorkerCount
	}

	if cli.MemTrace || cli.MemTraceDetailed || cli.MemTraceCSV != "" {
		longtaillib.EnableMemtrace()
		defer func() {
			memTraceLogLevel := longtaillib.MemTraceSummary
			if cli.MemTraceDetailed {
				memTraceLogLevel = longtaillib.MemTraceDetailed
			}
			if cli.MemTraceCSV != "" {
				longtaillib.MemTraceDumpStats(cli.MemTraceCSV)
			}
			memTraceLog := longtaillib.GetMemTraceStats(memTraceLogLevel)
			memTraceLines := strings.Split(memTraceLog, "\n")
			for _, l := range memTraceLines {
				if l == "" {
					continue
				}
				log.Printf("[MEM] %s", l)
			}
			longtaillib.DisableMemtrace()
		}()
	}

	initTime := time.Since(initStartTime)

	err = ctx.Run(context)

	context.TimeStats = append([]longtailutils.TimeStat{{"Init", initTime}}, context.TimeStats...)

	if err != nil {
		log.Fatal(err)
	}
}

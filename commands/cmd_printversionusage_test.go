package commands

import (
	"os"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestPrintVersionUsage(t *testing.T) {
	testPath, _ := os.MkdirTemp("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")

	cmd, err := executeCommandLine("print-version-usage", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-index-path", fsBlobPathPrefix+"/index/v1.lvi")
	assert.NoError(t, err, cmd)
	cmd, err = executeCommandLine("print-version-usage", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-index-path", fsBlobPathPrefix+"/index/v2.lvi")
	assert.NoError(t, err, cmd)
	cmd, err = executeCommandLine("print-version-usage", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-index-path", fsBlobPathPrefix+"/index/v3.lvi")
	assert.NoError(t, err, cmd)

	cmd, err = executeCommandLine("print-version-usage", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-index-path", fsBlobPathPrefix+"/index/v1.lvi", "--cache-path", testPath+"/cache")
	assert.NoError(t, err, cmd)
	cmd, err = executeCommandLine("print-version-usage", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-index-path", fsBlobPathPrefix+"/index/v2.lvi", "--cache-path", testPath+"/cache")
	assert.NoError(t, err, cmd)
	cmd, err = executeCommandLine("print-version-usage", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-index-path", fsBlobPathPrefix+"/index/v3.lvi", "--cache-path", testPath+"/cache")
	assert.NoError(t, err, cmd)
}

// Copyright 2020 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunks

import (
	"encoding/binary"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/blastbao/prometheus/tsdb/chunkenc"
	"github.com/blastbao/prometheus/util/testutil"
)

func TestHeadReadWriter_WriteChunk_Chunk_IterateChunks(t *testing.T) {
	hrw, close := testHeadReadWriter(t)
	defer func() {
		testutil.Ok(t, hrw.Close())
		close()
	}()

	expectedBytes := []byte{}
	nextChunkOffset := uint64(HeadChunkFileHeaderSize)
	chkCRC32 := newCRC32()

	type expectedDataType struct {
		seriesRef, chunkRef uint64
		mint, maxt          int64
		chunk               chunkenc.Chunk
	}
	expectedData := []expectedDataType{}

	var buf [MaxHeadChunkMetaSize]byte
	totalChunks := 0
	var firstFileName string
	for hrw.curFileSequence < 3 || hrw.chkWriter.Buffered() == 0 {
		for i := 0; i < 100; i++ {
			seriesRef, chkRef, mint, maxt, chunk := createChunk(t, totalChunks, hrw)
			totalChunks++
			expectedData = append(expectedData, expectedDataType{
				seriesRef: seriesRef,
				mint:      mint,
				maxt:      maxt,
				chunkRef:  chkRef,
				chunk:     chunk,
			})

			if hrw.curFileSequence != 1 {
				// We are checking for bytes written only for the first file.
				continue
			}

			// Calculating expected bytes written on disk for first file.
			firstFileName = hrw.curFile.Name()
			testutil.Equals(t, chunkRef(1, nextChunkOffset), chkRef)

			bytesWritten := 0
			chkCRC32.Reset()

			binary.BigEndian.PutUint64(buf[bytesWritten:], seriesRef)
			bytesWritten += SeriesRefSize
			binary.BigEndian.PutUint64(buf[bytesWritten:], uint64(mint))
			bytesWritten += MintMaxtSize
			binary.BigEndian.PutUint64(buf[bytesWritten:], uint64(maxt))
			bytesWritten += MintMaxtSize
			buf[bytesWritten] = byte(chunk.Encoding())
			bytesWritten += ChunkEncodingSize
			n := binary.PutUvarint(buf[bytesWritten:], uint64(len(chunk.Bytes())))
			bytesWritten += n

			expectedBytes = append(expectedBytes, buf[:bytesWritten]...)
			_, err := chkCRC32.Write(buf[:bytesWritten])
			testutil.Ok(t, err)
			expectedBytes = append(expectedBytes, chunk.Bytes()...)
			_, err = chkCRC32.Write(chunk.Bytes())
			testutil.Ok(t, err)

			expectedBytes = append(expectedBytes, chkCRC32.Sum(nil)...)

			// += seriesRef, mint, maxt, encoding, chunk data len, chunk data, CRC.
			nextChunkOffset += SeriesRefSize + 2*MintMaxtSize + ChunkEncodingSize + uint64(n) + uint64(len(chunk.Bytes())) + CRCSize
		}
	}

	// Checking on-disk bytes for the first file.
	testutil.Assert(t, len(hrw.mmappedChunkFiles) == 3 && len(hrw.closers) == 3, "expected 3 mmapped files, got %d", len(hrw.mmappedChunkFiles))

	actualBytes, err := ioutil.ReadFile(firstFileName)
	testutil.Ok(t, err)

	// Check header of the segment file.
	testutil.Equals(t, MagicHeadChunks, int(binary.BigEndian.Uint32(actualBytes[0:MagicChunksSize])))
	testutil.Equals(t, chunksFormatV1, int(actualBytes[MagicChunksSize]))

	// Remaining chunk data.
	fileEnd := HeadChunkFileHeaderSize + len(expectedBytes)
	testutil.Equals(t, expectedBytes, actualBytes[HeadChunkFileHeaderSize:fileEnd])

	// Test for the next chunk header to be all 0s. That marks the end of the file.
	for _, b := range actualBytes[fileEnd : fileEnd+MaxHeadChunkMetaSize] {
		testutil.Equals(t, byte(0), b)
	}

	// Testing reading of chunks.
	for _, exp := range expectedData {
		actChunk, err := hrw.Chunk(exp.chunkRef)
		testutil.Ok(t, err)
		testutil.Equals(t, exp.chunk.Bytes(), actChunk.Bytes())
	}

	// Testing IterateAllChunks method.
	dir := hrw.dir.Name()
	testutil.Ok(t, hrw.Close())
	hrw, err = NewChunkDiskMapper(dir, chunkenc.NewPool())
	testutil.Ok(t, err)

	idx := 0
	err = hrw.IterateAllChunks(func(seriesRef, chunkRef uint64, mint, maxt int64) error {
		t.Helper()

		expData := expectedData[idx]
		testutil.Equals(t, expData.seriesRef, seriesRef)
		testutil.Equals(t, expData.chunkRef, chunkRef)
		testutil.Equals(t, expData.maxt, maxt)
		testutil.Equals(t, expData.maxt, maxt)

		actChunk, err := hrw.Chunk(expData.chunkRef)
		testutil.Ok(t, err)
		testutil.Equals(t, expData.chunk.Bytes(), actChunk.Bytes())

		idx++
		return nil
	})
	testutil.Ok(t, err)
	testutil.Equals(t, len(expectedData), idx)

}

func TestHeadReadWriter_Truncate(t *testing.T) {
	hrw, close := testHeadReadWriter(t)
	defer func() {
		testutil.Ok(t, hrw.Close())
		close()
	}()

	testutil.Assert(t, !hrw.fileMaxtSet, "")
	testutil.Ok(t, hrw.IterateAllChunks(func(_, _ uint64, _, _ int64) error { return nil }))
	testutil.Assert(t, hrw.fileMaxtSet, "")

	timeRange := 0
	fileTimeStep := 100
	totalFiles, after1stTruncation, after2ndTruncation := 7, 5, 3
	var timeToTruncate, timeToTruncateAfterRestart int64

	cutFile := func(i int) {
		testutil.Ok(t, hrw.cut(int64(timeRange)))

		mint := timeRange + 1                // Just after the the new file cut.
		maxt := timeRange + fileTimeStep - 1 // Just before the next file.

		// Write a chunks to set maxt for the segment.
		_, err := hrw.WriteChunk(1, int64(mint), int64(maxt), randomChunk(t))
		testutil.Ok(t, err)

		if i == totalFiles-after1stTruncation+1 {
			// Truncate the segment files before the 5th segment.
			timeToTruncate = int64(mint)
		} else if i == totalFiles-after2ndTruncation+1 {
			// Truncate the segment files before the 3rd segment after restart.
			timeToTruncateAfterRestart = int64(mint)
		}

		timeRange += fileTimeStep
	}

	// Cut segments.
	for i := 1; i <= totalFiles; i++ {
		cutFile(i)
	}

	// Verifying the the remaining files.
	verifyRemainingFiles := func(remainingFiles int) {
		t.Helper()

		files, err := ioutil.ReadDir(hrw.dir.Name())
		testutil.Ok(t, err)
		testutil.Equals(t, remainingFiles, len(files))
		testutil.Equals(t, remainingFiles, len(hrw.mmappedChunkFiles))
		testutil.Equals(t, remainingFiles, len(hrw.closers))

		for i := 1; i <= totalFiles; i++ {
			_, ok := hrw.mmappedChunkFiles[i]
			if i < totalFiles-remainingFiles+1 {
				testutil.Equals(t, false, ok)
			} else {
				testutil.Equals(t, true, ok)
			}
		}
	}

	// Verify the number of segments.
	verifyRemainingFiles(totalFiles)

	// Truncating files.
	testutil.Ok(t, hrw.Truncate(timeToTruncate))
	verifyRemainingFiles(after1stTruncation)

	dir := hrw.dir.Name()
	testutil.Ok(t, hrw.Close())

	// Restarted.
	var err error
	hrw, err = NewChunkDiskMapper(dir, chunkenc.NewPool())
	testutil.Ok(t, err)

	testutil.Assert(t, !hrw.fileMaxtSet, "")
	testutil.Ok(t, hrw.IterateAllChunks(func(_, _ uint64, _, _ int64) error { return nil }))
	testutil.Assert(t, hrw.fileMaxtSet, "")

	// Truncating files after restart.
	testutil.Ok(t, hrw.Truncate(timeToTruncateAfterRestart))
	verifyRemainingFiles(after2ndTruncation)

	// Add another file to have an active file.
	totalFiles++
	cutFile(totalFiles)
	// Truncating till current time should not delete the current active file.
	testutil.Ok(t, hrw.Truncate(time.Now().UnixNano()/1e6))
	verifyRemainingFiles(1)
}

func testHeadReadWriter(t *testing.T) (hrw *ChunkDiskMapper, close func()) {
	tmpdir, err := ioutil.TempDir("", "data")
	testutil.Ok(t, err)
	hrw, err = NewChunkDiskMapper(tmpdir, chunkenc.NewPool())
	testutil.Ok(t, err)
	return hrw, func() {
		testutil.Ok(t, os.RemoveAll(tmpdir))
	}
}

func randomChunk(t *testing.T) chunkenc.Chunk {
	chunk := chunkenc.NewXORChunk()
	len := rand.Int() % 120
	app, err := chunk.Appender()
	testutil.Ok(t, err)
	for i := 0; i < len; i++ {
		app.Append(rand.Int63(), rand.Float64())
	}
	return chunk
}

func createChunk(t *testing.T, idx int, hrw *ChunkDiskMapper) (seriesRef uint64, chunkRef uint64, mint, maxt int64, chunk chunkenc.Chunk) {
	var err error
	seriesRef = uint64(rand.Int63())
	mint = int64((idx)*1000 + 1)
	maxt = int64((idx + 1) * 1000)
	chunk = randomChunk(t)
	chunkRef, err = hrw.WriteChunk(seriesRef, mint, maxt, chunk)
	testutil.Ok(t, err)
	return
}

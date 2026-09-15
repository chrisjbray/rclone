package multipart

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// skipChunkWriter is a test ChunkWriter honouring SkipChunk.
type skipChunkWriter struct {
	mu      sync.Mutex
	written []int
	skip    map[int]bool
	closed  bool
	aborted bool
}

func (w *skipChunkWriter) WriteChunk(ctx context.Context, chunkNumber int, reader io.ReadSeeker) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.written = append(w.written, chunkNumber)
	n, err := io.Copy(io.Discard, reader)
	return n, err
}

func (w *skipChunkWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

func (w *skipChunkWriter) Abort(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.aborted = true
	return nil
}

func (w *skipChunkWriter) SkipChunk(chunkNumber int) bool {
	return w.skip[chunkNumber]
}

// skipChunkOpener implements fs.OpenChunkWriter for tests.
type skipChunkOpener struct {
	writer  *skipChunkWriter
	chunkSz int64
}

func (o *skipChunkOpener) OpenChunkWriter(ctx context.Context, remote string, src fs.ObjectInfo, options ...fs.OpenOption) (fs.ChunkWriterInfo, fs.ChunkWriter, error) {
	return fs.ChunkWriterInfo{ChunkSize: o.chunkSz, Concurrency: 2}, o.writer, nil
}

func TestUploadMultipartSkipsResumedChunks(t *testing.T) {
	ctx := context.Background()
	content := bytes.Repeat([]byte("x"), 100)
	src := object.NewStaticObjectInfo("file.bin", time.Now(), int64(len(content)), true, nil, nil)
	in := bytes.NewReader(content)

	writer := &skipChunkWriter{skip: map[int]bool{0: true, 2: true}}
	opener := &skipChunkOpener{writer: writer, chunkSz: 30}

	out, err := UploadMultipart(ctx, src, in, UploadMultipartOptions{Open: opener})
	require.NoError(t, err)
	require.Equal(t, fs.ChunkWriter(writer), out)
	assert.True(t, writer.closed)
	assert.False(t, writer.aborted)
	// 100 bytes in chunks of 30 -> chunks 0..3; 0 and 2 skipped
	assert.ElementsMatch(t, []int{1, 3}, writer.written)
}

func TestUploadMultipartNoSkipgerWritesAll(t *testing.T) {
	ctx := context.Background()
	content := bytes.Repeat([]byte("y"), 65)
	src := object.NewStaticObjectInfo("file.bin", time.Now(), int64(len(content)), true, nil, nil)
	in := bytes.NewReader(content)

	plain := &plainChunkWriter{}
	opener := &plainOpener{writer: plain, chunkSz: 30}

	_, err := UploadMultipart(ctx, src, in, UploadMultipartOptions{Open: opener})
	require.NoError(t, err)
	assert.True(t, plain.closed)
	// 65 bytes in chunks of 30 -> chunks 0..2, all written
	assert.ElementsMatch(t, []int{0, 1, 2}, plain.written)
}

// plainChunkWriter has no SkipChunk method - UploadMultipart must write all.
type plainChunkWriter struct {
	mu      sync.Mutex
	written []int
	closed  bool
}

func (w *plainChunkWriter) WriteChunk(ctx context.Context, chunkNumber int, reader io.ReadSeeker) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.written = append(w.written, chunkNumber)
	n, err := io.Copy(io.Discard, reader)
	return n, err
}

func (w *plainChunkWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

func (w *plainChunkWriter) Abort(ctx context.Context) error { return nil }

type plainOpener struct {
	writer  *plainChunkWriter
	chunkSz int64
}

func (o *plainOpener) OpenChunkWriter(ctx context.Context, remote string, src fs.ObjectInfo, options ...fs.OpenOption) (fs.ChunkWriterInfo, fs.ChunkWriter, error) {
	return fs.ChunkWriterInfo{ChunkSize: o.chunkSz, Concurrency: 2}, o.writer, nil
}

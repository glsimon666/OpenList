package common

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/net"
	"github.com/OpenListTeam/OpenList/v4/internal/sign"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

func Proxy(w http.ResponseWriter, r *http.Request, link *model.Link, file model.Obj) error {
	// Check if this is a large ISO file (> 10GB) that needs special handling
	fileName := strings.ToLower(file.GetName())
	if (strings.HasSuffix(fileName, ".iso") || strings.HasSuffix(fileName, ".nrg")) && file.GetSize() > 10*1024*1024*1024 { // 10GB
		return handleLargeIsoFile(w, r, link, file)
	}

	// if link.MFile != nil {
	// 	attachHeader(w, file, link)
	// 	http.ServeContent(w, r, file.GetName(), file.ModTime(), link.MFile)
	// 	return nil
	// }

	if link.Concurrency > 0 || link.PartSize > 0 {
		attachHeader(w, file, link)
		size := link.ContentLength
		if size <= 0 {
			size = file.GetSize()
		}
		rrf, _ := stream.GetRangeReaderFromLink(size, link)
		if link.RangeReader == nil {
			r = r.WithContext(context.WithValue(r.Context(), conf.RequestHeaderKey, r.Header))
		}
		return net.ServeHTTP(w, r, file.GetName(), file.ModTime(), size, &model.RangeReadCloser{
			RangeReader: rrf,
		})
	}

	if link.RangeReader != nil {
		attachHeader(w, file, link)
		size := link.ContentLength
		if size <= 0 {
			size = file.GetSize()
		}
		return net.ServeHTTP(w, r, file.GetName(), file.ModTime(), size, &model.RangeReadCloser{
			RangeReader: link.RangeReader,
		})
	}

	//transparent proxy
	header := net.ProcessHeader(r.Header, link.Header)
	res, err := net.RequestHttp(r.Context(), r.Method, header, link.URL)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	// Copy headers manually instead of using maps.Copy
	for key, values := range res.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(res.StatusCode)
	if r.Method == http.MethodHead {
		return nil
	}
	_, err = utils.CopyWithBuffer(w, &stream.RateLimitReader{
		Reader:  res.Body,
		Limiter: stream.ServerDownloadLimit,
		Ctx:     r.Context(),
	})
	return err
}
func attachHeader(w http.ResponseWriter, file model.Obj, link *model.Link) {
	fileName := file.GetName()
	w.Header().Set("Content-Disposition", utils.GenerateContentDisposition(fileName))
	w.Header().Set("Content-Type", utils.GetMimeType(fileName))
	size := link.ContentLength
	if size <= 0 {
		size = file.GetSize()
	}
	w.Header().Set("Etag", GetEtag(file, size))
	contentType := link.Header.Get("Content-Type")
	if len(contentType) > 0 {
		w.Header().Set("Content-Type", contentType)
	} else {
		w.Header().Set("Content-Type", utils.GetMimeType(fileName))
	}
}
func GetEtag(file model.Obj, size int64) string {
	hash := ""
	for _, v := range file.GetHash().Export() {
		if v > hash {
			hash = v
		}
	}
	if len(hash) > 0 {
		return fmt.Sprintf(`"%s"`, hash)
	}
	// 参考nginx
	return fmt.Sprintf(`"%x-%x"`, file.ModTime().Unix(), size)
}

func ProxyRange(ctx context.Context, link *model.Link, size int64) *model.Link {
	if link.RangeReader == nil && !strings.HasPrefix(link.URL, GetApiUrl(ctx)+"/") {
		if link.ContentLength > 0 {
			size = link.ContentLength
		}
		rrf, err := stream.GetRangeReaderFromLink(size, link)
		if err == nil {
			return &model.Link{
				RangeReader:   rrf,
				ContentLength: size,
			}
		}
	}
	return link
}

type InterceptResponseWriter struct {
	http.ResponseWriter
	io.Writer
}

func (iw *InterceptResponseWriter) Write(p []byte) (int, error) {
	return iw.Writer.Write(p)
}

type WrittenResponseWriter struct {
	http.ResponseWriter
	written bool
}

func (ww *WrittenResponseWriter) Write(p []byte) (int, error) {
	n, err := ww.ResponseWriter.Write(p)
	if !ww.written && n > 0 {
		ww.written = true
	}
	return n, err
}

func (ww *WrittenResponseWriter) IsWritten() bool {
	return ww.written
}

func GenerateDownProxyURL(storage *model.Storage, reqPath string) string {
	if storage.DownProxyURL == "" {
		return ""
	}
	query := ""
	if !storage.DisableProxySign {
		query = "?sign=" + sign.Sign(reqPath)
	}
	return fmt.Sprintf("%s%s%s",
		strings.Split(storage.DownProxyURL, "\n")[0],
		utils.EncodePath(reqPath, true),
		query,
	)
}

// Cache structure to track ISO file caching state
type IsoCacheState struct {
	sync.Mutex
	Cached bool
	FilePath string
}

var isoCacheMap = sync.Map{} // Map to store cache states for ISO files

func handleLargeIsoFile(w http.ResponseWriter, r *http.Request, link *model.Link, file model.Obj) error {
	cacheDir := filepath.Join(os.TempDir(), "iso_cache")
	err := os.MkdirAll(cacheDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create cache directory: %v", err)
	}

	filePath := filepath.Join(cacheDir, file.GetName())
	
	// Get or create cache state for this file
	cacheStateInterface, _ := isoCacheMap.LoadOrStore(filePath, &IsoCacheState{})
	cacheState := cacheStateInterface.(*IsoCacheState)
	
	// Acquire lock to ensure only one goroutine caches the file
	cacheState.Lock()
	
	// Check if file is already cached
	if _, err := os.Stat(filePath); err == nil {
		// File already exists, we can serve directly
		cacheState.Cached = true
		cacheState.FilePath = filePath
		cacheState.Unlock()
	} else if !os.IsNotExist(err) {
		cacheState.Unlock()
		return fmt.Errorf("error checking cache file: %v", err)
	} else if !cacheState.Cached {
		// First time accessing this file, need to start caching
		go cacheIsoFileParts(filePath, link.URL, file.GetSize())
		cacheState.Cached = true
		cacheState.FilePath = filePath
		cacheState.Unlock() // Unlock before waiting
		
		// Wait until the file exists before proceeding
		for {
			if _, err := os.Stat(filePath); err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond) // Wait briefly before checking again
		}
	} else {
		// Another goroutine is already caching, just unlock and proceed
		cacheState.Unlock()
	}
	
	// Now serve the file with range support
	attachHeader(w, file, link)
	
	// Create a custom reader that can serve from cache for known ranges
	size := file.GetSize()
	isoReader := &IsoFileReader{
		filePath: filePath,
		linkURL:  link.URL,
		fileSize: size,
	}
	
	return net.ServeHTTP(w, r, file.GetName(), file.ModTime(), size, &model.RangeReadCloser{
		RangeReader: isoReader,
	})
}

// cacheIsoFileParts caches the first 32MB and last 5MB of the ISO file
func cacheIsoFileParts(filePath, linkURL string, fileSize int64) error {
	// Open the target file for writing
	outFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create cache file: %v", err)
	}
	defer outFile.Close()

	// Download first 32MB
	firstPartSize := int64(32 * 1024 * 1024) // 32MB
	if fileSize < firstPartSize {
		firstPartSize = fileSize
	}
	
	if firstPartSize > 0 {
		headers := make(map[string]string)
		headers["Range"] = fmt.Sprintf("bytes=0-%d", firstPartSize-1)
		resp, err := net.RequestHttp(context.Background(), "GET", http.Header(headers), linkURL)
		if err != nil {
			return fmt.Errorf("failed to download first part: %v", err)
		}
		defer resp.Body.Close()
		
		_, err = io.CopyN(outFile, resp.Body, firstPartSize)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to write first part: %v", err)
		}
	}

	// Pad the file to full size with zeros temporarily
	paddingSize := fileSize - firstPartSize
	if paddingSize > 0 {
		zeroChunk := make([]byte, 1024*1024) // 1MB chunk of zeros
		remaining := paddingSize
		for remaining > 0 {
			chunkSize := int64(len(zeroChunk))
			if remaining < chunkSize {
				chunkSize = remaining
			}
			_, err := outFile.Write(zeroChunk[:chunkSize])
			if err != nil {
				return fmt.Errorf("failed to pad file: %v", err)
			}
			remaining -= chunkSize
		}
	}

	// Download last 5MB if needed
	lastPartSize := int64(5 * 1024 * 1024) // 5MB
	if fileSize > lastPartSize {
		lastStart := fileSize - lastPartSize
		
		headers := make(map[string]string)
		headers["Range"] = fmt.Sprintf("bytes=%d-", lastStart)
		resp, err := net.RequestHttp(context.Background(), "GET", http.Header(headers), linkURL)
		if err != nil {
			return fmt.Errorf("failed to download last part: %v", err)
		}
		defer resp.Body.Close()
		
		// Seek to the correct position in the output file
		_, err = outFile.Seek(lastStart, 0)
		if err != nil {
			return fmt.Errorf("failed to seek in output file: %v", err)
		}
		
		_, err = io.Copy(outFile, resp.Body)
		if err != nil {
			return fmt.Errorf("failed to write last part: %v", err)
		}
	}
	
	return nil
}

// IsoFileReader handles reading from partially cached ISO file
type IsoFileReader struct {
	filePath string
	linkURL  string
	fileSize int64
}

func (r *IsoFileReader) RangeRead(ctx context.Context, httpRange http_range.Range) (io.ReadCloser, error) {
	offset := httpRange.Start
	length := httpRange.Length

	// Check if the requested range is in the cached parts (first 32MB or last 5MB)
	firstPartEnd := int64(32 * 1024 * 1024) // 32MB
	lastPartStart := r.fileSize - int64(5 * 1024 * 1024) // Last 5MB start
	
	requestEnd := offset + length
	
	// If the entire request is in the cached areas, serve from local cache
	if (offset < firstPartEnd && requestEnd <= firstPartEnd) || 
	   (offset >= lastPartStart) {
		// Read from local file
		file, err := os.Open(r.filePath)
		if err != nil {
			return nil, err
		}
		
		_, err = file.Seek(offset, 0)
		if err != nil {
			file.Close()
			return nil, err
		}
		
		limitReader := io.LimitReader(file, length)
		return &LimitedReadCloser{Reader: limitReader, File: file}, nil
	}
	
	// Otherwise, fetch from the original source
	headers := make(map[string]string)
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	resp, err := net.RequestHttp(ctx, "GET", http.Header(headers), r.linkURL)
	if err != nil {
		return nil, err
	}
	
	return resp.Body, nil
}

// Add a Close method to comply with RangeReadCloserIF interface if needed
func (r *IsoFileReader) Close() error {
	return nil
}

// LimitedReadCloser wraps a Reader with a Closer that closes the underlying file
type LimitedReadCloser struct {
	io.Reader
	File *os.File
}

func (l *LimitedReadCloser) Close() error {
	return l.File.Close()
}

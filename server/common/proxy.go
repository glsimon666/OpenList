package common

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"maps"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/net"
	"github.com/OpenListTeam/OpenList/v4/internal/sign"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

// --- ISO/UDF 缓存增强逻辑 (新增) ---

type ISOCache struct {
	Header []byte
	Footer []byte
	Size   int64
}

var (
	isoCacheMap = make(map[string]*ISOCache)
	isoLock     sync.RWMutex
)

const (
	ISOHeaderSize       = 30 * 1024 * 1024
	ISOFooterSize       = 5 * 1024 * 1024
	InternalActionKey   = "__ol_action"
	InternalActionClear = "clear_iso_cache"
)

func getUniqueFileID(file model.Obj, size int64) string {
	id := file.GetID()
	if id != "" {
		return id
	}
	return fmt.Sprintf("%x", md5.Sum([]byte(file.GetName()+fmt.Sprint(size))))
}

type SmartISORangeReader struct {
	baseReader model.RangeReaderIF
	cache      *ISOCache
}

func (r *SmartISORangeReader) RangeRead(ctx context.Context, hRange http_range.Range) (io.ReadCloser, error) {
	off := hRange.Start
	reqLen := hRange.Length
	if reqLen <= 0 {
		reqLen = r.cache.Size - off
	}

	// 1. 头部缓存边界校验
	headerLen := int64(len(r.cache.Header))
	if off >= 0 && off+reqLen <= headerLen {
		data := make([]byte, reqLen)
		copy(data, r.cache.Header[off:off+reqLen])
		return io.NopCloser(strings.NewReader(string(data))), nil
	}

	// 2. 尾部缓存边界校验
	footerLen := int64(len(r.cache.Footer))
	footerStart := r.cache.Size - footerLen
	if off >= footerStart && off+reqLen <= r.cache.Size {
		footerOff := off - footerStart
		if footerOff >= 0 && footerOff+reqLen <= footerLen {
			data := make([]byte, reqLen)
			copy(data, r.cache.Footer[footerOff:footerOff+reqLen])
			return io.NopCloser(strings.NewReader(string(data))), nil
		}
	}

	// 3. 越界或跨边界请求透传
	return r.baseReader.RangeRead(ctx, hRange)
}


func getSmartReader(ctx context.Context, rrf model.RangeReaderIF, file model.Obj, size int64) model.RangeReaderIF {
	name := strings.ToLower(file.GetName())
	if (strings.HasSuffix(name, ".iso") || strings.HasSuffix(name, ".udf")) && size > (ISOHeaderSize+ISOFooterSize) {
		fileID := getUniqueFileID(file, size)
		isoLock.RLock()
		cache, ok := isoCacheMap[fileID]
		isoLock.RUnlock()

		if !ok {
			header := make([]byte, ISOHeaderSize)
			footer := make([]byte, ISOFooterSize)
			rcH, errH := rrf.RangeRead(ctx, http_range.Range{Start: 0, Length: ISOHeaderSize})
			if errH == nil {
				io.ReadFull(rcH, header)
				rcH.Close()
				rcF, errF := rrf.RangeRead(ctx, http_range.Range{Start: size - ISOFooterSize, Length: ISOFooterSize})
				if errF == nil {
					io.ReadFull(rcF, footer)
					rcF.Close()
					cache = &ISOCache{Header: header, Footer: footer, Size: size}
					isoLock.Lock()
					isoCacheMap[fileID] = cache
					isoLock.Unlock()
					time.AfterFunc(2*time.Hour, func() {
						isoLock.Lock()
						delete(isoCacheMap, fileID)
						isoLock.Unlock()
					})
				}
			}
		}
		if cache != nil {
			return &SmartISORangeReader{baseReader: rrf, cache: cache}
		}
	}
	return rrf
}

// --- 原有逻辑融入 ---

func Proxy(w http.ResponseWriter, r *http.Request, link *model.Link, file model.Obj) error {
	size := link.ContentLength
	if size <= 0 {
		size = file.GetSize()
	}

	// [新增] 清理指令拦截
	if r.URL.Query().Get(InternalActionKey) == InternalActionClear {
		fileID := getUniqueFileID(file, size)
		isoLock.Lock()
		delete(isoCacheMap, fileID)
		isoLock.Unlock()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
		return nil
	}

	if link.Concurrency > 0 || link.PartSize > 0 {
		attachHeader(w, file, link)
		rrf, _ := stream.GetRangeReaderFromLink(size, link)
		
		// [新增] 注入 ISO 优化 Reader
		rrf = getSmartReader(r.Context(), rrf, file, size)

		if link.RangeReader == nil {
			r = r.WithContext(context.WithValue(r.Context(), conf.RequestHeaderKey, r.Header))
		}
		return net.ServeHTTP(w, r, file.GetName(), file.ModTime(), size, &model.RangeReadCloser{
			RangeReader: rrf,
		})
	}

	if link.RangeReader != nil {
		attachHeader(w, file, link)
		
		// [新增] 注入 ISO 优化 Reader
		rrf := getSmartReader(r.Context(), link.RangeReader, file, size)

		return net.ServeHTTP(w, r, file.GetName(), file.ModTime(), size, &model.RangeReadCloser{
			RangeReader: rrf,
		})
	}

	//transparent proxy
	header := net.ProcessHeader(r.Header, link.Header)
	res, err := net.RequestHttp(r.Context(), r.Method, header, link.URL)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	maps.Copy(w.Header(), res.Header)
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
	
	// [新增/修改] MIME 类型适配：针对 ISO/UDF 强行指定，其余逻辑保留原样
	lowerName := strings.ToLower(fileName)
	if strings.HasSuffix(lowerName, ".iso") || strings.HasSuffix(lowerName, ".udf") {
		w.Header().Set("Content-Type", "application/x-iso9660-image")
	} else {
		contentType := link.Header.Get("Content-Type")
		if len(contentType) > 0 {
			w.Header().Set("Content-Type", contentType)
		} else {
			w.Header().Set("Content-Type", utils.GetMimeType(fileName))
		}
	}

	size := link.ContentLength
	if size <= 0 {
		size = file.GetSize()
	}
	w.Header().Set("Etag", GetEtag(file, size))
	w.Header().Set("Accept-Ranges", "bytes")
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
	// 修正 conf.URL 拼接类型错误
	if link.RangeReader == nil && !strings.HasPrefix(link.URL, conf.URL.String()+"/") {
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

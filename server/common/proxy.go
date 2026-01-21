package common

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"maps"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/net"
	"github.com/OpenListTeam/OpenList/v4/internal/sign"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// 4MB 缓存大小常量
const CacheSize = 4 * 1024 * 1024

func Proxy(w http.ResponseWriter, r *http.Request, link *model.Link, file model.Obj) error {
	// ========== 新增ISO缓存逻辑开始（修复类型和字段错误） ==========
	var rangeReader io.ReaderAt
	// 修复：RangeReaderIF 转 io.ReaderAt 需要类型断言（仅当实现时才赋值）
	if link.RangeReader != nil {
		var ok bool
		rangeReader, ok = link.RangeReader.(io.ReaderAt)
		if !ok {
			log.Debugf("RangeReader 未实现 io.ReaderAt 接口，跳过ISO缓存")
		}
	}

	// 修复：移除所有 link.MFile 引用（该字段不存在）
	// 缓存ISO首尾4M（仅当有合法的 ReaderAt 且是ISO文件时执行）
	if rangeReader != nil && strings.EqualFold(filepath.Ext(file.GetName()), ".iso") {
		_ = CacheISOParts(r.Context(), file, rangeReader)
	}

	// 解析Range请求头
	rangeStr := r.Header.Get("Range")
	isCachedRange, start, end := IsRangeInCachedArea(rangeStr, file.GetSize())
	if isCachedRange {
		cachePath := GetTempFilePath(file)
		if cachePath == "" {
			goto ORIGINAL_LOGIC // 缓存路径无效，走原始逻辑
		}

		// 从缓存读取数据
		buf, err := ReadCachedRange(cachePath, start, end, file.GetSize())
		if err != nil {
			log.Warnf("读取ISO缓存失败，降级到原始逻辑: %v", err)
			goto ORIGINAL_LOGIC
		}

		// 设置响应头
		attachHeader(w, file, link)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, file.GetSize()))
		w.WriteHeader(http.StatusPartialContent)
		_, err = w.Write(buf)
		return err
	}
	// ========== 新增ISO缓存逻辑结束 ==========

ORIGINAL_LOGIC:
	// 原始代理逻辑（保持原有代码不变，已移除MFile相关注释）
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

// GetTempFilePath 获取ISO文件的缓存文件路径（复用conf.TempDirName目录）
func GetTempFilePath(file model.Obj) string {
	tempDir := conf.TempDirName
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		log.Warnf("创建temp目录失败: %v", err)
		return ""
	}
	// 用文件ID+名称生成唯一缓存文件名，避免冲突
	return filepath.Join(tempDir, file.GetID()+"_"+file.GetName()+".cache")
}

// CacheISOParts 缓存ISO文件的首尾各4M数据到temp目录
func CacheISOParts(ctx context.Context, file model.Obj, readerAt io.ReaderAt) error {
	// 仅处理ISO文件
	if !strings.EqualFold(filepath.Ext(file.GetName()), ".iso") {
		return nil
	}

	cachePath := GetTempFilePath(file)
	// 若缓存已存在，直接返回（避免重复缓存）
	if _, err := os.Stat(cachePath); err == nil {
		return nil
	}

	// 创建缓存文件
	cacheFile, err := os.Create(cachePath)
	if err != nil {
		log.Warnf("创建ISO缓存文件失败: %v", err)
		return err
	}
	defer cacheFile.Close()

	fileSize := file.GetSize()
	// 缓存前4M
	headBuf := make([]byte, CacheSize)
	n, err := readerAt.ReadAt(headBuf, 0)
	if err != nil && err != io.EOF {
		log.Warnf("读取ISO头部失败: %v", err)
		return err
	}
	if _, err := cacheFile.Write(headBuf[:n]); err != nil {
		log.Warnf("写入ISO头部缓存失败: %v", err)
		return err
	}

	// 缓存后4M（若文件小于8M则只缓存剩余部分）
	if fileSize > CacheSize {
		offset := fileSize - CacheSize
		if offset < CacheSize {
			offset = CacheSize
		}
		tailBuf := make([]byte, CacheSize)
		n, err := readerAt.ReadAt(tailBuf, offset)
		if err != nil && err != io.EOF {
			log.Warnf("读取ISO尾部失败: %v", err)
			return err
		}
		if _, err := cacheFile.Write(tailBuf[:n]); err != nil {
			log.Warnf("写入ISO尾部缓存失败: %v", err)
			return err
		}
	}

	log.Infof("ISO文件缓存完成: %s, 路径: %s", file.GetName(), cachePath)
	return nil
}

// IsRangeInCachedArea 判断Range是否在首尾4M范围内
func IsRangeInCachedArea(rangeStr string, fileSize int64) (bool, int64, int64) {
	if rangeStr == "" {
		return false, 0, 0
	}

	// 解析Range头，格式如 "bytes=0-1023"
	parts := strings.SplitN(strings.TrimPrefix(rangeStr, "bytes="), "-", 2)
	if len(parts) != 2 {
		return false, 0, 0
	}

	start, err := ParseInt(parts[0])
	if err != nil {
		return false, 0, 0
	}
	endStr := parts[1]
	end := fileSize - 1
	if endStr != "" {
		end, err = ParseInt(endStr)
		if err != nil {
			return false, 0, 0
		}
	}

	// 判断是否在头部4M或尾部4M
	inHead := end < CacheSize
	inTail := start > (fileSize - CacheSize)
	return inHead || inTail, start, end
}

// ReadCachedRange 从缓存文件读取指定Range的数据
func ReadCachedRange(cachePath string, start, end, fileSize int64) ([]byte, error) {
	cacheFile, err := os.Open(cachePath)
	if err != nil {
		return nil, err
	}
	defer cacheFile.Close()

	// 计算缓存内的偏移量（尾部数据在缓存文件中从4M开始）
	var cacheOffset int64
	if start < CacheSize {
		cacheOffset = start
	} else {
		cacheOffset = CacheSize + (start - (fileSize - CacheSize))
	}

	length := end - start + 1
	buf := make([]byte, length)
	_, err = cacheFile.ReadAt(buf, cacheOffset)
	return buf, err
}

// ParseInt 安全解析字符串为int64
func ParseInt(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
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

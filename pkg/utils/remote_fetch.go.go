// pkg/utils/remote_fetch.go
package utils

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// ISO_CACHE_SIZE ISO文件前10M缓存（固定值，无需配置化）
	ISO_CACHE_SIZE = 10 * 1024 * 1024 // 10MB
)

// FetchISOFirst10M 拉取远程URL的前10M字节到内存，带5秒超时
// url: 115文件的真实下载链接；header: 请求头（如User-Agent）
func FetchISOFirst10M(url string, header http.Header) ([]byte, error) {
	// 1. 5秒超时上下文（核心超时控制）
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 2. 构建Range请求（仅拉取前10M）
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request failed: %w", err)
	}
	// 设置Range头：仅获取0-10485759字节（前10M）
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", ISO_CACHE_SIZE-1))
	// 复用传入的请求头（如115驱动的User-Agent）
	for k, v := range header {
		req.Header[k] = v
	}

	// 3. 发起请求（受5秒超时控制）
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch failed: %w", err)
	}
	defer resp.Body.Close()

	// 4. 校验响应状态
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid status code: %d, url: %s", resp.StatusCode, url)
	}

	// 5. 读取前10M到内存
	buf := make([]byte, 0, ISO_CACHE_SIZE)
	n, err := io.ReadFull(resp.Body, make([]byte, ISO_CACHE_SIZE))
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		log.Warnf("read first 10M failed: %v, url: %s", err, url)
		return nil, fmt.Errorf("read content failed: %w", err)
	}
	buf = append(buf, make([]byte, n)...)

	// 6. 校验读取长度（异常处理）
	if int64(len(buf)) < ISO_CACHE_SIZE {
		log.Debugf("file size < 10M, actual read: %d bytes", len(buf))
	}

	return buf, nil
}

// ReleaseBytes 强制释放字节切片内存（置nil让GC回收）
func ReleaseBytes(buf []byte) {
	buf = nil
}

// internal/fs/file_stream.go
package fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

// 原有代码保留，新增以下内容：

// HybridReader 混合读取器（仅针对115 ISO文件：前10M内存，其余远程）
type HybridReader struct {
	isoCache    []byte       // ISO前10M缓存
	remoteURL   string       // 115真实下载链接
	header      http.Header  // 请求头（User-Agent等）
	remoteRead  io.ReadCloser// 远程流（10M后内容）
	offset      int64        // 当前读取偏移量
	closers     utils.SyncClosers // 资源释放器
}

// New115ISOHybridReader 创建115 ISO文件的混合读取器
// isoCache: 前10M缓存；remoteURL: 115真实下载链接；header: 请求头
func New115ISOHybridReader(isoCache []byte, remoteURL string, header http.Header) (*HybridReader, error) {
	hr := &HybridReader{
		isoCache:  isoCache,
		remoteURL: remoteURL,
		header:    header,
		closers:   utils.NewSyncClosers(),
	}
	return hr, nil
}

// Read 实现io.Reader接口（核心逻辑：前10M读内存，其余读远程）
func (h *HybridReader) Read(p []byte) (n int, err error) {
	// 1. 前10M从内存读取
	if h.offset < utils.ISO_CACHE_SIZE {
		remaining := utils.ISO_CACHE_SIZE - h.offset
		readLen := int64(len(p))
		if readLen > remaining {
			readLen = remaining
		}
		// 从内存缓存拷贝
		copy(p, h.isoCache[h.offset:h.offset+readLen])
		h.offset += readLen
		n = int(readLen)
		p = p[readLen:] // 剩余待读取的切片
		if len(p) == 0 {
			return n, nil
		}
	}

	// 2. 10M后从远程读取（懒加载远程流）
	if h.remoteRead == nil {
		// 构建远程请求：Range从10M开始
		req, err := http.NewRequest("GET", h.remoteURL, nil)
		if err != nil {
			return n, fmt.Errorf("create remote request failed: %w", err)
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", utils.ISO_CACHE_SIZE))
		for k, v := range h.header {
			req.Header[k] = v
		}

		// 发起请求（带5秒超时）
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req = req.WithContext(ctx)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return n, fmt.Errorf("remote request failed: %w", err)
		}
		h.remoteRead = resp.Body
		h.closers.Add(h.remoteRead) // 注册远程流到释放器
	}

	// 3. 读取远程内容
	remoteN, err := h.remoteRead.Read(p)
	n += remoteN
	h.offset += int64(remoteN)
	return n, err
}

// Close 实现io.Closer接口（强制释放所有资源）
func (h *HybridReader) Close() error {
	// 1. 释放远程流
	if err := h.closers.Close(); err != nil {
		log.Warnf("HybridReader close err: %v", err)
	}
	// 2. 强制释放内存缓存
	utils.ReleaseBytes(h.isoCache)
	h.isoCache = nil
	h.remoteRead = nil
	log.Debug("115 ISO HybridReader resources released")
	return nil
}

// 原有代码保留...

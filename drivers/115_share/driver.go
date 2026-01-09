package _115_share

import (
	"context"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	driver115 "github.com/SheltonZhu/115driver/pkg/driver"
	"golang.org/x/time/rate"
)

type Pan115Share struct {
	model.Storage
	Addition
	client  *driver115.Pan115Client
	limiter *rate.Limiter
}

func (d *Pan115Share) Config() driver.Config {
	return config
}

func (d *Pan115Share) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *Pan115Share) Init(ctx context.Context) error {
	if d.LimitRate > 0 {
		d.limiter = rate.NewLimiter(rate.Limit(d.LimitRate), 1)
	}

	return d.login()
}

func (d *Pan115Share) WaitLimit(ctx context.Context) error {
	if d.limiter != nil {
		return d.limiter.Wait(ctx)
	}
	return nil
}

func (d *Pan115Share) Drop(ctx context.Context) error {
	return nil
}

func (d *Pan115Share) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	if err := d.WaitLimit(ctx); err != nil {
		return nil, err
	}

	files := make([]driver115.ShareFile, 0)
	fileResp, err := d.client.GetShareSnap(d.ShareCode, d.ReceiveCode, dir.GetID(), driver115.QueryLimit(int(d.PageSize)))
	if err != nil {
		return nil, err
	}
	files = append(files, fileResp.Data.List...)
	total := fileResp.Data.Count
	count := len(fileResp.Data.List)
	for total > count {
		fileResp, err := d.client.GetShareSnap(
			d.ShareCode, d.ReceiveCode, dir.GetID(),
			driver115.QueryLimit(int(d.PageSize)), driver115.QueryOffset(count),
		)
		if err != nil {
			return nil, err
		}
		files = append(files, fileResp.Data.List...)
		count += len(fileResp.Data.List)
	}

	return utils.SliceConvert(files, transFunc)
}

func (d *Pan115Share) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	if err := d.WaitLimit(ctx); err != nil {
		return nil, err
	}

	// 原有逻辑：调用DownloadByShareCode获取下载信息
	downloadInfo, err := d.client.DownloadByShareCode(d.ShareCode, d.ReceiveCode, file.GetID())
	if err != nil {
		return nil, err
	}

	// ========== 新增：ISO文件前10M缓存核心逻辑 ==========
	var (
		linkReader io.ReadCloser       // 混合流/远程流
		contentLen int64               // ISO文件总大小
		realURL    = downloadInfo.URL.URL // 115分享文件真实下载链接
		userAgent  = args.Header.Get("User-Agent") // 获取UA
	)

	// 1. 类型转换：获取文件大小和名称（需确保file可转为ShareFileObj）
	fileObj, ok := file.(*ShareFileObj) // 替换为你实际的分享文件结构体名
	if !ok {
		// 转换失败：降级为原始逻辑（仅返回URL）
		log.Warnf("115 Share can't convert obj to ShareFileObj, fallback to raw URL")
		return &model.Link{URL: realURL}, nil
	}
	contentLen = fileObj.Size // 获取文件总大小

	// 2. 判断是否为ISO文件（通过文件名扩展名）
	ext := filepath.Ext(fileObj.GetName()) // 需确保ShareFileObj有GetName()方法
	if ext == ".iso" {
		log.Debugf("115 Share ISO file detected: %s, fetch first 10M", fileObj.GetName())
		
		// 3. 构建请求头（补充UA）
		header := http.Header{}
		if userAgent != "" {
			header.Set("User-Agent", userAgent)
		}

		// 4. 拉取前10M到内存（带5秒超时）
		isoCache, err := utils.FetchISOFirst10M(realURL, header)
		if err != nil {
			// 拉取失败：降级为原始逻辑（返回URL）
			log.Warnf("115 Share fetch ISO first 10M failed: %v, fallback to raw URL", err)
			return &model.Link{URL: realURL}, nil
		}

		// 5. 创建混合读取器（前10M内存，其余远程）
		hybridReader, err := fs.New115ISOHybridReader(isoCache, realURL, header)
		if err != nil {
			// 创建失败：释放缓存，降级为原始URL
			utils.ReleaseBytes(isoCache)
			log.Warnf("115 Share create hybrid reader failed: %v, fallback to raw URL", err)
			return &model.Link{URL: realURL}, nil
		}
		linkReader = hybridReader
	}

	// 6. 构建返回值（ISO文件用混合流，非ISO保留原有逻辑）
	link := &model.Link{}
	if linkReader != nil {
		// ISO文件：返回混合流（替换原有URL）
		link.RangeReader = stream.GetRangeReaderFromMFile(contentLen, linkReader)
		link.ContentLength = contentLen
		link.RequireReference = true
		link.Header = header // 补充请求头
	} else {
		// 非ISO文件：保留原有逻辑
		link.URL = realURL
	}
	// ========== 新增逻辑结束 ==========

	return link, nil
}

func (d *Pan115Share) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) error {
	return errs.NotSupport
}

func (d *Pan115Share) Move(ctx context.Context, srcObj, dstDir model.Obj) error {
	return errs.NotSupport
}

func (d *Pan115Share) Rename(ctx context.Context, srcObj model.Obj, newName string) error {
	return errs.NotSupport
}

func (d *Pan115Share) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	return errs.NotSupport
}

func (d *Pan115Share) Remove(ctx context.Context, obj model.Obj) error {
	return errs.NotSupport
}

func (d *Pan115Share) Put(ctx context.Context, dstDir model.Obj, stream model.FileStreamer, up driver.UpdateProgress) error {
	return errs.NotSupport
}

var _ driver.Driver = (*Pan115Share)(nil)

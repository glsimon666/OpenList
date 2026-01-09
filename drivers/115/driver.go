package _115

import (
	"context"
	"strings"
	"sync"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	streamPkg "github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	driver115 "github.com/SheltonZhu/115driver/pkg/driver"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
)

type Pan115 struct {
	model.Storage
	Addition
	client     *driver115.Pan115Client
	limiter    *rate.Limiter
	appVerOnce sync.Once
}

func (d *Pan115) Config() driver.Config {
	return config
}

func (d *Pan115) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *Pan115) Init(ctx context.Context) error {
	d.appVerOnce.Do(d.initAppVer)
	if d.LimitRate > 0 {
		d.limiter = rate.NewLimiter(rate.Limit(d.LimitRate), 1)
	}
	return d.login()
}

func (d *Pan115) WaitLimit(ctx context.Context) error {
	if d.limiter != nil {
		return d.limiter.Wait(ctx)
	}
	return nil
}

func (d *Pan115) Drop(ctx context.Context) error {
	return nil
}

func (d *Pan115) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	if err := d.WaitLimit(ctx); err != nil {
		return nil, err
	}
	files, err := d.getFiles(dir.GetID())
	if err != nil && !errors.Is(err, driver115.ErrNotExist) {
		return nil, err
	}
	return utils.SliceConvert(files, func(src FileObj) (model.Obj, error) {
		return &src, nil
	})
}

func (d *Pan115) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	if err := d.WaitLimit(ctx); err != nil {
		return nil, err
	}

	// 原有逻辑：获取UA、调用DownloadWithUA获取下载信息
	userAgent := args.Header.Get("User-Agent")
	fileObj, ok := file.(*FileObj) // 显式类型转换（便于后续获取Size/Name）
	if !ok {
		return nil, fmt.Errorf("can't convert obj to FileObj")
	}
	downloadInfo, err := d.DownloadWithUA(fileObj.PickCode, userAgent)
	if err != nil {
		return nil, err
	}

	// ========== 新增：ISO文件前10M缓存核心逻辑 ==========
	var (
		linkReader io.ReadCloser       // 混合流/远程流
		contentLen int64 = fileObj.Size// ISO文件总大小（需确保FileObj有Size字段）
		realURL    = downloadInfo.Url.Url // 115真实下载链接
	)

	// 1. 判断是否为ISO文件（通过文件名扩展名）
	ext := filepath.Ext(fileObj.GetName()) // 需确保FileObj有GetName()方法
	if ext == ".iso" {
		log.Debugf("115 Pan ISO file detected: %s, fetch first 10M", fileObj.GetName())
		
		// 2. 构建请求头（复用原有Header + UA）
		header := downloadInfo.Header.Clone() // 克隆原有Header，避免修改原数据
		if header.Get("User-Agent") == "" {
			header.Set("User-Agent", userAgent)
		}

		// 3. 拉取前10M到内存（带5秒超时）
		isoCache, err := utils.FetchISOFirst10M(realURL, header)
		if err != nil {
			// 拉取失败：降级为原始逻辑（返回URL）
			log.Warnf("115 Pan fetch ISO first 10M failed: %v, fallback to raw URL", err)
			return &model.Link{
				URL:    realURL,
				Header: downloadInfo.Header,
			}, nil
		}

		// 4. 创建混合读取器（前10M内存，其余远程）
		hybridReader, err := fs.New115ISOHybridReader(isoCache, realURL, header)
		if err != nil {
			// 创建失败：释放缓存，降级为原始URL
			utils.ReleaseBytes(isoCache)
			log.Warnf("115 Pan create hybrid reader failed: %v, fallback to raw URL", err)
			return &model.Link{
				URL:    realURL,
				Header: downloadInfo.Header,
			}, nil
		}
		linkReader = hybridReader
	}

	// 5. 构建返回值（ISO文件用混合流，非ISO保留原有逻辑）
	link := &model.Link{
		Header: downloadInfo.Header, // 复用原有Header
	}
	if linkReader != nil {
		// ISO文件：返回混合流（替换原有URL）
		link.RangeReader = stream.GetRangeReaderFromMFile(contentLen, linkReader)
		link.ContentLength = contentLen
		link.RequireReference = true
	} else {
		// 非ISO文件：保留原有逻辑
		link.URL = realURL
	}
	// ========== 新增逻辑结束 ==========

	return link, nil
}

func (d *Pan115) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) (model.Obj, error) {
	if err := d.WaitLimit(ctx); err != nil {
		return nil, err
	}

	result := driver115.MkdirResp{}
	form := map[string]string{
		"pid":   parentDir.GetID(),
		"cname": dirName,
	}
	req := d.client.NewRequest().
		SetFormData(form).
		SetResult(&result).
		ForceContentType("application/json;charset=UTF-8")

	resp, err := req.Post(driver115.ApiDirAdd)

	err = driver115.CheckErr(err, &result, resp)
	if err != nil {
		return nil, err
	}
	f, err := d.getNewFile(result.FileID)
	if err != nil {
		return nil, nil
	}
	return f, nil
}

func (d *Pan115) Move(ctx context.Context, srcObj, dstDir model.Obj) (model.Obj, error) {
	if err := d.WaitLimit(ctx); err != nil {
		return nil, err
	}
	if err := d.client.Move(dstDir.GetID(), srcObj.GetID()); err != nil {
		return nil, err
	}
	f, err := d.getNewFile(srcObj.GetID())
	if err != nil {
		return nil, nil
	}
	return f, nil
}

func (d *Pan115) Rename(ctx context.Context, srcObj model.Obj, newName string) (model.Obj, error) {
	if err := d.WaitLimit(ctx); err != nil {
		return nil, err
	}
	if err := d.client.Rename(srcObj.GetID(), newName); err != nil {
		return nil, err
	}
	f, err := d.getNewFile((srcObj.GetID()))
	if err != nil {
		return nil, nil
	}
	return f, nil
}

func (d *Pan115) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	if err := d.WaitLimit(ctx); err != nil {
		return err
	}
	return d.client.Copy(dstDir.GetID(), srcObj.GetID())
}

func (d *Pan115) Remove(ctx context.Context, obj model.Obj) error {
	if err := d.WaitLimit(ctx); err != nil {
		return err
	}
	return d.client.Delete(obj.GetID())
}

func (d *Pan115) Put(ctx context.Context, dstDir model.Obj, stream model.FileStreamer, up driver.UpdateProgress) (model.Obj, error) {
	if err := d.WaitLimit(ctx); err != nil {
		return nil, err
	}

	var (
		fastInfo *driver115.UploadInitResp
		dirID    = dstDir.GetID()
	)

	if ok, err := d.client.UploadAvailable(); err != nil || !ok {
		return nil, err
	}
	if stream.GetSize() > d.client.UploadMetaInfo.SizeLimit {
		return nil, driver115.ErrUploadTooLarge
	}
	//if digest, err = d.client.GetDigestResult(stream); err != nil {
	//	return err
	//}

	const PreHashSize int64 = 128 * utils.KB
	hashSize := PreHashSize
	if stream.GetSize() < PreHashSize {
		hashSize = stream.GetSize()
	}
	reader, err := stream.RangeRead(http_range.Range{Start: 0, Length: hashSize})
	if err != nil {
		return nil, err
	}
	preHash, err := utils.HashReader(utils.SHA1, reader)
	if err != nil {
		return nil, err
	}
	preHash = strings.ToUpper(preHash)
	fullHash := stream.GetHash().GetHash(utils.SHA1)
	if len(fullHash) != utils.SHA1.Width {
		_, fullHash, err = streamPkg.CacheFullAndHash(stream, &up, utils.SHA1)
		if err != nil {
			return nil, err
		}
	}
	fullHash = strings.ToUpper(fullHash)

	// rapid-upload
	// note that 115 add timeout for rapid-upload,
	// and "sig invalid" err is thrown even when the hash is correct after timeout.
	if fastInfo, err = d.rapidUpload(stream.GetSize(), stream.GetName(), dirID, preHash, fullHash, stream); err != nil {
		return nil, err
	}
	if matched, err := fastInfo.Ok(); err != nil {
		return nil, err
	} else if matched {
		f, err := d.getNewFileByPickCode(fastInfo.PickCode)
		if err != nil {
			return nil, nil
		}
		return f, nil
	}

	var uploadResult *UploadResult
	// 闪传失败，上传
	if stream.GetSize() <= 10*utils.MB { // 文件大小小于10MB，改用普通模式上传
		if uploadResult, err = d.UploadByOSS(ctx, &fastInfo.UploadOSSParams, stream, dirID, up); err != nil {
			return nil, err
		}
	} else {
		// 分片上传
		if uploadResult, err = d.UploadByMultipart(ctx, &fastInfo.UploadOSSParams, stream.GetSize(), stream, dirID, up); err != nil {
			return nil, err
		}
	}

	file, err := d.getNewFile(uploadResult.Data.FileID)
	if err != nil {
		return nil, nil
	}
	return file, nil
}

func (d *Pan115) OfflineList(ctx context.Context) ([]*driver115.OfflineTask, error) {
	resp, err := d.client.ListOfflineTask(0)
	if err != nil {
		return nil, err
	}
	return resp.Tasks, nil
}

func (d *Pan115) OfflineDownload(ctx context.Context, uris []string, dstDir model.Obj) ([]string, error) {
	return d.client.AddOfflineTaskURIs(uris, dstDir.GetID(), driver115.WithAppVer(appVer))
}

func (d *Pan115) DeleteOfflineTasks(ctx context.Context, hashes []string, deleteFiles bool) error {
	return d.client.DeleteOfflineTasks(hashes, deleteFiles)
}

func (d *Pan115) GetDetails(ctx context.Context) (*model.StorageDetails, error) {
	info, err := d.client.GetInfo()
	if err != nil {
		return nil, err
	}
	return &model.StorageDetails{
		DiskUsage: model.DiskUsage{
			TotalSpace: info.SpaceInfo.AllTotal.Size,
			UsedSpace:  info.SpaceInfo.AllUse.Size,
		},
	}, nil
}

var _ driver.Driver = (*Pan115)(nil)

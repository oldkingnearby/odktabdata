package odktabdata

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"
)

// 表格仓库
const (
	TAB_EXPIRE_SECONDS = 300 //5分钟过期
)

type OdkTabRep struct {
	ExpireSeconds int64
	dfs           map[string]*DataFrameFile
	mutx          sync.Mutex
	exit          bool
}

func (otr *OdkTabRep) Init() {
	otr.dfs = make(map[string]*DataFrameFile)
	go otr.removeExpiredDfs()
}

// 添加一个表格
func (otr *OdkTabRep) Add(filePath, hash string) (err error) {
	fileHash, err := GetMd5(filePath)
	if err != nil {
		return
	}
	if fileHash != hash {
		err = fmt.Errorf("hash不一致:%v", fileHash)
		return
	}
	otr.mutx.Lock()
	_, ok := otr.dfs[fileHash]
	otr.mutx.Unlock()
	if ok {
		err = fmt.Errorf("文件已存在:%v", fileHash)
		return
	}
	ext := filepath.Ext(filePath)
	var df DataFrame
	switch ext {
	case ".csv":
		df, err = NewStringDataFrameFromCSV(filePath, "0")
		if err != nil {
			return
		}
	case ".xlsx":
		df, err = NewStringDataFrameFromXLSX(filePath, "", "0")
		if err != nil {
			return
		}
	default:
		err = fmt.Errorf("未识别格式:%v", ext)
		return
	}
	otr.mutx.Lock()
	otr.dfs[fileHash] = &DataFrameFile{
		df:         df,
		Rows:       df.Rows(),
		Cols:       df.Cols(),
		FilePath:   filePath,
		Ext:        ext,
		FileId:     fileHash,
		updateTime: time.Now().Unix(),
		createTime: time.Now().Unix(),
	}
	otr.mutx.Unlock()
	return
}

// 获取一个只读表格，保证不会进行修改

// 获取一个表格的副本
func (otr *OdkTabRep) GetCopy(fileHash string) (df DataFrame, err error) {
	otr.mutx.Lock()
	temDf, ok := otr.dfs[fileHash]
	otr.mutx.Unlock()
	if ok {
		df = temDf.df.Copy()
		temDf.updateTime = time.Now().Unix()
		return
	}
	err = fmt.Errorf("%v不存在,或已过期", fileHash)
	return
}

// 移除一个表格
func (otr *OdkTabRep) Remove(fileHash string) {
	otr.mutx.Lock()
	delete(otr.dfs, fileHash)
	otr.mutx.Unlock()
	return
}

// 退出
func (otr *OdkTabRep) Exit() {
	otr.exit = true
	return
}

// 刷新过期表格并移除
func (otr *OdkTabRep) removeExpiredDfs() {
	for {
		time.Sleep(time.Second * time.Duration(otr.ExpireSeconds))
		otr.mutx.Lock()
		newDfs := make(map[string]*DataFrameFile, len(otr.dfs))
		t := time.Now().Unix()
		for k, v := range otr.dfs {
			if t-v.updateTime < otr.ExpireSeconds {
				newDfs[k] = v
			}
		}
		otr.dfs = newDfs
		otr.mutx.Unlock()
		if otr.exit {
			return
		}
	}
}

package odktabdata

import (
	"crypto/md5"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/tealeg/xlsx"
)

// 判断文件是否存在
func Exists(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}
func WriteConfigToFile(filePath string, config interface{}) (err error) {
	b, err := json.MarshalIndent(config, "", "    ")
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	err = ioutil.WriteFile(filePath, b, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Println("写入文件失败:", err)
		return
	}
	return
}

// 写入数据到文件中 会覆盖文件
func WriteDatas2File(filePath string, datas ...interface{}) (err error) {
	if Exists(filePath) {
		os.Remove(filePath)
	}
	f, err := os.Create(filePath)
	if err != nil {
		return
	}
	defer f.Close()
	for _, data := range datas {
		_, err = f.WriteString(fmt.Sprintf("%v", data))
		if err != nil {
			return
		}
	}
	return
}

// 读取配置文件
func ReadConfigFromFile(filepath string, config interface{}) error {
	//ReadFile函数会读取文件的全部内容，并将结果以[]byte类型返回
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		return err
	}

	//读取的数据为json格式，需要进行解码
	err = json.Unmarshal(data, config)
	if err != nil {
		return err
	}
	return nil
}

// 获取路径 中的文件列表
func GetFilesOfFolder(folder string, ext string) (files []string, err error) {
	dirPath, err := filepath.Abs(folder)
	dir, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return
	}
	PthSep := string(os.PathSeparator)
	for _, fi := range dir {
		if !fi.IsDir() { // 目录, 递归遍历
			// 过滤指定格式
			ok := strings.HasSuffix(fi.Name(), ext)
			if ok {
				files = append(files, dirPath+PthSep+fi.Name())
			}
		}
	}
	return

}

func GetMd5(filePath string) (fileid string, err error) {
	if !Exists(filePath) {
		err = errors.New("文件不存在")
		return
	}
	f, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Open", err)
		return
	}

	defer f.Close()

	md5hash := md5.New()
	_, err = io.Copy(md5hash, f)
	if err != nil {
		fmt.Println("Copy", err)
		return
	}

	fileid = fmt.Sprintf("%x", md5hash.Sum(nil))
	return
}
func NewEmptyColumn(dataType int, rows int) Column {
	switch dataType {
	case ODKINT:
		datas := make([]int64, rows)
		return NewIntColumn(datas...)
	case ODKFLOAT:
		datas := make([]float64, rows)
		return NewFloatColumn(datas...)
	case ODKSTRING:
		datas := make([]string, rows)
		return NewStringColumn(datas...)
	case ODKDATE:
		datas := make([]OdkDateTime, rows)
		return NewDateColumn(datas...)
	default:
		datas := make([]string, rows)
		return NewStringColumn(datas...)
	}
}
func NewIntColumn(datas ...int64) *IntColumn {
	return &IntColumn{
		data:    datas,
		dataLen: len(datas),
	}
}
func NewIntColumnFronInt(datas ...int) *IntColumn {
	int64Datas := make([]int64, len(datas))
	for i, value := range datas {
		int64Datas[i] = int64(value)
	}
	return &IntColumn{
		data:    int64Datas,
		dataLen: len(datas),
	}
}
func NewFloatColumn(datas ...float64) *FloatColumn {
	return &FloatColumn{
		data:    datas,
		dataLen: len(datas),
	}
}
func NewFloatColumnFromFloat32(datas ...float32) *FloatColumn {
	f32Datas := make([]float64, len(datas))
	for i, value := range datas {
		f32Datas[i] = float64(value)
	}
	return &FloatColumn{
		data:    f32Datas,
		dataLen: len(datas),
	}
}
func NewStringColumn(datas ...string) *StringColumn {
	return &StringColumn{
		data:    datas,
		dataLen: len(datas),
	}
}
func NewStringColumnFromBool(datas ...bool) *StringColumn {
	boolDatas := make([]string, len(datas))
	for i, value := range datas {
		if value {
			boolDatas[i] = "true"
		} else {
			boolDatas[i] = "false"
		}

	}
	return &StringColumn{
		data:    boolDatas,
		dataLen: len(datas),
	}
}
func NewDateColumn(datas ...OdkDateTime) *DateColumn {
	return &DateColumn{
		data:    datas,
		dataLen: len(datas),
	}
}
func NewDateColumnFromString(datas ...string) *DateColumn {
	data := make([]OdkDateTime, len(datas))
	for i, value := range datas {
		data[i].InitFormString(value)
	}
	return &DateColumn{
		data:    data,
		dataLen: len(data),
	}
}
func NewDataFrame(columns ...Column) (df DataFrame, err error) {
	err = df.InitDatas(columns...)
	return
}
func NewDataFrameFromMaps(ms []map[string]interface{}) (df DataFrame, err error) {
	colDatas := make(map[string]*[]interface{})
	for i, row := range ms {
		for key, data := range row {
			colData := colDatas[key]
			if colData == nil {
				colDatas[key] = &[]interface{}{}
				colData = colDatas[key]
				*colData = make([]interface{}, len(ms))
			}
			(*colData)[i] = data

		}
	}
	columns := make([]Column, len(colDatas))
	titles := make([]string, len(colDatas))
	i := 0
	for key, value := range colDatas {
		titles[i] = key
		var intCol IntColumn
		err = intCol.InitData(*value...)
		if err == nil {
			columns[i] = intCol.Copy()
			i++
			continue
		}
		var floatCol FloatColumn
		err = floatCol.InitData(*value...)
		if err == nil {
			columns[i] = floatCol.Copy()
			i++
			continue
		}
		var dateCol DateColumn
		err = dateCol.InitData(*value...)
		if err == nil {
			columns[i] = dateCol.Copy()
			i++
			continue
		}
		var strCol StringColumn
		err = strCol.InitData(*value...)
		if err == nil {
			columns[i] = strCol.Copy()
			i++
			continue
		}

		datas := make([]string, len(*value))
		for j, v := range *value {
			datas[j] = fmt.Sprintf("%v", v)
		}
		columns[i] = NewStringColumn(datas...)
		i++
	}
	df, err = NewDataFrame(columns...)
	df.SetColumnTitles(titles...)
	return
}

func NewDataFrameFromJson(jsonPath string) (df DataFrame, err error) {
	var ms []map[string]interface{}
	err = ReadConfigFromFile(jsonPath, &ms)
	if err != nil {
		return
	}
	df, err = NewDataFrameFromMaps(ms)
	return
}

func NewDataFrameFromCSV(csvPath string, fillEmptyStr string) (df DataFrame, err error) {
	// 获取数据，按照文件
	if !Exists(csvPath) {
		err = errors.New("文件不存在")
		return
	}
	cntb, err := ioutil.ReadFile(csvPath)
	if err != nil {
		return
	}
	// 读取文件数据
	r2 := csv.NewReader(strings.NewReader(string(cntb)))
	r2.FieldsPerRecord = -1
	ss, err := r2.ReadAll()
	if err != nil {
		fmt.Println("NewDataFrameFromCSV", err)
		return
	}
	rows := len(ss)
	// 表头
	if rows < 2 {
		err = errors.New("文件数据为空")
		return
	}
	rows--
	cols := len(ss[0])
	if cols < 1 {
		err = errors.New("数据列为空")
		return
	}

	titles := make([]string, cols)
	for i, title := range ss[0] {
		if i == 0 && len(title) > 3 {
			title = title[3:]
		}
		if title == "" {
			title = fmt.Sprintf("T%v", i+1)
		}
		titles[i] = title
	}
	datas := ss[1:]
	columns := make([]Column, cols)
	for j := 0; j < cols; j++ {
		stringData := make([]string, rows)
		for i := 0; i < rows; i++ {
			if len(datas[i]) < j {
				err = errors.New(fmt.Sprintf("读取第%v行,第%v列出错，数据维度不一致", i+2, j+1))
				return
			}
			stringData[i] = datas[i][j]
			if datas[i][j] == "" {
				stringData[i] = fillEmptyStr
			}
		}
		columns[j] = NewStringColumn(stringData...)
		nCol, err := columns[j].Conv2IntColumn()
		if err == nil {
			columns[j] = nCol
			continue
		}
		nCol, err = columns[j].Conv2FloatColumn()
		if err == nil {
			columns[j] = nCol
			continue
		}
		nCol, err = columns[j].Conv2DateColumn()
		if err == nil {
			columns[j] = nCol
			continue
		}
	}
	df, err = NewDataFrame(columns...)
	df.SetColumnTitles(titles...)
	return
}

func NewStringDataFrameFromCSV(csvPath string, fillEmptyStr string) (df DataFrame, err error) {
	// 获取数据，按照文件
	if !Exists(csvPath) {
		err = errors.New("文件不存在")
		return
	}
	cntb, err := ioutil.ReadFile(csvPath)
	if err != nil {
		return
	}
	// 读取文件数据
	r2 := csv.NewReader(strings.NewReader(string(cntb)))
	r2.FieldsPerRecord = -1
	ss, err := r2.ReadAll()
	if err != nil {
		fmt.Println("NewDataFrameFromCSV", err)
		return
	}
	rows := len(ss)
	// 表头
	if rows < 2 {
		err = errors.New("文件数据为空")
		return
	}
	rows--
	cols := len(ss[0])
	if cols < 1 {
		err = errors.New("数据列为空")
		return
	}

	titles := make([]string, cols)
	for i, title := range ss[0] {
		if i == 0 && len(title) > 3 {
			title = title[3:]
		}
		if title == "" {
			title = fmt.Sprintf("T%v", i+1)
		}
		titles[i] = title
	}
	datas := ss[1:]
	columns := make([]Column, cols)
	for j := 0; j < cols; j++ {
		stringData := make([]string, rows)
		for i := 0; i < rows; i++ {
			if len(datas[i]) < j {
				err = errors.New(fmt.Sprintf("读取第%v行,第%v列出错，数据维度不一致", i+2, j+1))
				return
			}
			stringData[i] = datas[i][j]
			if datas[i][j] == "" {
				stringData[i] = fillEmptyStr
			}
		}
		columns[j] = NewStringColumn(stringData...)
	}
	df, err = NewDataFrame(columns...)
	df.SetColumnTitles(titles...)
	return
}

func NewDataFrameFromXLSX(xlsxPath string, sheetName string, fillEmptyStr string) (df DataFrame, err error) {
	if !Exists(xlsxPath) {
		err = errors.New("文件不存在")
		return
	}
	xlFile, err := xlsx.OpenFile(xlsxPath)
	length := xlFile.Sheets[0].MaxRow
	//遍历sheet

	//遍历每一行
	sheet := xlFile.Sheets[0]
	if sheetName != "" {
		sheet = nil
		for _, iSheet := range xlFile.Sheets {
			if iSheet.Name == sheetName {
				sheet = iSheet
				length = sheet.MaxRow
				break
			}
		}
		if sheet == nil {
			err = errors.New(fmt.Sprintf("数据表Sheet:%v 不存在", sheetName))
			return
		}
	}

	ss := make([][]string, length)
	for iRow := 0; iRow < sheet.MaxRow; iRow++ {
		row := sheet.Row(iRow)

		ss[iRow] = make([]string, sheet.MaxCol)
		for j, v := range row.Cells {
			ss[iRow][j] = v.String()
		}

	}
	rows := len(ss)
	// 表头
	if rows < 2 {
		err = errors.New("文件数据为空")
		return
	}
	rows--
	cols := len(ss[0])
	if cols < 1 {
		err = errors.New("数据列为空")
		return
	}

	titles := make([]string, cols)
	for i, title := range ss[0] {

		if title == "" {
			title = fmt.Sprintf("T%v", i+1)
		}
		titles[i] = title
	}
	fmt.Println("发现行列", rows, cols)
	datas := ss[1:]
	columns := make([]Column, cols)
	for j := 0; j < cols; j++ {
		stringData := make([]string, rows)
		for i := 0; i < rows; i++ {
			if len(datas[i]) < j {
				err = errors.New(fmt.Sprintf("读取第%v行,第%v列出错，数据维度不一致", i+2, j+1))
				return
			}
			stringData[i] = datas[i][j]
			if datas[i][j] == "" {
				stringData[i] = fillEmptyStr
			}
		}
		columns[j] = NewStringColumn(stringData...)
		nCol, err := columns[j].Conv2IntColumn()
		if err == nil {
			columns[j] = nCol
			continue
		}
		nCol, err = columns[j].Conv2FloatColumn()
		if err == nil {
			columns[j] = nCol
			continue
		}
		nCol, err = columns[j].Conv2DateColumn()
		if err == nil {
			columns[j] = nCol
			continue
		}
	}
	df, err = NewDataFrame(columns...)
	df.SetColumnTitles(titles...)
	return
}

func NewStringDataFrameFromXLSX(xlsxPath string, sheetName string, fillEmptyStr string) (df DataFrame, err error) {
	if !Exists(xlsxPath) {
		err = errors.New("文件不存在")
		return
	}
	xlFile, err := xlsx.OpenFile(xlsxPath)
	length := xlFile.Sheets[0].MaxRow
	//遍历sheet

	//遍历每一行
	sheet := xlFile.Sheets[0]
	if sheetName != "" {
		sheet = nil
		for _, iSheet := range xlFile.Sheets {
			if iSheet.Name == sheetName {
				sheet = iSheet
				length = sheet.MaxRow
				break
			}
		}
		if sheet == nil {
			err = errors.New(fmt.Sprintf("数据表Sheet:%v 不存在", sheetName))
			return
		}
	}

	ss := make([][]string, length)
	for iRow := 0; iRow < sheet.MaxRow; iRow++ {
		row := sheet.Row(iRow)

		ss[iRow] = make([]string, sheet.MaxCol)
		for j, v := range row.Cells {
			ss[iRow][j] = v.String()
		}
		// for j := 0; j < sheet.MaxCol; j++ {
		// 	cell := row.GetCell(j)
		// 	ss[iRow][j] = cell.String()
		// }
	}
	rows := len(ss)
	// 表头
	if rows < 2 {
		err = errors.New("文件数据为空")
		return
	}
	rows--
	cols := len(ss[0])
	if cols < 1 {
		err = errors.New("数据列为空")
		return
	}

	titles := make([]string, cols)
	for i, title := range ss[0] {

		if title == "" {
			title = fmt.Sprintf("T%v", i+1)
		}
		titles[i] = title
	}
	fmt.Println("发现行列", rows, cols)
	datas := ss[1:]
	columns := make([]Column, cols)
	for j := 0; j < cols; j++ {
		stringData := make([]string, rows)
		for i := 0; i < rows; i++ {
			if len(datas[i]) < j {
				err = errors.New(fmt.Sprintf("读取第%v行,第%v列出错，数据维度不一致", i+2, j+1))
				return
			}
			stringData[i] = datas[i][j]
			if datas[i][j] == "" {
				stringData[i] = fillEmptyStr
			}
		}
		columns[j] = NewStringColumn(stringData...)
	}
	df, err = NewDataFrame(columns...)
	df.SetColumnTitles(titles...)
	return
}

// 导出表格到文件夹中
func OutPutSubDfs2XlsxFolder(folder string, fileNameCol Column, dfs []DataFrame, numTitles ...string) (err error) {
	if fileNameCol.Len() != len(dfs) {
		err = errors.New("数据维度不一致，无法导出")
		return
	}
	for i := 0; i < fileNameCol.Len(); i++ {
		xlsxPath := filepath.Join(folder, fileNameCol.GetStringAt(i)+".xlsx")
		err = dfs[i].Save2Xlsx(xlsxPath, numTitles...)
		if err != nil {
			return
		}
	}
	return
}
func OutPutGroupByRes2Folder(folder string, subDfs []DataFrame, title string) (err error) {
	for i, df := range subDfs {
		xlsxPath := filepath.Join(folder, df.GetColumn(title).GetStringAt(i)+".xlsx")
		err = df.Save2Xlsx(xlsxPath)
		if err != nil {
			return
		}
	}
	return
}
func NewDateFromString(dateStr string) (date OdkDateTime) {
	date.InitFormString(dateStr)
	return
}

// 合并一个文件夹里的Excel
func ConcatFolder(folder string, ext string) (df DataFrame, err error) {
	files, err := GetFilesOfFolder(folder, ext)
	if err != nil {
		return
	}
	dfs := make([]DataFrame, len(files))
	switch ext {
	case ".xlsx":
		for i, file := range files {
			log.Println(file)
			dfs[i], err = NewDataFrameFromXLSX(file, "", "0")
			if err != nil {
				return
			}
		}
	case ".xls":
		for i, file := range files {
			dfs[i], err = NewDataFrameFromXLSX(file, "", "0")
			if err != nil {
				return
			}
		}
	case ".csv":
		for i, file := range files {
			dfs[i], err = NewDataFrameFromCSV(file, "0")
			if err != nil {
				return
			}
		}
	default:
		err = errors.New("不支持的文件格式")
		return
	}
	log.Println("开始合并")
	df, err = Concat(dfs...)
	return
}

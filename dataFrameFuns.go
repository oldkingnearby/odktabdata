package odktabdata

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/tealeg/xlsx"
)

// 新建数据表
func (df *DataFrame) InitDatas(columns ...Column) (err error) {
	dataLens := make(map[int]int)
	df.titles = make([]string, len(columns))
	df.columnTitles = make(map[string]int)
	for i, column := range columns {
		// log.Println(column.Len(), column)
		df.rows = column.Len()
		dataLens[column.Len()] = column.Len()
		df.columnTitles[fmt.Sprintf("未定义%v", i+1)] = i
		df.titles[i] = fmt.Sprintf("未定义%v", i+1)
	}
	if len(dataLens) != 1 {
		log.Println(dataLens)
		err = errors.New("数据维度不一致")
		return
	}
	df.columns = columns
	df.cols = len(columns)
	return
}

// 设置标题
func (df *DataFrame) SetColumnTitles(columnTitles ...string) (err error) {
	if len(columnTitles) != df.cols {
		err = errors.New("数据维度不一致")
		return
	}
	df.titles = columnTitles
	df.columnTitles = make(map[string]int)
	for i, title := range df.titles {
		df.columnTitles[title] = i
	}
	return
}

// 打印数据表
func (df *DataFrame) GetDataFrameString(sep string) string {
	buf := bytes.NewBufferString("")
	for _, title := range df.titles {
		buf.WriteString(title)
		buf.WriteString(sep)
	}
	buf.WriteString("\n")
	for i := 0; i < df.rows; i++ {
		for j := 0; j < df.cols; j++ {
			buf.WriteString(df.columns[j].GetStringAt(i))
			buf.WriteString(sep)
		}
		buf.WriteString("\n")
	}
	return buf.String()
}

// 获取Json数据
func (df *DataFrame) GetDataFrameJson() (ret DataFrameJson) {

	ms := make([]map[string]interface{}, df.rows)
	for i := 0; i < df.rows; i++ {
		row := make(map[string]interface{})
		for j := 0; j < df.cols; j++ {
			switch df.columns[j].DataType() {
			case ODKINT:
				row[df.titles[j]] = df.columns[j].GetIntAt(i)
			case ODKFLOAT:
				row[df.titles[j]] = df.columns[j].GetFloatAt(i)
			default:
				row[df.titles[j]] = df.columns[j].GetStringAt(i)
			}
		}
		ms[i] = row
	}
	ret.Cols, ret.Rows, ret.Titles, ret.Data = df.cols, df.rows, df.titles, ms
	return
}

// 检查某列是否存在
func (df *DataFrame) Exist(title string) bool {
	_, ok := df.columnTitles[title]
	return ok
}

// 获取某列
func (df *DataFrame) GetColumn(title string) Column {
	if !df.Exist(title) {
		log.Fatal("此列不存在")
	}
	return df.columns[df.columnTitles[title]]
}

// 获取一个元素的string值
func (df *DataFrame) GetStringAt(row, col int) string {
	if row >= df.rows {
		return ""
	}
	if col >= df.cols {
		return ""
	}
	return df.columns[col].GetStringAt(row)
}

// 改表头
func (df *DataFrame) ChangeTitle(src, dst string) (err error) {
	if df.Exist(src) {
		index := df.columnTitles[src]
		df.columnTitles[dst] = index
		df.titles[index] = dst
	} else {
		err = errors.New("此列不存在")
	}
	return
}

// 导出到CSV
func (df *DataFrame) Save2Csv(csvPath string) (err error) {
	dfString := df.GetDataFrameString(",")
	err = WriteDatas2File(csvPath, "\xEF\xBB\xBF", dfString)
	return
}

// 导出到xlsx
func (df *DataFrame) Save2Xlsx(xlsxPath string, numTitles ...string) (err error) {
	numTitleMap := make(map[string]bool)
	for _, title := range numTitles {
		numTitleMap[title] = true
	}
	file := xlsx.NewFile()
	sheet, err := file.AddSheet("Sheet1")
	if err != nil {
		fmt.Printf(err.Error())
		return
	}
	row := sheet.AddRow()
	for j := 0; j < df.cols; j++ {
		cell := row.AddCell()
		cell.Value = df.titles[j]
	}
	for i := 0; i < df.rows; i++ {
		row := sheet.AddRow()
		for j := 0; j < df.cols; j++ {
			cell := row.AddCell()
			if numTitleMap[df.titles[j]] {
				cell.SetString(df.columns[j].GetStringAt(i))
			} else {
				switch df.columns[j].DataType() {
				case ODKINT:
					cell.SetInt64(df.columns[j].GetIntAt(i))
				case ODKFLOAT:
					cell.SetFloat(df.columns[j].GetFloatAt(i))
				case ODKDATE:
					cell.SetDate(time.Unix(df.columns[j].GetIntAt(i), 0))
				default:
					cell.Value = df.columns[j].GetStringAt(i)
				}
			}
		}
	}
	err = file.Save(xlsxPath)
	if err != nil {
		fmt.Println(xlsxPath, err)
		return
	}
	return
}

// 获取子表格
func (df *DataFrame) GetSubDataFrame(subIndex []int) (ret DataFrame, err error) {
	columns := make([]Column, df.cols)
	for i := range df.columns {
		columns[i] = df.columns[i].GetSubColumn(subIndex)
	}
	ret.InitDatas(columns...)
	ret.SetColumnTitles(df.titles...)
	return
}
func (df *DataFrame) GetSubDataFrames(subIndexs [][]int) (ret []DataFrame, err error) {
	ret = make([]DataFrame, len(subIndexs))
	for i, subIndex := range subIndexs {
		ret[i], err = df.GetSubDataFrame(subIndex)
		if err != nil {
			return
		}
	}
	return
}

// 获取子表格
func (df *DataFrame) GetSubDataFrameFromStartEndIndex(startIndex, endIndex int) (ret DataFrame, err error) {
	if endIndex > df.rows {
		err = errors.New("超出行")
		return
	}
	subIndex := make([]int, endIndex-startIndex)
	for i := startIndex; i < endIndex; i++ {
		subIndex[i-startIndex] = i
	}
	ret, err = df.GetSubDataFrame(subIndex)
	return
}

// 分组
func (df *DataFrame) GroupBy(title string) (ret []DataFrame, err error) {
	if !df.Exist(title) {
		err = errors.New("此列不存在")
		return
	}
	column := df.GetColumn(title)
	_, subIndexs := column.Group()
	ret, err = df.GetSubDataFrames(subIndexs)
	return
}

// 年月分组
func (df *DataFrame) GroupByYearMonth(title string) (ret []DataFrame, err error) {
	if !df.Exist(title) {
		err = errors.New("此列不存在")
		return
	}

	column := df.GetColumn(title)
	if column.DataType() != ODKDATE {
		err = errors.New("此列无法按年月分组")
		return
	}
	_, subIndexs := column.GroupByYearMonth()
	ret, err = df.GetSubDataFrames(subIndexs)
	return
}
func (df *DataFrame) GroupByYear(title string) (ret []DataFrame, err error) {
	if !df.Exist(title) {
		err = errors.New("此列不存在")
		return
	}

	column := df.GetColumn(title)
	if column.DataType() != ODKDATE {
		err = errors.New("此列无法按年分组")
		return
	}
	_, subIndexs := column.GroupByYear()
	ret, err = df.GetSubDataFrames(subIndexs)
	return
}

// 后添加
func (df *DataFrame) Append(adf DataFrame, needEqCol bool) (ret DataFrame, err error) {
	columnTitles := make(map[string]int)
	columnDataType := make(map[string]int)
	var titles []string
	iCol := 0
	for _, title := range df.titles {
		if _, ok := columnTitles[title]; !ok {
			columnTitles[title] = iCol
			columnDataType[title] = df.GetColumn(title).DataType()
			titles = append(titles, title)
			iCol++
		}
	}
	for _, title := range adf.titles {
		if _, ok := columnTitles[title]; !ok {
			columnTitles[title] = iCol
			columnDataType[title] = adf.GetColumn(title).DataType()
			titles = append(titles, title)
			iCol++
		}
	}
	cols := len(columnTitles)
	if needEqCol {
		if cols != df.cols || cols != adf.cols {
			err = errors.New("数据列不同")
			return
		}
	}

	columns := make([]Column, cols)
	for i, title := range titles {
		var col1, col2 Column
		dataType := columnDataType[title]
		if df.Exist(title) {
			col1 = df.GetColumn(title)
		} else {
			col1 = NewEmptyColumn(dataType, df.rows)
		}
		if adf.Exist(title) {
			col2 = adf.GetColumn(title)
		} else {
			col2 = NewEmptyColumn(dataType, adf.rows)
		}
		columns[i], err = col1.Append(col2)
		if err != nil {
			return
		}
	}
	ret.InitDatas(columns...)
	ret.SetColumnTitles(titles...)
	return

}

// 合并多个数据表
func Concat(dfs ...DataFrame) (ret DataFrame, err error) {
	if len(dfs) == 0 {
		err = errors.New("未传入数据表")
		return
	}
	titles := dfs[0].GetTitles()
	size := 0
	for _, df := range dfs {
		if len(titles) != df.cols {
			err = errors.New("数据表头长度不一致")
			return
		}
		for j, title := range df.GetTitles() {
			if title != titles[j] {
				err = errors.New("数据表头不统一")
				return
			}

		}
		size += df.rows
	}
	colDatas := make([][]string, len(titles))
	for i := range titles {
		colDatas[i] = make([]string, size)
	}
	iC := 0
	for _, df := range dfs {
		for i, col := range df.columns {
			for j := 0; j < df.rows; j++ {
				colDatas[i][iC+j] = col.GetStringAt(j)
			}

		}
		iC += df.rows
	}

	cols := make([]Column, len(titles))
	for i, colData := range colDatas {
		cols[i] = NewStringColumn(colData...)
	}
	ret, err = NewDataFrame(cols...)
	if err != nil {
		return
	}
	err = ret.SetColumnTitles(titles...)
	return
}

// 合并 交集
func (df *DataFrame) InterMerge(adf DataFrame, title string) (ret DataFrame, err error) {
	if !df.Exist(title) {
		err = errors.New("src1此列不存在")
		return
	}
	if !adf.Exist(title) {
		err = errors.New("src2此列不存在")
		return
	}
	// 先排序再操作
	sortDf1, err := df.SortBy(title)
	if err != nil {
		return
	}
	sortDf2, err := adf.SortBy(title)
	if err != nil {
		return
	}
	_, interIndex1, _, interIndex2, _, err := sortDf1.GetColumn(title).GetInterIndex(sortDf2.GetColumn(title))
	if err != nil {
		return
	}

	columnTitles := make(map[string]int)
	columnDataType := make(map[string]int)
	var titles []string
	iCol := 0
	for _, title := range df.titles {
		if _, ok := columnTitles[title]; !ok {
			columnTitles[title] = iCol
			columnDataType[title] = df.GetColumn(title).DataType()
			titles = append(titles, title)
			iCol++
		}
	}
	for _, title := range adf.titles {
		if _, ok := columnTitles[title]; !ok {
			columnTitles[title] = iCol
			columnDataType[title] = adf.GetColumn(title).DataType()
			titles = append(titles, title)
			iCol++
		}
	}

	cols := len(columnTitles)
	columns := make([]Column, cols)
	df1, err := sortDf1.GetSubDataFrame(interIndex1)
	if err != nil {
		return
	}
	df2, err := sortDf2.GetSubDataFrame(interIndex2)
	if err != nil {
		return
	}

	for i, title := range titles {
		if df1.Exist(title) {
			columns[i] = df1.GetColumn(title)
		} else {
			columns[i] = df2.GetColumn(title)
		}
	}
	ret.InitDatas(columns...)
	ret.SetColumnTitles(titles...)
	return
}

// 合并 并集
func (df *DataFrame) OuterMerge(adf DataFrame, title string) (ret DataFrame, err error) {
	if !df.Exist(title) || !adf.Exist(title) {
		err = errors.New("此列不存在")
		return
	}
	// 先排序再操作
	sortDf1, err := df.SortBy(title)
	if err != nil {
		return
	}
	sortDf2, err := adf.SortBy(title)
	if err != nil {
		return
	}
	log.Println(sortDf1.rows, sortDf2.rows)
	_, interIndex1, outerIndex1, interIndex2, outerIndex2, err := sortDf1.GetColumn(title).GetInterIndex(sortDf2.GetColumn(title))
	if err != nil {
		return
	}
	columnTitles := make(map[string]int)
	columnDataType := make(map[string]int)
	var titles []string
	iCol := 0
	for _, title := range df.titles {
		if _, ok := columnTitles[title]; !ok {
			columnTitles[title] = iCol
			columnDataType[title] = df.GetColumn(title).DataType()
			titles = append(titles, title)
			iCol++
		}
	}
	for _, title := range adf.titles {
		if _, ok := columnTitles[title]; !ok {
			columnTitles[title] = iCol
			columnDataType[title] = adf.GetColumn(title).DataType()
			titles = append(titles, title)
			iCol++
		}
	}
	cols := len(columnTitles)
	columns := make([]Column, cols)
	df1, err := sortDf1.GetSubDataFrame(interIndex1)
	if err != nil {
		return
	}
	df2, err := sortDf2.GetSubDataFrame(interIndex2)
	if err != nil {
		return
	}
	outerDf1, err := sortDf1.GetSubDataFrame(outerIndex1)
	outerDf2, err := sortDf2.GetSubDataFrame(outerIndex2)
	for i, title := range titles {
		dataType := columnDataType[title]
		if df1.Exist(title) {
			columns[i] = df1.GetColumn(title)
		} else {
			columns[i] = df2.GetColumn(title)
		}
		if outerDf1.Exist(title) {
			columns[i], err = columns[i].Append(outerDf1.GetColumn(title))
			if err != nil {
				return
			}
		} else {
			columns[i], err = columns[i].Append(NewEmptyColumn(dataType, outerDf1.rows))
			if err != nil {
				return
			}
		}
		if outerDf2.Exist(title) {
			columns[i], err = columns[i].Append(outerDf2.GetColumn(title))
			if err != nil {
				return
			}
		} else {
			columns[i], err = columns[i].Append(NewEmptyColumn(dataType, outerDf2.rows))
			if err != nil {
				return
			}
		}
	}
	ret.InitDatas(columns...)
	ret.SetColumnTitles(titles...)
	return
}

// 排序
func (df *DataFrame) SortBy(title string) (ret DataFrame, err error) {
	if !df.Exist(title) {
		err = errors.New("此列不存在")
		return
	}
	_, sortIndex := df.GetColumn(title).Sort()
	ret, err = df.GetSubDataFrame(sortIndex)
	return
}

// 设置某一列数据
func (df *DataFrame) SetColumn(title string, col Column) (err error) {
	if !df.Exist(title) {
		err = errors.New("此列不存在")
		return
	}
	df.columns[df.columnTitles[title]] = col
	return
}

// 添加一列数据
func (df *DataFrame) AddColumn(title string, col Column) (err error) {
	if df.Exist(title) {
		err = errors.New("已存在此列数据，请换个表头")
		return
	}
	df.columns = append(df.columns, col)
	df.titles = append(df.titles, title)
	df.columnTitles[title] = df.cols
	df.cols++
	return
}

// 筛选目标数据
func (df *DataFrame) Filter(title string, filterType int, value interface{}) (ret DataFrame, err error) {
	if !df.Exist(title) {
		err = errors.New("此列不存在")
		return
	}
	_, filterIndex, err := df.GetColumn(title).AdvancedFilter(filterType, value)
	if err != nil {
		return
	}
	ret, err = df.GetSubDataFrame(filterIndex)
	return
}

// 获取表头
func (df *DataFrame) GetTitles() []string {
	return df.titles
}

// 行数
func (df *DataFrame) Rows() int {
	return df.rows
}

// 列数
func (df *DataFrame) Cols() int {
	return df.cols
}

// 提取特定数据 需要传入一个 Column
func (df *DataFrame) ExtractSubDataframes(tarCol Column, title string) (interCol Column, subDfs []DataFrame, tDf DataFrame, err error) {
	if !df.Exist(title) {
		err = errors.New("此列不存在")
		return
	}
	uniqueCol, _ := tarCol.DropDuplicate()
	interCol, _, _, _, _, err = uniqueCol.GetInterIndex(df.GetColumn(title))
	if err != nil {
		return
	}
	groupedCols, groupIndexs := df.GetColumn(title).Group()
	groupMap := make(map[string]int)
	for i, col := range groupedCols {
		groupMap[col.GetStringAt(0)] = i
	}
	var tSubIndex []int

	subDfs = make([]DataFrame, interCol.Len())
	for i := 0; i < interCol.Len(); i++ {
		subIndex := groupIndexs[groupMap[interCol.GetStringAt(i)]]
		subDfs[i], err = df.GetSubDataFrame(subIndex)
		if err != nil {
			return
		}
		tSubIndex = append(tSubIndex, subIndex...)
	}

	tDf, err = df.GetSubDataFrame(tSubIndex)
	return
}

// 拷贝
func (df *DataFrame) Copy() DataFrame {
	copyDf, _ := NewDataFrame(df.columns...)
	copyDf.SetColumnTitles(df.GetTitles()...)
	return copyDf
}

// 转化为vega数据表
func (df *DataFrame) Convert2VegaData(xTitle string, yTitles ...string) (ret DataFrame, err error) {
	if !df.Exist(xTitle) {
		err = errors.New(xTitle + "不存在")
		return
	}

	// dfs := make([]DataFrame, 0, len(yTitles))

	for i, yTitle := range yTitles {
		if !df.Exist(yTitle) {
			err = errors.New(yTitle + "不存在")
			return
		}
		colorCol := make([]string, 0, df.Rows())
		for ii := 0; ii < df.Rows(); ii++ {
			colorCol = append(colorCol, yTitle)
		}
		iDf, ierr := NewDataFrame(df.GetColumn(xTitle), df.GetColumn(yTitle), NewStringColumn(colorCol...))
		err = ierr
		if err != nil {
			return
		}
		iDf.SetColumnTitles("X", "Y", "COLOR")
		// dfs = append(dfs, iDf)

		if i == 0 {
			ret = iDf
		} else {
			ret, err = ret.Append(iDf, true)
			if err != nil {

				return
			}
		}
	}

	return
}

package odktabdata

import (
	"errors"
	"fmt"
	"log"
	"math"

	"sort"
)

func (c *DateColumn) InitData(datas ...interface{}) (err error) {
	c.dataLen = len(datas)
	c.data = make([]OdkDateTime, c.dataLen)
	for i, value := range datas {
		err = c.data[i].InitFormString(fmt.Sprintf("%v", value))
		if err != nil {
			return
		}
	}
	return
}
func (c *DateColumn) DataType() int {
	return ODKDATE
}
func (c *DateColumn) Len() int {
	return c.dataLen
}
func (c *DateColumn) Swap(a, b int) {
	c.data[b], c.data[a] = c.data[a], c.data[b]
	c.index[b], c.index[a] = c.index[a], c.index[b]
}
func (c *DateColumn) Less(a, b int) bool {
	return c.data[a].Timestamp < c.data[b].Timestamp
}

func (c *DateColumn) Index() []int {
	return c.index
}
func (c *DateColumn) Copy() Column {
	data := make([]OdkDateTime, c.dataLen)
	for i, value := range c.data {
		data[i] = value
	}
	return &DateColumn{
		data:    data,
		dataLen: c.dataLen,
		index:   c.index,
	}
}
func (c *DateColumn) Sort() (Column, []int) {
	c.index = make([]int, c.dataLen)
	for i := range c.data {
		c.index[i] = i
	}
	retCol := c.Copy()
	sort.Sort(retCol)
	return retCol, retCol.Index()
}

func (c *DateColumn) SortInplace() {
	sort.Sort(c)
}
func (c *DateColumn) GetStringAt(index int) string {
	if index < c.dataLen {
		return c.data[index].DateStr
	}
	return ""
}
func (c *DateColumn) GetFloatAt(index int) float64 {
	if index < c.dataLen {
		return float64(c.data[index].Timestamp)

	}
	return 0.0
}
func (c *DateColumn) GetIntAt(index int) int64 {
	if index < c.dataLen {
		return (c.data[index].Timestamp)

	}
	return 0
}
func (c *DateColumn) GetDateAt(index int) OdkDateTime {
	if index < c.dataLen {
		return c.data[index]
	}
	return OdkDateTime{}
}
func (c *DateColumn) GetInterfaceAt(index int) interface{} {
	if index < c.dataLen {
		return c.data[index]
	}
	return nil
}
func (c *DateColumn) SetStringAt(index int, data string) {
	if index < c.dataLen {
		c.data[index].InitFormString(data)
	}
}
func (c *DateColumn) SetIntAt(index int, data int64) {
	return
}
func (c *DateColumn) SetFloatAt(index int, data float64) {
	return
}

func (c *DateColumn) Group() ([]Column, [][]int) {
	groupIndexs := make(map[string]*[]int)
	for i, value := range c.data {
		groupDataP := groupIndexs[value.DateStr]
		if groupDataP == nil {
			groupIndexs[value.DateStr] = &[]int{i}
		} else {
			*groupDataP = append(*groupDataP, i)
		}
	}
	subCols := make([]Column, len(groupIndexs))
	subColIndexs := make([][]int, len(groupIndexs))
	i := 0
	for _, subIndex := range groupIndexs {
		subCols[i] = c.GetSubColumn(*subIndex)
		subColIndexs[i] = *subIndex
		i++
	}

	return subCols, subColIndexs
}
func (c *DateColumn) GroupByYearMonth() ([]Column, [][]int) {
	groupIndexs := make(map[string]*[]int)
	for i, value := range c.data {
		groupDataP := groupIndexs[value.YearMonth]
		if groupDataP == nil {
			groupIndexs[value.YearMonth] = &[]int{i}
		} else {
			*groupDataP = append(*groupDataP, i)
		}
	}
	subCols := make([]Column, len(groupIndexs))
	subColIndexs := make([][]int, len(groupIndexs))
	i := 0
	for _, subIndex := range groupIndexs {
		subCols[i] = c.GetSubColumn(*subIndex)
		subColIndexs[i] = *subIndex
		i++
	}

	return subCols, subColIndexs
}
func (c *DateColumn) GroupByYear() ([]Column, [][]int) {
	groupIndexs := make(map[int]*[]int)
	for i, value := range c.data {
		groupDataP := groupIndexs[value.Year]
		if groupDataP == nil {
			groupIndexs[value.Year] = &[]int{i}
		} else {
			*groupDataP = append(*groupDataP, i)
		}
	}
	subCols := make([]Column, len(groupIndexs))
	subColIndexs := make([][]int, len(groupIndexs))
	i := 0
	for _, subIndex := range groupIndexs {
		subCols[i] = c.GetSubColumn(*subIndex)
		subColIndexs[i] = *subIndex
		i++
	}

	return subCols, subColIndexs
}
func (c *DateColumn) GetSubColumns(subColIndexs [][]int) []Column {
	subCols := make([]Column, len(subColIndexs))
	for i, subIndex := range subColIndexs {
		subCols[i] = c.GetSubColumn(subIndex)
	}
	return subCols
}
func (c *DateColumn) GetSubColumn(subIndex []int) Column {
	data := make([]OdkDateTime, len(subIndex))
	for i, index := range subIndex {
		data[i] = c.data[index]
	}
	return &DateColumn{
		data:    data,
		dataLen: len(data),
	}
}
func (c *DateColumn) Conv2DateColumn() (Column, error) {
	return c, nil
}
func (c *DateColumn) Conv2IntColumn() (Column, error) {
	data := make([]int64, c.dataLen)

	for i, value := range c.data {
		data[i] = value.Timestamp
	}
	return &IntColumn{
		data:    data,
		dataLen: c.dataLen,
	}, nil

}
func (c *DateColumn) Conv2FloatColumn() (Column, error) {
	data := make([]float64, c.dataLen)

	for i, value := range c.data {
		data[i] = float64(value.Timestamp)
	}
	return &FloatColumn{
		data:    data,
		dataLen: c.dataLen,
	}, nil
}

func (c *DateColumn) Conv2StringColumn() (Column, error) {
	data := make([]string, c.dataLen)

	for i, value := range c.data {
		if value.rawStr == "" {
			data[i] = (value.DateStr)
		} else {
			data[i] = (value.rawStr)
		}

	}
	return &StringColumn{
		data:    data,
		dataLen: c.dataLen,
	}, nil
}

func (c *DateColumn) Filter(values ...interface{}) ([]Column, [][]int, error) {

	groupIndexs := make(map[string]*[]int)
	for _, value := range values {
		temp, ok := value.(OdkDateTime)
		if !ok {
			err := errors.New("数据类型不正确,需要Date数据")
			return nil, nil, err
		}
		groupIndexs[temp.DateStr] = &[]int{}
	}
	for i, value := range c.data {
		groupDataP, ok := groupIndexs[value.DateStr]
		if ok {
			*groupDataP = append(*groupDataP, i)
		}
	}
	subCols := make([]Column, len(groupIndexs))
	subColIndexs := make([][]int, len(groupIndexs))
	i := 0
	for _, subIndex := range groupIndexs {
		subCols[i] = c.GetSubColumn(*subIndex)
		subColIndexs[i] = *subIndex
		i++
	}

	return subCols, subColIndexs, nil
}
func (c *DateColumn) AdvancedFilter(operateSymbol int, value interface{}) (tarCol Column, filterIndex []int, err error) {
	tarValue, ok := value.(OdkDateTime)
	if !ok {
		err = errors.New("筛选值类型不正确")
		return
	}

	for i, v := range c.data {
		switch operateSymbol {
		case FILTER_EQ:
			if v.Timestamp == tarValue.Timestamp {
				filterIndex = append(filterIndex, i)
			}
		case FILTER_NOTEQ:
			if v.Timestamp != tarValue.Timestamp {
				filterIndex = append(filterIndex, i)
			}
		case FILTER_GT:
			if v.Timestamp > tarValue.Timestamp {
				filterIndex = append(filterIndex, i)
			}
		case FILTER_GTE:
			if v.Timestamp >= tarValue.Timestamp {
				filterIndex = append(filterIndex, i)
			}
		case FILTER_LT:
			if v.Timestamp < tarValue.Timestamp {
				filterIndex = append(filterIndex, i)
			}
		case FILTER_LTE:
			if v.Timestamp <= tarValue.Timestamp {
				filterIndex = append(filterIndex, i)
			}
		default:
			err = errors.New("筛选运算不支持")
			return
		}
	}
	tarCol = c.GetSubColumn(filterIndex)
	return
}
func (c *DateColumn) DataMainFactors() (min float64, max float64, mean float64, sum float64, err error) {
	temmax, temmin, temsum := -int64(math.MaxInt64), int64(math.MaxInt64), int64(0)

	for _, value := range c.data {
		if temmax < value.Timestamp {
			temmax = value.Timestamp
		}
		if temmin > value.Timestamp {
			temmin = value.Timestamp
		}
		temsum += value.Timestamp
	}
	min, max, mean, sum = float64(temmin), float64(temmax), float64(temsum)/float64(c.dataLen), float64(temsum)
	return
}
func (c *DateColumn) ColumnOperate(operateSymbol int) Column {
	log.Fatal("不支持的运算类型")
	return c
}
func (c *DateColumn) ColumnsOperate(col Column, operateSymbol int) Column {
	log.Fatal("不支持的运算类型")
	return c
}
func (c *DateColumn) ColumnNumOperate(num float64, operateSymbol int) Column {
	log.Fatal("不支持的运算类型")
	return c
}
func (c *DateColumn) NumColumnOperate(num float64, operateSymbol int) Column {
	log.Fatal("不支持的运算类型")
	return c
}
func (c *DateColumn) Append(col Column) (Column, error) {
	if col.DataType() != c.DataType() {
		return c, errors.New("数据类型不一致")
	}
	dataLen := c.dataLen + col.Len()
	data := make([]OdkDateTime, dataLen)
	for i, value := range c.data {
		data[i] = value
	}
	for i := c.dataLen; i < dataLen; i++ {
		data[i] = col.GetDateAt(i - c.dataLen)
	}
	return &DateColumn{
		data:    data,
		dataLen: dataLen,
	}, nil
}
func (c *DateColumn) GetInterIndex(col Column) (inter Column, interIndexs1 []int, outerIndexs1 []int, interIndexs2 []int, outerIndexs2 []int, err error) {
	if col.DataType() != c.DataType() {
		err = errors.New("数据类型不一致")
		return
	}
	dataMap1 := make(map[string]bool)
	for _, value := range c.data {
		dataMap1[value.DateStr] = true
	}
	interMap := make(map[string]bool)
	for i := 0; i < col.Len(); i++ {
		if dataMap1[col.GetStringAt(i)] {
			interIndexs2 = append(interIndexs2, i)
			interMap[col.GetStringAt(i)] = true
		} else {
			outerIndexs2 = append(outerIndexs2, i)
		}
	}
	for i, value := range c.data {
		if interMap[value.DateStr] {
			interIndexs1 = append(interIndexs1, i)
		} else {
			outerIndexs1 = append(outerIndexs1, i)
		}
	}
	inter = c.GetSubColumn(interIndexs1)
	return
}
func (c *DateColumn) DropDuplicate() (ret Column, subIndexs []int) {
	uniqueMap := make(map[string]bool)

	for i, value := range c.data {
		if !uniqueMap[value.DateStr] {
			uniqueMap[value.DateStr] = true
			subIndexs = append(subIndexs, i)
		}
	}
	ret = c.GetSubColumn(subIndexs)
	return
}
func (c *DateColumn) Grade(steps ...interface{}) (gradedCol, gradeNumCol, gradedValueCol Column, err error) {
	newSteps := make([]OdkDateTime, len(steps))
	for i, value := range steps {
		intValue, ok := value.(OdkDateTime)
		if ok {
			newSteps[i] = (intValue)
		} else {
			err = errors.New("初始化数据失败")
			return
		}
	}
	newSteps = append([]OdkDateTime{NewDateFromString("1900-1-1")}, newSteps...)
	newSteps = append(newSteps, NewDateFromString("5000-1-1"))
	gradeLimit := make([][]int64, len(newSteps)-1)
	gradeLimitDate := make([][]OdkDateTime, len(newSteps)-1)
	gradeNum := len(gradeLimit)
	for i := 0; i < gradeNum; i++ {
		gradeLimit[i] = []int64{newSteps[i].Timestamp, newSteps[i+1].Timestamp}
		gradeLimitDate[i] = []OdkDateTime{newSteps[i], newSteps[i+1]}
	}
	gradeData := make([]int64, c.dataLen)
	gradeValue := make([]OdkDateTime, c.dataLen)
	gradeDataNum := make(map[int64]int64)
	for i := 0; i < c.dataLen; i++ {
		for j := 0; j < gradeNum; j++ {
			if c.data[i].Timestamp >= gradeLimit[j][0] && c.data[i].Timestamp < gradeLimit[j][1] {
				gradeData[i] = int64(j)
				gradeValue[i] = gradeLimitDate[j][1]
				gradeDataNum[int64(j)]++
				break
			}
		}
	}
	gradeDataNumVec := make([]int64, c.dataLen)
	for i, value := range gradeData {
		gradeDataNumVec[i] = gradeDataNum[value]
	}
	gradedCol = &IntColumn{
		data:    gradeData,
		dataLen: c.dataLen,
	}
	gradedValueCol = &DateColumn{
		data:    gradeValue,
		dataLen: c.dataLen,
	}
	gradeNumCol = &IntColumn{
		data:    gradeDataNumVec,
		dataLen: c.dataLen,
	}
	return
}

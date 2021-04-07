package odktabdata

import (
	"errors"
	"fmt"
	"log"

	"sort"
	"strconv"
)

func (c *StringColumn) InitData(datas ...interface{}) (err error) {
	c.dataLen = len(datas)
	c.data = make([]string, c.dataLen)
	for i, value := range datas {
		c.data[i] = fmt.Sprintf("%v", value)
	}
	return
}
func (c *StringColumn) DataType() int {
	return ODKSTRING
}
func (c *StringColumn) Len() int {
	return c.dataLen
}
func (c *StringColumn) Swap(a, b int) {
	c.data[b], c.data[a] = c.data[a], c.data[b]
	c.index[b], c.index[a] = c.index[a], c.index[b]
}
func (c *StringColumn) Less(a, b int) bool {
	return c.data[a] < c.data[b]
}
func (c *StringColumn) Index() []int {
	return c.index
}
func (c *StringColumn) Copy() Column {
	data := make([]string, c.dataLen)
	for i, value := range c.data {
		data[i] = value
	}
	return &StringColumn{
		data:    data,
		dataLen: c.dataLen,
		index:   c.index,
	}
}
func (c *StringColumn) Sort() (Column, []int) {
	c.index = make([]int, c.dataLen)
	for i := range c.data {
		c.index[i] = i
	}
	retCol := c.Copy()
	sort.Sort(retCol)
	return retCol, retCol.Index()
}

func (c *StringColumn) SortInplace() {
	sort.Sort(c)
}
func (c *StringColumn) GetStringAt(index int) string {
	if index < c.dataLen {
		return c.data[index]
	}
	return ""
}
func (c *StringColumn) GetFloatAt(index int) float64 {
	if index < c.dataLen {
		value, err := strconv.ParseFloat(c.data[index], 64)
		if err != nil {
			return 0.0
		}
		return value
	}
	return 0.0
}
func (c *StringColumn) GetIntAt(index int) int64 {
	if index < c.dataLen {
		value, err := strconv.ParseInt(c.data[index], 10, 64)
		if err != nil {
			return 0
		}
		return value
	}
	return 0
}
func (c *StringColumn) GetDateAt(index int) OdkDateTime {
	return OdkDateTime{}
}
func (c *StringColumn) GetInterfaceAt(index int) interface{} {
	if index < c.dataLen {
		return c.data[index]
	}
	return nil
}
func (c *StringColumn) SetStringAt(index int, data string) {
	if index < c.dataLen {
		c.data[index] = data
	}
}
func (c *StringColumn) SetIntAt(index int, data int64) {
	if index < c.dataLen {
		c.data[index] = strconv.Itoa(int(data))
	}
}
func (c *StringColumn) SetFloatAt(index int, data float64) {
	if index < c.dataLen {
		c.data[index] = strconv.FormatFloat(data, 'f', -1, 64)
	}
}
func (c *StringColumn) Group() ([]Column, [][]int) {
	groupIndexs := make(map[string]*[]int)
	for i, value := range c.data {
		groupDataP := groupIndexs[value]
		if groupDataP == nil {
			groupIndexs[value] = &[]int{i}
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
func (c *StringColumn) GroupByYearMonth() ([]Column, [][]int) {
	return []Column{}, [][]int{}
}
func (c *StringColumn) GroupByYear() ([]Column, [][]int) {
	return []Column{}, [][]int{}
}
func (c *StringColumn) GetSubColumns(subColIndexs [][]int) []Column {
	subCols := make([]Column, len(subColIndexs))
	for i, subIndex := range subColIndexs {
		subCols[i] = c.GetSubColumn(subIndex)
	}
	return subCols
}
func (c *StringColumn) GetSubColumn(subIndex []int) Column {
	data := make([]string, len(subIndex))
	for i, index := range subIndex {
		data[i] = c.data[index]
	}
	return &StringColumn{
		data:    data,
		dataLen: len(data),
	}
}
func (c *StringColumn) Conv2DateColumn() (Column, error) {
	data := make([]OdkDateTime, c.dataLen)
	var err error
	for i, value := range c.data {
		err = data[i].InitFormString(value)
		if err != nil {
			return nil, err
		}
	}
	return &DateColumn{
		data:    data,
		dataLen: c.dataLen,
	}, nil
}
func (c *StringColumn) Conv2IntColumn() (Column, error) {
	data := make([]int64, c.dataLen)
	var err error
	for i, value := range c.data {
		data[i], err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, err
		}
	}
	return &IntColumn{
		data:    data,
		dataLen: c.dataLen,
	}, nil

}
func (c *StringColumn) Conv2FloatColumn() (Column, error) {
	data := make([]float64, c.dataLen)
	var err error
	for i, value := range c.data {
		data[i], err = strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, err
		}
	}
	return &FloatColumn{
		data:    data,
		dataLen: c.dataLen,
	}, nil
}

func (c *StringColumn) Conv2StringColumn() (Column, error) {
	return c, nil
}
func (c *StringColumn) Filter(values ...interface{}) ([]Column, [][]int, error) {
	groupIndexs := make(map[string]*[]int)
	for _, value := range values {
		temp, ok := value.(string)
		if !ok {
			err := errors.New("数据类型不正确,需要String数据")
			return nil, nil, err
		}
		groupIndexs[temp] = &[]int{}
	}
	for i, value := range c.data {

		groupDataP, ok := groupIndexs[value]
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
func (c *StringColumn) AdvancedFilter(operateSymbol int, value interface{}) (tarCol Column, filterIndex []int, err error) {
	tarValue, ok := value.(string)
	if !ok {
		err = errors.New("筛选值类型不正确")
		return
	}

	for i, v := range c.data {
		switch operateSymbol {
		case FILTER_EQ:
			if v == tarValue {
				filterIndex = append(filterIndex, i)
			}
		case FILTER_NOTEQ:
			if v != tarValue {
				filterIndex = append(filterIndex, i)
			}
		case FILTER_GT:
			if v > tarValue {
				filterIndex = append(filterIndex, i)
			}
		case FILTER_GTE:
			if v >= tarValue {
				filterIndex = append(filterIndex, i)
			}
		case FILTER_LT:
			if v < tarValue {
				filterIndex = append(filterIndex, i)
			}
		case FILTER_LTE:
			if v <= tarValue {
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
func (c *StringColumn) DataMainFactors() (min float64, max float64, mean float64, sum float64, err error) {
	err = errors.New("string类型不能进行运算")
	return
}
func (c *StringColumn) ColumnOperate(operateSymbol int) Column {
	log.Fatal("不支持的运算类型")
	return c
}
func (c *StringColumn) ColumnsOperate(col Column, operateSymbol int) Column {
	log.Fatal("不支持的运算类型")
	return c
}
func (c *StringColumn) ColumnNumOperate(num float64, operateSymbol int) Column {
	log.Fatal("不支持的运算类型")
	return c
}
func (c *StringColumn) NumColumnOperate(num float64, operateSymbol int) Column {
	log.Fatal("不支持的运算类型")
	return c
}
func (c *StringColumn) Append(col Column) (Column, error) {
	if col.DataType() != c.DataType() {
		return c, errors.New("数据类型不一致")
	}
	dataLen := c.dataLen + col.Len()
	data := make([]string, dataLen)
	for i, value := range c.data {
		data[i] = value
	}
	for i := c.dataLen; i < dataLen; i++ {
		data[i] = col.GetStringAt(i - c.dataLen)
	}
	return &StringColumn{
		data:    data,
		dataLen: dataLen,
	}, nil
}
func (c *StringColumn) GetInterIndex(col Column) (inter Column, interIndexs1 []int, outerIndexs1 []int, interIndexs2 []int, outerIndexs2 []int, err error) {
	if col.DataType() != c.DataType() {
		err = errors.New("数据类型不一致")
		return
	}
	dataMap1 := make(map[string]int)
	for i, value := range c.data {
		dataMap1[value] = i + 1
	}

	interMap := make(map[string]bool)
	for i := 0; i < col.Len(); i++ {
		if dataMap1[col.GetStringAt(i)] > 0 {
			interIndexs2 = append(interIndexs2, i)
			interMap[col.GetStringAt(i)] = true
		} else {
			outerIndexs2 = append(outerIndexs2, i)
		}
	}
	for i, value := range c.data {
		if interMap[value] {
			interIndexs1 = append(interIndexs1, i)
		} else {
			outerIndexs1 = append(outerIndexs1, i)
		}
	}
	inter = c.GetSubColumn(interIndexs1)
	return
}
func (c *StringColumn) DropDuplicate() (ret Column, subIndexs []int) {
	uniqueMap := make(map[string]bool)

	for i, value := range c.data {
		if !uniqueMap[value] {
			uniqueMap[value] = true
			subIndexs = append(subIndexs, i)
		}
	}
	ret = c.GetSubColumn(subIndexs)
	return
}

func (c *StringColumn) Grade(steps ...interface{}) (gradedCol, gradeNumCol, gradedValueCol Column, err error) {
	err = errors.New("String此类型无法分级")
	return
}

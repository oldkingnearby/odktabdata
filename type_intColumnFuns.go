package odktabdata

import (
	"errors"
	"fmt"

	// "fmt"
	"log"
	"math"

	"sort"
	"strconv"
)

func (c *IntColumn) InitData(datas ...interface{}) (err error) {
	c.dataLen = len(datas)
	c.data = make([]int64, c.dataLen)
	for i, value := range datas {
		intValue, ok := value.(int)
		if ok {
			c.data[i] = int64(intValue)
		} else {
			intValue1, ok1 := value.(int64)
			if ok1 {
				c.data[i] = intValue1
			} else {
				err = errors.New("初始化数据失败")
				return
			}
		}
	}
	return
}
func (c *IntColumn) DataType() int {
	return ODKINT
}
func (c *IntColumn) Len() int {
	return c.dataLen
}
func (c *IntColumn) Swap(a, b int) {
	c.data[b], c.data[a] = c.data[a], c.data[b]
	c.index[b], c.index[a] = c.index[a], c.index[b]
}
func (c *IntColumn) Less(a, b int) bool {
	return c.data[a] < c.data[b]
}
func (c *IntColumn) Index() []int {
	return c.index
}
func (c *IntColumn) Copy() Column {
	data := make([]int64, c.dataLen)
	for i, value := range c.data {
		data[i] = value
	}
	return &IntColumn{
		data:    data,
		dataLen: c.dataLen,
		index:   c.index,
	}

}
func (c *IntColumn) Sort() (Column, []int) {
	c.index = make([]int, c.dataLen)
	for i := range c.data {
		c.index[i] = i
	}
	retCol := c.Copy()
	sort.Sort(retCol)
	return retCol, retCol.Index()
}

func (c *IntColumn) SortInplace() {
	sort.Sort(c)
}
func (c *IntColumn) GetStringAt(index int) string {
	if index < c.dataLen {
		return strconv.Itoa(int(c.data[index]))
	}
	return ""
}
func (c *IntColumn) GetFloatAt(index int) float64 {
	if index < c.dataLen {
		return float64(c.data[index])
	}
	return 0.0
}
func (c *IntColumn) GetIntAt(index int) int64 {
	if index < c.dataLen {
		return (c.data[index])
	}
	return 0
}
func (c *IntColumn) GetDateAt(index int) OdkDateTime {
	return OdkDateTime{}
}
func (c *IntColumn) GetInterfaceAt(index int) interface{} {
	if index < c.dataLen {
		return c.data[index]
	}
	return nil
}
func (c *IntColumn) SetStringAt(index int, data string) {
	if index < c.dataLen {
		value, err := strconv.ParseInt(data, 10, 64)
		if err != nil {
			c.data[index] = value
		}
	}
}
func (c *IntColumn) SetIntAt(index int, data int64) {
	if index < c.dataLen {
		c.data[index] = data
	}
}
func (c *IntColumn) SetFloatAt(index int, data float64) {
	if index < c.dataLen {
		c.data[index] = int64(data)
	}
}
func (c *IntColumn) Group() ([]Column, [][]int) {
	groupIndexs := make(map[int64]*[]int)
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
func (c *IntColumn) GroupByYearMonth() ([]Column, [][]int) {
	return []Column{}, [][]int{}
}
func (c *IntColumn) GroupByYear() ([]Column, [][]int) {
	return []Column{}, [][]int{}
}
func (c *IntColumn) GetSubColumns(subColIndexs [][]int) []Column {
	subCols := make([]Column, len(subColIndexs))
	for i, subIndex := range subColIndexs {
		subCols[i] = c.GetSubColumn(subIndex)
	}
	return subCols
}
func (c *IntColumn) GetSubColumn(subIndex []int) Column {
	data := make([]int64, len(subIndex))
	for i, index := range subIndex {
		data[i] = c.data[index]
	}
	return &IntColumn{
		data:    data,
		dataLen: len(data),
	}
}
func (c *IntColumn) Conv2DateColumn() (Column, error) {
	data := make([]OdkDateTime, c.dataLen)
	var err error
	for i, value := range c.data {
		err = data[i].InitFormString(fmt.Sprintf("%v", value))
		if err != nil {
			return nil, err
		}
	}
	return &DateColumn{
		data:    data,
		dataLen: c.dataLen,
	}, nil
}
func (c *IntColumn) Conv2IntColumn() (Column, error) {
	return c, nil
}
func (c *IntColumn) Conv2FloatColumn() (Column, error) {
	data := make([]float64, c.dataLen)
	for i, value := range c.data {
		data[i] = float64(value)
	}
	return &FloatColumn{
		data:    data,
		dataLen: c.dataLen,
	}, nil
}

func (c *IntColumn) Conv2StringColumn() (Column, error) {
	data := make([]string, c.dataLen)
	for i, value := range c.data {
		data[i] = strconv.Itoa(int(value))
	}
	return &StringColumn{
		data:    data,
		dataLen: c.dataLen,
	}, nil
	return c, nil
}

func (c *IntColumn) Filter(values ...interface{}) ([]Column, [][]int, error) {

	groupIndexs := make(map[int64]*[]int)
	for _, value := range values {
		temp, ok := value.(int64)
		if !ok {
			err := errors.New("数据类型不正确,需要Int64数据")
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
func (c *IntColumn) AdvancedFilter(operateSymbol int, value interface{}) (tarCol Column, filterIndex []int, err error) {
	tarValue, ok := value.(int64)
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
func (c *IntColumn) DataMainFactors() (min float64, max float64, mean float64, sum float64, err error) {
	temmax, temmin, temsum := -int64(math.MaxInt64), int64(math.MaxInt64), int64(0)
	for _, value := range c.data {
		if temmax < value {
			temmax = value
		}
		if temmin > value {
			temmin = value
		}
		temsum += value
	}
	min, max, mean, sum = float64(temmin), float64(temmax), float64(temsum)/float64(c.dataLen), float64(temsum)
	return
}
func (c *IntColumn) ColumnOperate(operateSymbol int) Column {
	switch operateSymbol {
	case MATH_LOG:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = math.Log(c.GetFloatAt(i))
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	case MATH_LOG2:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = math.Log2(c.GetFloatAt(i))
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	case MATH_LOG10:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = math.Log10(c.GetFloatAt(i))
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	case MATH_CUMSUM:
		data := make([]float64, c.dataLen)
		sum := 0.0
		for i := range c.data {
			sum += c.GetFloatAt(i)
			data[i] = sum
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	case MATH_EXP:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = math.Exp(c.GetFloatAt(i))
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	default:
		log.Fatal("不支持的运算符")
		return c
	}
	return c
}
func (c *IntColumn) ColumnsOperate(col Column, operateSymbol int) Column {
	switch operateSymbol {
	case MATH_ADD:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = c.GetFloatAt(i) + col.GetFloatAt(i)
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	case MATH_MINUS:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = c.GetFloatAt(i) - col.GetFloatAt(i)
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	case MATH_MULTIPLY:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = c.GetFloatAt(i) * col.GetFloatAt(i)
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	case MATH_DIVIDE:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = c.GetFloatAt(i) / col.GetFloatAt(i)
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	case MATH_POWER:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = math.Pow(c.GetFloatAt(i), col.GetFloatAt(i))
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}

	default:
		log.Fatal("不支持的运算符")
		return c
	}
	return c
}
func (c *IntColumn) ColumnNumOperate(num float64, operateSymbol int) Column {
	switch operateSymbol {
	case MATH_ADD:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = c.GetFloatAt(i) + num
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	case MATH_MINUS:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = c.GetFloatAt(i) - num
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	case MATH_MULTIPLY:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = c.GetFloatAt(i) * num
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	case MATH_DIVIDE:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = c.GetFloatAt(i) / num
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	case MATH_POWER:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = math.Pow(c.GetFloatAt(i), num)
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}

	default:
		log.Fatal("不支持的运算符")
		return c
	}
	return c
}
func (c *IntColumn) NumColumnOperate(num float64, operateSymbol int) Column {
	switch operateSymbol {
	case MATH_ADD:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = c.GetFloatAt(i) + num
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	case MATH_MINUS:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = num - c.GetFloatAt(i)
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	case MATH_MULTIPLY:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = c.GetFloatAt(i) * num
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	case MATH_DIVIDE:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = num / c.GetFloatAt(i)
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}
	case MATH_POWER:
		data := make([]float64, c.dataLen)
		for i := range c.data {
			data[i] = math.Pow(num, c.GetFloatAt(i))
		}
		return &FloatColumn{
			data:    data,
			dataLen: c.dataLen,
		}

	default:
		log.Fatal("不支持的运算符")
		return c
	}
	return c
}
func (c *IntColumn) Append(col Column) (Column, error) {
	if col.DataType() != c.DataType() {
		return c, errors.New("数据类型不一致")
	}
	dataLen := c.dataLen + col.Len()
	data := make([]int64, dataLen)
	for i, value := range c.data {
		data[i] = value
	}
	for i := c.dataLen; i < dataLen; i++ {
		data[i] = col.GetIntAt(i - c.dataLen)
	}
	return &IntColumn{
		data:    data,
		dataLen: dataLen,
	}, nil
}
func (c *IntColumn) GetInterIndex(col Column) (inter Column, interIndexs1 []int, outerIndexs1 []int, interIndexs2 []int, outerIndexs2 []int, err error) {
	if col.DataType() != c.DataType() {
		err = errors.New("数据类型不一致")
		return
	}
	dataMap1 := make(map[int64]bool)
	for _, value := range c.data {
		dataMap1[value] = true
	}
	interMap := make(map[int64]bool)
	for i := 0; i < col.Len(); i++ {
		if dataMap1[col.GetIntAt(i)] {
			interIndexs2 = append(interIndexs2, i)
			interMap[col.GetIntAt(i)] = true
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
func (c *IntColumn) DropDuplicate() (ret Column, subIndexs []int) {
	uniqueMap := make(map[int64]bool)

	for i, value := range c.data {
		if !uniqueMap[value] {
			uniqueMap[value] = true
			subIndexs = append(subIndexs, i)
		}
	}
	ret = c.GetSubColumn(subIndexs)
	return
}
func (c *IntColumn) Grade(steps ...interface{}) (gradedCol, gradeNumCol, gradedValueCol Column, err error) {
	newSteps := make([]int64, len(steps))
	for i, value := range steps {
		intValue, ok := value.(int)
		if ok {
			newSteps[i] = int64(intValue)
		} else {
			err = errors.New("初始化数据失败")
			return
		}
	}
	newSteps = append([]int64{-9223372036854775808}, newSteps...)
	newSteps = append(newSteps, 9223372036854775807)
	gradeLimit := make([][]int64, len(newSteps)-1)
	gradeNum := len(gradeLimit)
	for i := 0; i < gradeNum; i++ {
		gradeLimit[i] = []int64{newSteps[i], newSteps[i+1]}
	}

	gradeData := make([]int64, c.dataLen)
	gradeValue := make([]int64, c.dataLen)
	gradeDataNum := make(map[int64]int64)
	for i := 0; i < c.dataLen; i++ {
		for j := 0; j < gradeNum; j++ {
			if c.data[i] >= gradeLimit[j][0] && c.data[i] < gradeLimit[j][1] {
				gradeData[i] = int64(j)
				gradeValue[i] = gradeLimit[j][1]
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
	gradedValueCol = &IntColumn{
		data:    gradeValue,
		dataLen: c.dataLen,
	}
	gradeNumCol = &IntColumn{
		data:    gradeDataNumVec,
		dataLen: c.dataLen,
	}
	return
}

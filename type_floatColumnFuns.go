package odktabdata

import (
	"errors"
	// "fmt"
	"log"
	"math"

	"sort"
	"strconv"
)

func (c *FloatColumn) InitData(datas ...interface{}) (err error) {
	c.dataLen = len(datas)
	c.data = make([]float64, c.dataLen)
	for i, value := range datas {
		intValue, ok := value.(float64)
		if ok {
			c.data[i] = intValue
		} else {
			err = errors.New("初始化数据失败")
			return
		}
	}

	return
}
func (c *FloatColumn) DataType() int {
	return ODKFLOAT
}
func (c *FloatColumn) Len() int {
	return c.dataLen
}
func (c *FloatColumn) Swap(a, b int) {
	c.data[b], c.data[a] = c.data[a], c.data[b]
	c.index[b], c.index[a] = c.index[a], c.index[b]
}
func (c *FloatColumn) Less(a, b int) bool {
	return c.data[a] < c.data[b]
}

func (c *FloatColumn) Index() []int {
	return c.index
}
func (c *FloatColumn) Copy() Column {
	data := make([]float64, c.dataLen)
	for i, value := range c.data {
		data[i] = value
	}
	return &FloatColumn{
		data:    data,
		dataLen: c.dataLen,
		index:   c.index,
	}

}
func (c *FloatColumn) Sort() (Column, []int) {
	c.index = make([]int, c.dataLen)
	for i := range c.data {
		c.index[i] = i
	}
	retCol := c.Copy()
	sort.Sort(retCol)
	return retCol, retCol.Index()
}

func (c *FloatColumn) SortInplace() {
	sort.Sort(c)
}
func (c *FloatColumn) GetStringAt(index int) string {
	if index < c.dataLen {
		return strconv.FormatFloat(c.data[index], 'f', -1, 64)
	}
	return ""
}
func (c *FloatColumn) GetFloatAt(index int) float64 {
	if index < c.dataLen {
		return c.data[index]
	}
	return 0.0
}
func (c *FloatColumn) GetIntAt(index int) int64 {
	if index < c.dataLen {
		return int64(c.data[index])
	}
	return 0
}
func (c *FloatColumn) GetDateAt(index int) OdkDateTime {
	return OdkDateTime{}
}
func (c *FloatColumn) GetInterfaceAt(index int) interface{} {
	if index < c.dataLen {
		return c.data[index]
	}
	return nil
}
func (c *FloatColumn) SetStringAt(index int, data string) {
	if index < c.dataLen {
		value, err := strconv.ParseFloat(data, 64)
		if err != nil {
			c.data[index] = value
		}
	}
}
func (c *FloatColumn) SetIntAt(index int, data int64) {
	if index < c.dataLen {
		c.data[index] = float64(data)
	}
}
func (c *FloatColumn) SetFloatAt(index int, data float64) {
	if index < c.dataLen {
		c.data[index] = (data)
	}
}
func (c *FloatColumn) Group() ([]Column, [][]int) {
	groupIndexs := make(map[float64]*[]int)
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
func (c *FloatColumn) GroupByYearMonth() ([]Column, [][]int) {
	return []Column{}, [][]int{}
}
func (c *FloatColumn) GroupByYear() ([]Column, [][]int) {
	return []Column{}, [][]int{}
}
func (c *FloatColumn) GetSubColumns(subColIndexs [][]int) []Column {
	subCols := make([]Column, len(subColIndexs))
	for i, subIndex := range subColIndexs {
		subCols[i] = c.GetSubColumn(subIndex)
	}
	return subCols
}
func (c *FloatColumn) GetSubColumn(subIndex []int) Column {
	data := make([]float64, len(subIndex))
	for i, index := range subIndex {
		data[i] = c.data[index]
	}
	return &FloatColumn{
		data:    data,
		dataLen: len(data),
	}
}
func (c *FloatColumn) Conv2DateColumn() (Column, error) {
	return nil, errors.New("不能从Int类型转为Date类型")
}
func (c *FloatColumn) Conv2IntColumn() (Column, error) {
	data := make([]int64, c.dataLen)
	for i, value := range c.data {
		data[i] = int64(value)
	}
	return &IntColumn{
		data:    data,
		dataLen: c.dataLen,
	}, nil
}
func (c *FloatColumn) Conv2FloatColumn() (Column, error) {
	return c, nil
}

func (c *FloatColumn) Conv2StringColumn() (Column, error) {
	data := make([]string, c.dataLen)
	for i, value := range c.data {
		data[i] = strconv.FormatFloat((value), 'f', -1, 64)
	}
	return &StringColumn{
		data:    data,
		dataLen: c.dataLen,
	}, nil
	return c, nil
}
func (c *FloatColumn) Filter(values ...interface{}) ([]Column, [][]int, error) {

	groupIndexs := make(map[float64]*[]int)
	for _, value := range values {
		temp, ok := value.(float64)
		if !ok {
			err := errors.New("数据类型不正确,需要Float64数据")
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
func (c *FloatColumn) AdvancedFilter(operateSymbol int, value interface{}) (tarCol Column, filterIndex []int, err error) {
	tarValue, ok := value.(float64)
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
func (c *FloatColumn) DataMainFactors() (min float64, max float64, mean float64, sum float64, err error) {
	temmax, temmin, temsum := -math.MaxFloat64, math.MaxFloat64, 0.0
	for _, value := range c.data {
		if temmax < value {
			temmax = value
		}
		if temmin > value {
			temmin = value
		}
		temsum += value
	}
	min, max, mean, sum = (temmin), (temmax), (temsum)/float64(c.dataLen), (temsum)
	return
}
func (c *FloatColumn) ColumnOperate(operateSymbol int) Column {
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
func (c *FloatColumn) ColumnsOperate(col Column, operateSymbol int) Column {
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
func (c *FloatColumn) ColumnNumOperate(num float64, operateSymbol int) Column {
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
func (c *FloatColumn) NumColumnOperate(num float64, operateSymbol int) Column {
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
func (c *FloatColumn) Append(col Column) (Column, error) {
	if col.DataType() != c.DataType() {
		return c, errors.New("数据类型不一致")
	}
	dataLen := c.dataLen + col.Len()
	data := make([]float64, dataLen)
	for i, value := range c.data {
		data[i] = value
	}
	for i := c.dataLen; i < dataLen; i++ {
		data[i] = col.GetFloatAt(i - c.dataLen)
	}
	return &FloatColumn{
		data:    data,
		dataLen: dataLen,
	}, nil
}
func (c *FloatColumn) GetInterIndex(col Column) (inter Column, interIndexs1 []int, outerIndexs1 []int, interIndexs2 []int, outerIndexs2 []int, err error) {
	if col.DataType() != c.DataType() {
		err = errors.New("数据类型不一致")
		return
	}
	dataMap1 := make(map[float64]bool)
	for _, value := range c.data {
		dataMap1[value] = true
	}
	interMap := make(map[float64]bool)
	for i := 0; i < col.Len(); i++ {
		if dataMap1[col.GetFloatAt(i)] {
			interIndexs2 = append(interIndexs2, i)
			interMap[col.GetFloatAt(i)] = true
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
func (c *FloatColumn) DropDuplicate() (ret Column, subIndexs []int) {
	uniqueMap := make(map[float64]bool)

	for i, value := range c.data {
		if !uniqueMap[value] {
			uniqueMap[value] = true
			subIndexs = append(subIndexs, i)
		}
	}
	ret = c.GetSubColumn(subIndexs)
	return
}
func (c *FloatColumn) Grade(steps ...interface{}) (gradedCol, gradeNumCol, gradedValueCol Column, err error) {
	newSteps := make([]float64, len(steps))
	for i, value := range steps {
		intValue, ok := value.(float64)
		if ok {
			newSteps[i] = (intValue)
		} else {
			err = errors.New("初始化数据失败")
			return
		}
	}
	newSteps = append([]float64{-92233720368547758.08}, newSteps...)
	newSteps = append(newSteps, 92233720368547758.07)
	gradeLimit := make([][]float64, len(newSteps)-1)
	gradeNum := len(gradeLimit)
	for i := 0; i < gradeNum; i++ {
		gradeLimit[i] = []float64{newSteps[i], newSteps[i+1]}
	}
	gradeData := make([]int64, c.dataLen)
	gradeValue := make([]float64, c.dataLen)
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
	gradedValueCol = &FloatColumn{
		data:    gradeValue,
		dataLen: c.dataLen,
	}
	gradeNumCol = &IntColumn{
		data:    gradeDataNumVec,
		dataLen: c.dataLen,
	}
	return
}

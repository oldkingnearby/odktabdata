package odktabdata

// 常量
const ODKINT = 1
const ODKSTRING = 2
const ODKFLOAT = 3
const ODKDATE = 4
const ODKTIME = 5

// 日期结构体
type OdkDateTime struct {
	DateStr   string //日期字符串
	Year      int
	Month     int
	Day       int
	YearMonth string
	Timestamp int64
	rawStr    string
}

type OdkTime struct {
	TimeStr   string
	Timestamp int64
}

type IntColumn struct {
	dataLen int //数据长度
	data    []int64
	index   []int
}
type FloatColumn struct {
	dataLen int //数据长度
	data    []float64
	index   []int
}
type StringColumn struct {
	dataLen int //数据长度
	data    []string
	index   []int
}
type DateColumn struct {
	dataLen int //数据长度
	data    []OdkDateTime
	index   []int
}

const (
	MATH_ADD = 1 + iota
	MATH_MINUS
	MATH_MULTIPLY
	MATH_DIVIDE
	MATH_POWER
	MATH_LOG
	MATH_EXP
	MATH_CUMSUM
	MATH_LOG2
	MATH_LOG10
)

const (
	FILTER_EQ = 101 + iota
	FILTER_NOTEQ
	FILTER_GT
	FILTER_GTE
	FILTER_LT
	FILTER_LTE
)

type Column interface {
	InitData(...interface{}) error
	DataType() int //数据格式
	Len() int
	Swap(int, int)
	Less(int, int) bool
	Index() []int
	Copy() Column           //数据拷贝
	Sort() (Column, []int)  //排序
	SortInplace()           //原地排序
	GetStringAt(int) string //设置元素
	GetFloatAt(int) float64
	GetIntAt(int) int64
	GetDateAt(int) OdkDateTime
	GetInterfaceAt(int) interface{}
	SetStringAt(int, string)
	SetIntAt(int, int64)
	SetFloatAt(int, float64)

	Group() ([]Column, [][]int)            //分组
	GroupByYearMonth() ([]Column, [][]int) //按年月分组
	GroupByYear() ([]Column, [][]int)      //按年分组
	GetSubColumns([][]int) []Column        //获取子列
	GetSubColumn([]int) Column             //获取子列
	Conv2DateColumn() (Column, error)      //转换为日期列
	Conv2IntColumn() (Column, error)
	Conv2FloatColumn() (Column, error)
	Conv2StringColumn() (Column, error)
	Filter(...interface{}) ([]Column, [][]int, error) //筛选
	// FILTER_EQ FILTER_NOTEQ FILTER_GT FILTER_GTE FILTER_LT FILTER_LTE
	AdvancedFilter(int, interface{}) (Column, []int, error)                            //高级筛选
	DataMainFactors() (min float64, max float64, mean float64, sum float64, err error) //数据的基本参数 最大值 最小值 平均值 累加值
	// LOG LOG2 LOG10 EXP CUMSUM
	ColumnOperate(int) Column //单列运算 传入运算符
	// ADD MINUS MULTIPLY DIVIDE POWER
	ColumnsOperate(Column, int) Column //两列之间进行运算 可以用这种方式叠加运算
	// ADD MINUS MULTIPLY DIVIDE POWER
	ColumnNumOperate(float64, int) Column //列与数进行运算
	// ADD MINUS MULTIPLY DIVIDE POWER
	NumColumnOperate(float64, int) Column                                                                                           //数与列进行运算
	Append(Column) (Column, error)                                                                                                  //添加数据
	GetInterIndex(Column) (inter Column, interIndexs1 []int, outerIndexs1 []int, interIndexs2 []int, outerIndexs2 []int, err error) //取交集
	DropDuplicate() (Column, []int)                                                                                                 //去重
	// 给定几组数据进行分类 比如 给 5 10 则分三类  <=5   5<x<=10  >10 三类 返回一个分级后的整数列  和不同级的数量列
	Grade(...interface{}) (Column, Column, Column, error)
}

// 返回Json数据方便web调用
type DataFrameJson struct {
	Titles []string
	Rows   int
	Cols   int
	Data   []map[string]interface{}
}

// 数据表
type DataFrame struct {
	columns      []Column
	titles       []string
	columnTitles map[string]int
	rows         int
	cols         int
}

// Excel 文件基本信息
type DataFrameFile struct {
	FilePath   string `bson :"filepath",json :"filepath"`
	Ext        string `bson :"ext",json :"ext"`
	FileId     string `bson :"fileid",json :"fileid"`
	Rows       int    `bson :"rows",json :"rows"`
	Cols       int    `bson :"cols",json :"cols"`
	df         DataFrame
	createTime int64
	updateTime int64
}

// 检查数据是滞存在 队列
type getDfEvent struct {
	done   chan bool
	fileId string
	dfp    *DataFrameFile
}

// 获取分布数据
type GetPageDataInput struct {
	Page     int    `bson :"page",json :"page"`
	PageSize int    `bson :"pagesize",json :"pagesize"`
	FileId   string `bson :"fileid",json :"fileid"`
}

// 分组导出Excel文件
type OutputGroupByResInput struct {
	OutPutFolder string `bson :"outputfolder",json :"outputfolder"`
	GroupBy      string `bson :"groupby",json :"groupby"`
	FileId       string `bson :"fileid",json :"fileid"`
}

// 提取目标数据事件
type ExtractSubDataframesInput struct {
	SrcFileId    string
	DstFileId    string
	Title        string
	OutputFolder string
}

type CollectionTitleConvert struct {
	Collection      string
	titleConvertMap map[string]string
	mustFields      []string
	showFields      map[string]string
	dateFields      []string
	indexFields     []string
}

const (
	CDN_URL                  = "http://oldking.club:2082"
	COLLECTION_TITLE_CONVERT = "CollectionTitleConvert"
	COLLECTION_CONFIG        = "CollectionConfig"
	TITLE_DB_ID              = "collectiontitleconvert"
	TITLE_DB_PWD             = "collectiontitleconvert"
)

// 上传事件
type Upload2dbInput struct {
	FileId     string
	Collection string
}

type colConfig struct {
	Collection   string
	MustFields   []string
	ShowFields   map[string]string
	IndexFields  []string
	DateFields   []string
	UniqueFields []string
	Auth         map[string]interface{}
}

type titleConvert struct {
	Collection string
	DbField    string
	ShowName   string
}

const (
	GRADE_BY_COLUMN = "data"
	GRADE_BY_COLNUM = "num"
	GRADE_BY_STEP   = "step"
)

// 分级操作
type GradeColumnInput struct {
	FileId       string
	GradeCol     string
	GradeNum     int //分级数
	GradeStep    float64
	GradeFileId  string
	GradeType    string
	OutputFolder string
}

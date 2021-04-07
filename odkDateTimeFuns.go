package odktabdata

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// 初始化日期
func (odt *OdkDateTime) InitFormString(dateStr string) (err error) {
	odt.rawStr = dateStr
	reg := regexp.MustCompile(`(\d{4})[-/](\d{1,2})[-/](\d{1,2})`)
	match_res := reg.FindStringSubmatch(dateStr)
	if len(match_res) == 4 {
		odt.Year, _ = strconv.Atoi(match_res[1])
		odt.Month, _ = strconv.Atoi(match_res[2])
		odt.Day, _ = strconv.Atoi(match_res[3])
		odt.DateStr = fmt.Sprintf("%v-%v-%v", odt.Year, odt.Month, odt.Day)
		odt.YearMonth = fmt.Sprintf("%v%02d", odt.Year, odt.Month)
		t, _ := time.Parse("2006-1-2", odt.DateStr)
		odt.Timestamp = t.Unix()
		return
	}
	reg = regexp.MustCompile(`(\d{4})(\d{2})(\d{2})`)
	match_res = reg.FindStringSubmatch(dateStr)
	if len(match_res) == 4 {

		odt.Year, _ = strconv.Atoi(match_res[1])
		odt.Month, _ = strconv.Atoi(match_res[2])
		odt.Day, _ = strconv.Atoi(match_res[3])
		odt.DateStr = fmt.Sprintf("%v-%v-%v", odt.Year, odt.Month, odt.Day)
		odt.YearMonth = fmt.Sprintf("%v%02d", odt.Year, odt.Month)
		t, _ := time.Parse("2006-1-2", odt.DateStr)
		odt.Timestamp = t.Unix()
		return
	}

	reg = regexp.MustCompile(`(\d{4})[-/]?(\d{2})`)
	match_res = reg.FindStringSubmatch(dateStr)
	if len(match_res) == 3 {

		odt.Year, _ = strconv.Atoi(match_res[1])
		odt.Month, _ = strconv.Atoi(match_res[2])
		odt.Day = 1
		odt.DateStr = fmt.Sprintf("%v-%v-%v", odt.Year, odt.Month, odt.Day)
		odt.YearMonth = fmt.Sprintf("%v%02d", odt.Year, odt.Month)
		t, _ := time.Parse("2006-1-2", odt.DateStr)
		odt.Timestamp = t.Unix()
		return
	}
	reg = regexp.MustCompile(`(\d{2})-(\d{2})-(\d{2})`)
	match_res = reg.FindStringSubmatch(dateStr)
	if len(match_res) == 4 {
		odt.Year, _ = strconv.Atoi(match_res[3])
		odt.Month, _ = strconv.Atoi(match_res[1])
		odt.Day, _ = strconv.Atoi(match_res[2])
		if odt.Year > 50 {
			odt.Year += 1900
		} else {
			odt.Year += 2000
		}
		odt.DateStr = fmt.Sprintf("%v-%v-%v", odt.Year, odt.Month, odt.Day)
		odt.YearMonth = fmt.Sprintf("%v%02d", odt.Year, odt.Month)
		t, _ := time.Parse("2006-1-2", odt.DateStr)
		odt.Timestamp = t.Unix()
		return
	}
	err = errors.New(fmt.Sprintf("未识别日期格式:%v", dateStr))
	return
}

// 字符串
func (odt *OdkDateTime) String() string {
	return odt.DateStr
}

// 整型
func (odt *OdkDateTime) Int() int64 {
	return odt.Timestamp
}

// 浮点型
func (odt *OdkDateTime) Float() float64 {
	return float64(odt.Timestamp)
}

// 添加秒
func (odt *OdkDateTime) AddSeconds(seconds int64) {
	odt.Timestamp += seconds
	t := time.Unix(odt.Timestamp, 0)
	odt.Year = t.Year()
	odt.Month = int(t.Month())
	odt.Day = t.Day()
	odt.DateStr = fmt.Sprintf("%v-%v-%v", odt.Year, odt.Month, odt.Day)
	odt.YearMonth = fmt.Sprintf("%v%02d", odt.Year, odt.Month)
}

// 添加天
func (odt *OdkDateTime) AddDays(days int64) {
	odt.Timestamp += days * 86400
	t := time.Unix(odt.Timestamp, 0)
	odt.Year = t.Year()
	odt.Month = int(t.Month())
	odt.Day = t.Day()
	odt.DateStr = fmt.Sprintf("%v-%v-%v", odt.Year, odt.Month, odt.Day)
	odt.YearMonth = fmt.Sprintf("%v%02d", odt.Year, odt.Month)
}

// 添加年
func (odt *OdkDateTime) AddYear(years int64) {
	odt.Timestamp += years * 86400 * 365
	t := time.Unix(odt.Timestamp, 0)
	odt.Year = t.Year()
	odt.Month = int(t.Month())
	odt.Day = t.Day()
	odt.DateStr = fmt.Sprintf("%v-%v-%v", odt.Year, odt.Month, odt.Day)
	odt.YearMonth = fmt.Sprintf("%v%02d", odt.Year, odt.Month)
}

func (odt *OdkDateTime) InitFromTimeStamp(timestamp int64) (err error) {
	odt.Timestamp = timestamp
	t := time.Unix(timestamp, 0)
	odt.Year = t.Year()
	odt.Month = int(t.Month())
	odt.Day = t.Day()
	odt.DateStr = fmt.Sprintf("%v-%v-%v", odt.Year, odt.Month, odt.Day)
	odt.YearMonth = fmt.Sprintf("%v%02d", odt.Year, odt.Month)
	return
}
func Convert2EnMonth(month int) string {
	switch month {
	case 1:
		return "JAN"
	case 2:
		return "FEB"
	case 3:
		return "MAR"
	case 4:
		return "APR"
	case 5:
		return "MAY"
	case 6:
		return "JUN"
	case 7:
		return "JUL"
	case 8:
		return "AUG"
	case 9:
		return "SEP"
	case 10:
		return "OCT"
	case 11:
		return "NOV"
	case 12:
		return "DEC"
	default:
		return "NONE"
	}
	return "NONE"
}

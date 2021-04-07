package odktabdata

// 开放给外部调用
func (dff *DataFrameFile) GetDf() DataFrame {
	return dff.df
}

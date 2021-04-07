package odktabdata

func (ctc *CollectionTitleConvert) GetMustFields() []string {
	return ctc.mustFields
}
func (ctc *CollectionTitleConvert) GetIndexFields() []string {
	return ctc.indexFields
}
func (ctc *CollectionTitleConvert) GetDateFields() []string {
	return ctc.dateFields
}

func (ctc *CollectionTitleConvert) Convert2DbField(title string) (dbField string, ok bool) {
	dbField, ok = ctc.titleConvertMap[title]
	return
}
func (ctc *CollectionTitleConvert) Convert2ShowField(dbField string) (showField string, ok bool) {
	showField, ok = ctc.showFields[dbField]
	return
}

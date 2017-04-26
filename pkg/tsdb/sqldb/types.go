package sqldb

type sqldbTsdbQuery struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
	Order string `json:"order"`
	Ids   []string `json:"ids,omitempty"`
	Tags  string `json:"tags,omitempty"`
}

type Tag struct {
	Name       string
	Value      string
}


type sqldbTsdbResponse struct {
	Series []Series
}

type Series struct {
	Id string
	Points []DataPoint `json:"data"`
}

type DataPoint struct {
	Time float64 `json:"timestamp"`
	Value float64 `json:"value"`
	Tags      map[string]string `json:"tags,omitempty"`
}

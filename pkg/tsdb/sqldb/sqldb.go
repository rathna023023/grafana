package sqldb

import (
"context"
  "encoding/json"
  "errors"
  "fmt"
  "net/http"
	"net/url"
	"path"
  "io/ioutil"
  "strings"

  "github.com/grafana/grafana/pkg/log"
  "github.com/grafana/grafana/pkg/middleware"
  m "github.com/grafana/grafana/pkg/models"
  "github.com/grafana/grafana/pkg/tsdb"
	"strings"
	"golang.org/x/net/context/ctxhttp"
	"bytes"
	"github.com/grafana/grafana/pkg/components/simplejson"
	"io/ioutil"
	"gopkg.in/guregu/null.v3"

  _ "github.com/go-sql-driver/mysql"
  "github.com/go-xorm/core"
  "github.com/go-xorm/xorm"
  _ "github.com/lib/pq"
  _ "github.com/mattn/go-sqlite3"
)

type SQLDBExecutor struct {
	*models.DataSource
	httpClient *http.Client
	log log.Logger
}

type sqlDataRequest struct {
  Query string `json:"query"`
  Body  []byte `json:"-"`
}

type seriesStruct struct {
  Columns []string        `json:"columns"`
  Name    string          `json:"name"`
  Values  [][]interface{} `json:"values"`
}

type resultsStruct struct {
  Series []seriesStruct `json:"series"`
}

type dataStruct struct {
  Results []resultsStruct `json:"results"`
}

func NewsqldbDBExecutor(datasource *models.DataSource) (tsdb.Executor, error) {
	return &sqldbDBExecutor{
		DataSource: datasource,
		log:        log.New("tsdb.sqldb"),
	}, nil
}

func init() {
	log.Info("sqldbDBE.init")
	// TODO the datasource name should only by 'sqldb, but the
	// plugin had this to be named sqldb-datasource (iirc)
	tsdb.RegisterExecutor("sqldb-datasource", NewsqldbDBExecutor)
}

func (e *sqldbDBExecutor) Execute(ctx context.Context, queries tsdb.QuerySlice, context *tsdb.QueryContext) *tsdb.BatchResult {

	log.Info("sqldbDBE.execute")
	result := &tsdb.BatchResult{}


	fromTime := context.TimeRange.GetFromAsMsEpoch()
	endTime  := context.TimeRange.GetToAsMsEpoch()

	for _, query := range queries {

		var tsdbQuery sqldbTsdbQuery

		tsdbQuery.Start = fromTime
		tsdbQuery.End = endTime
		tsdbQuery.Order = "ASC"

		isTags := query.Model.Get("queryBy").MustString() == "tags"

		if isTags {
			tsdbQuery.Tags, _ = fetchTags(query)
		} else {
			tsdbQuery.Ids = fetchIds(query)
		}

		isTags, metric := e.buildMetric(query)


		//if setting.Env == setting.DEV {
		log.Info("sqldbTsdb request", "params", tsdbQuery)
		log.Info("sqldbTsdb metric", "metric", metric)
		//}

		req, err := e.createRequest(tsdbQuery, query.Model.Get("type").MustString())
		if err != nil {
			result.Error = err
			return result
		}

		res, err := ctxhttp.Do(ctx, e.httpClient, req)
		if err != nil {
			result.Error = err
			return result
		}

		queryResult, err := e.parseResponse(tsdbQuery, res)
		if err != nil {
			return result.WithError(err)
		}

		result.QueryResults = queryResult
	}
	return result

}
func fetchIds(query *tsdb.Query) []string {
	var result []string

	target := query.Model.Get("target").MustString()

	result = append(result, target)

	return result
}

func fetchTags(query *tsdb.Query) (string, error) {
	var result string = ""
	model := query.Model

	for _,t := range model.Get("tags").MustArray() {
		tagJson := simplejson.NewFromAny(t)
		tag := &Tag{}
		var err error

		tag.Name, err = tagJson.Get("name").String()
		if err != nil {
			return "", err
		}

		tag.Value, err = tagJson.Get("value").String()
		if err != nil {
			return "", err
		}

		aTag := fmt.Sprintf("%s:%s", tag.Name, tag.Value)

		if result != "" {
			result = fmt.Sprintf("%s,%s", result, aTag)
		} else {
			result = aTag
		}
	}

	return result, nil
}

func (e *sqldbDBExecutor) createRequest(data sqldbTsdbQuery, qType string) (*http.Request,
error) {

	u, _ := url.Parse(e.Url)
	u.Path = path.Join(u.Path, fmt.Sprintf("/%ss/raw/query", qType )) // TODO %ss -> real func

	postData, err := json.Marshal(data)

	req, err := http.NewRequest(http.MethodPost, u.String(), strings.NewReader(string(postData)))
	if err != nil {
		log.Info("Failed to create request", "error", err)
		return nil, fmt.Errorf("Failed to create request. error: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("sqldb-Tenant", e.DataSource.JsonData.Get("tenant").MustString())
	if e.BasicAuth {
		req.SetBasicAuth(e.BasicAuthUser, e.BasicAuthPassword)
	}

	return req, err

}

func (e *sqldbDBExecutor) parseResponse(query sqldbTsdbQuery, res *http.Response) (map[string]*tsdb.QueryResult,
error) {

	queryResults := make(map[string]*tsdb.QueryResult)
	queryRes := tsdb.NewQueryResult()

	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return nil, err
	}

	if res.StatusCode/100 != 2 {
		log.Info("Request failed", "status", res.Status, "body", string(body))
		return nil, fmt.Errorf("Request failed status: %v", res.Status)
	}

	var data []Series
	err = json.Unmarshal(body, &data)
	if err != nil {
		log.Info("Failed to unmarshal sqldb response", "error", err, "status", res.Status, "body", string(body))
		return nil, err
	}


	for _, val := range data {
		log.Info(val.Id)

		series := tsdb.TimeSeries{
			Name: val.Id,
		}

		for _, point := range val.Points {
			timestamp := point.Time
			if err != nil {
				log.Info("Failed to unmarshal opentsdb timestamp", "timestamp", timestamp)
				return nil, err
			}
			series.Points = append(series.Points, tsdb.NewTimePoint(null.FloatFrom(point.Value), timestamp))
		}

		queryRes.Series = append(queryRes.Series, &series)
	}

	queryResults["A"] = queryRes
	return queryResults, nil

	return nil, nil
}

func (e *sqldbDBExecutor) buildMetric(query *tsdb.Query) (bool, map[string]interface{} ) {

	/* With tags:
	INFO[12-09|12:13:35] {
	  "queryBy": "tags",
	  "rate": false,
	  "refId": "A",
	  "seriesAggFn": "none",
	  "tags": [
	    {
	      "name": "heap",
	      "value": "used"
	    }
	  ],
	  "target": "select metric",
	  "timeAggFn": "avg",
	  "type": "gauge"
	}
	 */

	/* With single id
	{
	  "queryBy": "ids",
	  "rate": false,
	  "refId": "A",
	  "seriesAggFn": "none",
	  "tags": [],
	  "target": "MI~R~[d9c17e5f-c9b4-4d22-af2d-b9a40831c3af/Local~~]~MT~WildFly Memory Metrics~Heap Used",
	  "timeAggFn": "avg",
	  "type": "gauge"
	}
	 */



	metric := make(map[string]interface{})

	b, err := query.Model.EncodePretty()
	if (err != nil) {
		log.Info("error %s", err.Error() )
	}
	tmpString := bytes.NewBuffer(b).String()
	log.Info(tmpString)

	// Setting metric and aggregator
	metric["metric"] = query.Model.Get("measurement").MustString()
	metric["timeAggFn"] = query.Model.Get("timeAggFn").MustString()
	metric["name"] = query.Model.Get("alert.name").MustString()
	metric["query"] = query.Model.Get("query").MustString()
	metric["rawQuery"] = query.Model.Get("rawQuery").MustString()
	metric["interval"] = query.Model.Get("interval").MustString()
	metric["key"] = query.Model.Get("key").MustString()
	metric["value"] = query.Model.Get("value").MustString()
	metric["operator"] = query.Model.Get("operator").MustString()

	// Setting tags
	tags, tagsCheck := query.Model.CheckGet("tags")
	if tagsCheck && len(tags.MustMap()) > 0 {
		metric["tags"] = tags.MustMap()
	}

	isTags := query.Model.Get("queryBy").MustString() == "tags"

	return isTags, metric

}

func getEngine(ds *m.DataSource) (*xorm.Engine, error) {
  dbms, err := ds.JsonData.Get("dbms").String()
  if err != nil {
    return nil, errors.New("Invalid DBMS")
  }

  host, err := ds.JsonData.Get("host").String()
  if err != nil {
    return nil, errors.New("Invalid host")
  }

  port, err := ds.JsonData.Get("port").String()
  if err != nil {
    return nil, errors.New("Invalid port")
  }

  constr := ""

  switch dbms {
  case "mysql":
    constr = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
      ds.User, ds.Password, host, port, ds.Database)

  case "postgres":
    sslEnabled, _ := ds.JsonData.Get("ssl").Bool()
    sslMode := "disable"
    if sslEnabled {
      sslMode = "require"
    }

    constr = fmt.Sprintf("user=%s password=%s host=%s port=%s dbname=%s sslmode=%s",
      ds.User, ds.Password, host, port, ds.Database, sslMode)

  default:
    return nil, fmt.Errorf("Unknown DBMS: %s", dbms)
  }

  return xorm.NewEngine(dbms, constr)
}

func getData(db *core.DB, req *sqlDataRequest) (interface{}, error) {
  queries := strings.Split(req.Query, ";")

  data := dataStruct{}
  data.Results = make([]resultsStruct, 1)
  data.Results[0].Series = make([]seriesStruct, 0)

  for i := range queries {
    if queries[i] == "" {
      continue
    }

    rows, err := db.Query(queries[i])
    if err != nil {
      return nil, err
    }
    defer rows.Close()

    name := fmt.Sprintf("table_%d", 1)
    series, err := arrangeResult(rows, name)
    if err != nil {
      return nil, err
    }
    data.Results[0].Series = append(data.Results[0].Series, series.(seriesStruct))
  }

  return data, nil
}

func arrangeResult(rows *core.Rows, name string) (interface{}, error) {
  columnNames, err := rows.Columns()

  series := seriesStruct{}
  series.Columns = columnNames
  series.Name = name

  for rows.Next() {
    columnValues := make([]interface{}, len(columnNames))

    err = rows.ScanSlice(&columnValues)
    if err != nil {
      return nil, err
    }

    // bytes -> string
    for i := range columnValues {
      switch columnValues[i].(type) {
      case []byte:
        columnValues[i] = fmt.Sprintf("%s", columnValues[i])
      }
    }

    series.Values = append(series.Values, columnValues)
  }

  return series, err
}

func HandleRequest(c *middleware.Context, ds *m.DataSource) {
  var req sqlDataRequest
  req.Body, _ = ioutil.ReadAll(c.Req.Request.Body)
  json.Unmarshal(req.Body, &req)

  log.Debug("SQL request: query='%v'", req.Query)

  engine, err := getEngine(ds)
  if err != nil {
    c.JsonApiErr(500, "Unable to open SQL connection", err)
    return
  }
  defer engine.Close()

  session := engine.NewSession()
  defer session.Close()

  db := session.DB()

  result, err := getData(db, &req)
  if err != nil {
    c.JsonApiErr(500, fmt.Sprintf("Data error: %v, Query: %s", err.Error(), req.Query), err)
    return
  }

  c.JSON(200, result)
}

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/olivere/elastic"
)

const indexName = "ReWriteMe!!!"

func ESQueryTool() ([]*ESMessage, int64, error) {
	//fromID:发送信息的用户ID
	var fromID int64 = 100
	//toID:接收信息的用户ID
	var toID int64 = 231
	//userID:我的用户ID
	var userID int64 = 121
	//begin:查询开始时间(unix时间戳)
	var begin int64 = 1580046211
	//end:查询结束时间(unix时间戳)
	var end int64 = 1584546239
	//limit:查询多少条聊天记录
	var limit int64 = 1000
	//返回一个结构体支持跨年查询数据
	getYear, err := getYearAndTime(begin, end)
	if err != nil {
		return nil, 0, err
	}
	if limit <= 0 {
		limit = 100
	} else if limit > 10000 {
		limit = 10000
	}

	//设置超时方法
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	esClient := &elastic.Client{}
	//不涉及跨年数据
	if !getYear.MoreYears {
		_, err := esClient.IndexExists(indexName).Do(ctx)
		if err != nil {
			return nil, 0, err
		}
		f := NewFinder()
		f.Init(getYear.Begin, getYear.End).Size(limit).Pretty(true)
		res, err := f.Find(ctx, esClient, fromID, toID, userID, indexName)
		if err != nil {
			return nil, 0, err
		}
		log.Printf("success and data is %#v", res.Items)
		return res.Items, res.Total, nil
	}
	//跨年数据，多个年份的数据
	var count int64 = 0
	var resSlice = make([]FinderResponse, 0)
	var total int64 = 0
	var items = make([]*ESMessage, 0)
	//遍历跨年数据，支持跨多个年份(>=2个年份)
	for _, year := range getYear.Years {
		_, err := esClient.IndexExists(indexName).Do(ctx)
		if err != nil {
			return nil, 0, err
		}
		f := NewFinder()
		//获取过往年份比如2018年最后1秒时间戳
		var end int64 = getYearEndUnixNino(year)
		f.Init(begin, end).Size(limit).Pretty(true)
		res, err := f.Find(ctx, esClient, fromID, toID, userID, indexName)
		resSlice = append(resSlice, res)
		if err != nil {
			return nil, 0, err
		}
		//如果以前年份获取的聊天记录条数>=要查询的条数，直接返回成功信息并退出，否则查询下一个年份
		for _, res := range resSlice {
			total = total + res.Total
			items = append(items, res.Items...)
		}
		count = count + total
		if count >= limit {
			log.Printf("success and data is %#v,total hit count is %d", items, count)
			return items, count, nil
		}

	}
	log.Printf("success and data is %#v,total hit count is %d", items, count)
	return items, count, nil
}

//获取某一年的最后一秒钟(12/31 12:59:59)时间戳
func getYearEndUnixNino(year int) int64 {
	timeStr := fmt.Sprintf("12/31/%d", year)
	t, _ := time.Parse("01/02/2006", timeStr)
	return time.Date(year, t.Month(), 31, 23, 59, 59, 0, time.Now().Location()).UnixNano()
}

//GetYearAndTime 年份结构体
type GetYearAndTime struct {
	MoreYears  bool
	Years      []int
	Begin, End int64
}

//获取年份
func getYearAndTime(begin int64, end int64) (getYearAndTime *GetYearAndTime, err error) {
	var (
		beginYear int = 0
		endYear   int = 0
	)
	if end <= 0 || end > time.Now().UnixNano()/1e6 {
		end = time.Now().UnixNano() / 1e6
		endYear = time.Now().Year()
	} else {
		endYear = time.Unix(end/1e3, 0).Year()
	}
	if begin <= 0 || end > time.Now().UnixNano()/1e6 {
		begin = time.Now().AddDate(0, 0, -7).UnixNano() / 1e6
		beginYear = time.Now().AddDate(0, 0, -7).Year()
	} else {
		beginYear = time.Unix(begin/1e3, 0).Year()
	}
	if begin > end {
		return nil, fmt.Errorf("开始时间不能大雨结束时间")
	}
	if beginYear == endYear {
		getYearAndTime = &GetYearAndTime{
			MoreYears: false,
			Years:     []int{beginYear},
			Begin:     begin,
			End:       end,
		}
	} else {
		years := make([]int, 0)
		for i := beginYear; i <= endYear; i++ {
			years = append(years, i)
		}
		getYearAndTime = &GetYearAndTime{
			MoreYears: true,
			Years:     years,
			Begin:     begin,
			End:       end,
		}
	}
	return getYearAndTime, nil
}

//Finder 查找对象
type Finder struct {
	begin, end, from, size int64
	sort                   []string
	pretty                 bool
}

//NewFinder 创建空的查找对象
func NewFinder() *Finder {
	return &Finder{}
}

//Init 初始化
func (f *Finder) Init(begin, end int64) *Finder {
	f.begin = begin
	f.end = end
	return f
}

//Size 赋值
func (f *Finder) Size(size int64) *Finder {
	f.size = size
	return f
}

//Pretty 赋值
func (f *Finder) Pretty(pretty bool) *Finder {
	f.pretty = pretty
	return f
}

func (f *Finder) sorting(service *elastic.SearchService) *elastic.SearchService {
	if len(f.sort) == 0 {
		// Sort by score by default
		service = service.Sort("created_timestamp", false)
		return service
	}
	for _, s := range f.sort {
		s = strings.TrimSpace(s)

		var field string
		var asc bool

		if strings.HasPrefix(s, "-") {
			field = s[1:]
			asc = false
		} else {
			field = s
			asc = true
		}
		service = service.Sort(field, asc)
	}
	return service
}

func (f *Finder) query(service *elastic.SearchService, fromID int64, toID int64, userID int64) *elastic.SearchService {
	//多条件过滤查询器
	q := elastic.NewBoolQuery()
	q.Must(elastic.NewTermQuery("subtype", 1))
	q.Filter(elastic.NewRangeQuery("created_timestamp").Gte(f.begin).Lte(f.end))
	if userID == 0 && fromID > 0 && toID > 0 {
		q.Must(
			elastic.NewTermQuery("from_id", fromID),
			elastic.NewTermQuery("to_id", toID),
		)
	} else if userID == 0 && fromID <= 0 && toID > 0 {
		q.Must(
			elastic.NewTermQuery("to_id", toID),
		)
	} else if userID == 0 && fromID > 0 && toID <= 0 {
		q.Must(
			elastic.NewTermQuery("from_id", fromID),
		)
	}
	if userID >= 0 {
		q.Should(
			elastic.NewTermQuery("from_id", fromID),
			elastic.NewTermQuery("to_id", toID),
		)
	}
	service = service.Query(q)
	return service
}

func (f *Finder) paginate(service *elastic.SearchService) *elastic.SearchService {
	if f.from > 0 {
		service = service.From(int(f.from))
	}
	if f.size > 0 {
		service = service.Size(int(f.size))
	}
	return service
}

//FinderResponse 查找方法返回
type FinderResponse struct {
	Items []*ESMessage
	Total int64
}

//Find 查找方法
func (f *Finder) Find(ctx context.Context, esClient *elastic.Client, fromID int64, toID int64, userID int64, indexName string) (FinderResponse, error) {
	var resp FinderResponse
	//init searchservice
	search := esClient.Search().Index(indexName).Type("message").Pretty(f.pretty)
	//query
	f.query(search, fromID, toID, userID)
	f.sorting(search)
	search = f.paginate(search)
	sr, err := search.Do(ctx)
	if err != nil {
		return resp, err
	}
	items, err := f.decodeMessages(sr)
	if err != nil {
		return resp, err
	}
	resp.Items = items
	resp.Total = sr.Hits.TotalHits
	return resp, nil
}

func (f *Finder) decodeMessages(res *elastic.SearchResult) ([]*ESMessage, error) {
	log.Printf("==== access decodeMessages() ====")
	if res == nil || res.TotalHits() == 0 {
		return nil, nil
	}
	var items []*ESMessage
	for _, hit := range res.Hits.Hits {
		item := new(ESMessage)
		if err := json.Unmarshal(*hit.Source, item); err != nil {
			return nil, err
		}
		log.Printf("==== decodeMessages item is:%v ====", item)
		items = append(items, item)
	}
	return items, nil
}

//查找elasticsearch搜索引擎,返回的es json结构体
type ESMessage struct {
	UUID             string `protobuf:"bytes,1,opt,name=uuid" json:"uuid,omitempty" xml:"uuid,omitempty"`
	FromID           int64  `protobuf:"varint,2,opt,name=from_id,json=fromId" json:"from_id" xml:"from_id,omitempty"`
	ToID             int64  `protobuf:"varint,3,opt,name=to_id,json=toId" json:"to_id" xml:"to_id,omitempty"`
	AtID             int64  `protobuf:"varint,4,opt,name=at_id,json=atId" json:"at_id" xml:"at_id,omitempty"`
	Thread           string `protobuf:"bytes,5,opt,name=thread" json:"thread,omitempty" xml:"thread,omitempty"`
	MessageType      int64  `protobuf:"varint,6,opt,name=message_type,json=messageType" json:"message_type" xml:"message_type,omitempty"`
	ContentType      int64  `protobuf:"varint,7,opt,name=content_type,json=contentType" json:"content_type" xml:"content_type,omitempty"`
	Subtype          int64  `protobuf:"varint,8,opt,name=subtype" json:"subtype" xml:"subtype,omitempty"`
	MessageBody      string `protobuf:"bytes,9,opt,name=message_body,json=messageBody" json:"message_body,omitempty" xml:"message_body,omitempty"`
	CreatedTime      string `protobuf:"bytes,10,opt,name=created_time,json=createdTime" json:"created_time,omitempty" xml:"created_time,omitempty"`
	Ext              string `protobuf:"bytes,11,opt,name=ext" json:"ext,omitempty" xml:"ext,omitempty"`
	CreatedTimestamp int64  `protobuf:"varint,12,opt,name=created_timestamp,json=createdTimestamp" json:"created_timestamp" xml:"created_timestamp,omitempty"`
}

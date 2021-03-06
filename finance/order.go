package finance

import (
	"fmt"
	"sync"
	"time"

	"github.com/smartwalle/container/smap"

	"github.com/llmofang/kdbutils"
)

type Response struct {
	//	sync.Mutex
	Sym         string
	Qid         string
	Accountname string
	Time        time.Time
	Entrustno   int32
	Stockcode   string
	Askprice    float64
	Askvol      int32
	Bidprice    float64
	Bidvol      int32
	Withdraw    int32
	Status      int32
}

type ResponseInt64 struct {
	sync.Mutex
	Sym         string
	Qid         string
	Accountname string
	Time        time.Time
	Entrustno   int32
	Stockcode   string
	Askprice    float64
	Askvol      int64
	Bidprice    float64
	Bidvol      int64
	Withdraw    int64
	Status      int64
}

var mapOrder smap.Map = smap.New(true)

var orderChan chan int = make(chan int)

func getTransaction(host string, port int) {

	fmt.Println("==getOrder  host:", host)
	fmt.Println("==getOrder  port:", port)

	kdb := kdbutils.MewKdb(host, port)

	kdb.Connect()

	kdb.Subscribe("request", nil)

	ch := make(chan interface{}, 1000)
	table2struct := make(map[string]kdbutils.Factory_New)

	table2struct["request"] = func() interface{} {
		return new(Response)
	}

	go kdb.SubscribedData2Channel(ch, table2struct)

	var data interface{}

	go func() {
		for {
			data = <-ch
			switch data.(type) {

			case *Response:
				order := data.(*Response)
				//				orderInt64 := data.(*ResponseInt64)
				//				if order.Askvol == 0 && orderInt64.Askvol != 0 {
				//					order.Askvol = int32(orderInt64.Askvol)
				//					order.Withdraw = int32(orderInt64.Withdraw)
				//				}
				fmt.Println("data----order:", order)
				fmt.Println("data----data:", data)
				//				if order.Askvol == 0 {
				//					table2struct["response"] = func() interface{} {
				//						return new(ResponseInt64)
				//					}
				//					kdb.SubscribedData2Channel(ch, table2struct)
				//				}
				mapOrder.Set(order.Qid, order)
			}
		}
	}()

}

//func doUpsert() {
//	var con *kdb.KDBConn
//	var err error
//	con, err = kdb.DialKDB("127.0.0.1", 3900, "")
//	//	con, err = kdb.DialKDB("139.196.77.165", 5033, "")
//	if err != nil {
//		fmt.Printf("Failed to connect kdb: %s", err.Error())
//		return

//	}
//	// bulk insert
//	sym := &kdb.K{kdb.KS, kdb.NONE, []string{"kx", "msft"}}
//	qid := &kdb.K{kdb.KS, kdb.NONE, []string{"qqqq", "sdsdsds"}}
//	accountname := &kdb.K{kdb.KS, kdb.NONE, []string{"ac3", "ac4"}}

//	//	time := &kdb.K{kdb.KF, kdb.NONE, []time{time.Data, time.Data}}
//	//	sizes := &kdb.K{kdb.KJ, kdb.NONE, []int64{1000, 2000}}
//	tab := &kdb.K{kdb.XT, kdb.NONE, kdb.Table{[]string{"sym", "qid", "accountname"}, []*kdb.K{sym, qid, accountname}}}
//	// insert tab sync
//	bulkInsertRes, err := con.Call("upsert", &kdb.K{-kdb.KS, kdb.NONE, "response"}, tab)
//	if err != nil {
//		fmt.Println("Query failed:", err)
//		return
//	}
//	fmt.Println(bulkInsertRes)
//	// close connection
//	con.Close()

//}

//func Dopub(kline_data *Response, sql string) {

//	var con *kdb.KDBConn
//	var err error
//	//	con, err = kdb.DialKDB("10.0.0.71", 5010, "")
//	con, err = kdb.DialKDB("127.0.0.1", 3900, "")
//	if err != nil {
//		fmt.Printf("Failed to connect kdb: %s", err.Error())
//		return

//	}
//	sym := &kdb.K{kdb.KS, kdb.NONE, []string{kline_data.Sym}}
//	qid := &kdb.K{kdb.KS, kdb.NONE, []string{kline_data.Qid}}
//	accountname := &kdb.K{kdb.KS, kdb.NONE, []string{kline_data.Accountname}}
//	ftest := getNumDate(kline_data.Time.Local(), kline_data.Time.Location())
//	mytime := &kdb.K{kdb.KZ, kdb.NONE, []float64{float64(ftest)}}
//	entrustno := &kdb.K{kdb.KI, kdb.NONE, []int32{int32(kline_data.Entrustno)}}
//	stockcode := &kdb.K{kdb.KS, kdb.NONE, []string{kline_data.Stockcode}}
//	askprice := &kdb.K{kdb.KF, kdb.NONE, []float64{kline_data.Askprice}}
//	askvol := &kdb.K{kdb.KI, kdb.NONE, []int32{int32(kline_data.Askvol)}}
//	bidprice := &kdb.K{kdb.KF, kdb.NONE, []float64{kline_data.Bidprice}}
//	bidvol := &kdb.K{kdb.KI, kdb.NONE, []int32{int32(kline_data.Bidvol)}}
//	withdraw := &kdb.K{kdb.KI, kdb.NONE, []int32{int32(kline_data.Withdraw)}}
//	status := &kdb.K{kdb.KI, kdb.NONE, []int32{int32(kline_data.Status)}}
//	tab := &kdb.K{kdb.XT, kdb.NONE, kdb.Table{[]string{"sym", "qid", "accountname", "time", "entrustno", "stockcode", "askprice", "askvol", "bidprice", "bidvol", "withdraw", "status"}, []*kdb.K{sym, qid, accountname, mytime, entrustno, stockcode, askprice, askvol, bidprice, bidvol, withdraw, status}}}

//	var err2 error
//	fmt.Println("K:", tab)
//	err2 = con.AsyncCall(sql, &kdb.K{-kdb.KS, kdb.NONE, "response"}, tab)
//	//	fmt.Println("==dopub== finished:", kline_data)
//	if err2 != nil {
//		fmt.Println("Subscribe: %s", err2.Error())
//		return
//	}

//	//	var err3 error
//	//	err3 = con.AsyncCall(sql, &kdb.K{-kdb.KS, kdb.NONE, "response1"}, tab)
//	//	//	fmt.Println("==dopub== finished:", kline_data)
//	//	if err3 != nil {
//	//		fmt.Println("Subscribe: %s", err3.Error())
//	//		return
//	//	}
//}

func getNumDate(datetime time.Time, local *time.Location) float64 {
	var qEpoch = time.Date(2000, time.January, 1, 0, 0, 0, 0, local)
	diff := ((float64)(datetime.UnixNano()-qEpoch.UnixNano()) / (float64)(864000*100000000))
	return diff
}

//func MarshalTable(v interface{}, table kdb.Table) (e1 error, t kdb.Table) {
//	var err error = nil
//	var keys = []string{}
//	var values = []*kdb.K{}
//	vv := reflect.ValueOf(v)

//	//	fmt.Println("vv", vv)

//	vv = reflect.Indirect(vv)

//	for k := 0; k < vv.NumField(); k++ {
//		//		fmt.Println("vv.Field(k)", vv.Field(k))
//		//		fmt.Println("%s -- %v \n", vv.Type().Field(k).Name, vv.Field(k).Kind().String())
//		//		fmt.Println("vv.Field(k).Type()", vv.Field(k).Type())
//		if vv.Field(k).Kind() == reflect.Struct {
//			typ := vv.Field(k).Type()
//			if vv.Field(k).NumField() == 3 && typ.Field(0).Name == "sec" && typ.Field(1).Name == "nsec" {
//				m := vv.Field(k).MethodByName("Local")
//				rets := m.Call([]reflect.Value{})
//				var t time.Time = rets[0].Interface().(time.Time)
//				m2 := vv.Field(k).MethodByName("Location")
//				rets2 := m2.Call([]reflect.Value{})
//				var l *time.Location = rets2[0].Interface().(*time.Location)
//				var timeFloat64 float64 = getNumDate(t, l)
//				fmt.Println("timeFloat64,", timeFloat64)
//				keys = append(keys, strings.ToLower(vv.Type().Field(k).Name))
//				var tempk = &kdb.K{kdb.KZ, kdb.NONE, []float64{timeFloat64}}
//				values = append(values, tempk)
//			}

//			continue
//		}
//		//		fmt.Printf("aa value :%s ", aa.Field(k))

//		if vv.Field(k).Kind() == reflect.Int32 {
//			key := strings.ToLower((vv.Type().Field(k).Name)[0:1]) + (vv.Type().Field(k).Name)[1:]
//			keys = append(keys, key)
//			var tempk = &kdb.K{kdb.KI, kdb.NONE, []int32{vv.Field(k).Interface().(int32)}}
//			values = append(values, tempk)
//		}

//		if vv.Field(k).Kind() == reflect.Int64 {
//			key := strings.ToLower((vv.Type().Field(k).Name)[0:1]) + (vv.Type().Field(k).Name)[1:]
//			keys = append(keys, key)
//			var tempk *kdb.K
//			if vv.Field(k).Type().String() == "time.Duration" {
//				//				keys = append(keys, key)
//				//				tempk = &kdb.K{kdb.KN, kdb.NONE, []time.Duration{aa.Field(k).Interface().(time.Duration)}}
//				//				fmt.Printf(" tempk : ", tempk)
//			} else {
//				keys = append(keys, key)
//				tempk = &kdb.K{kdb.KI, kdb.NONE, []int64{vv.Field(k).Interface().(int64)}}
//			}

//			values = append(values, tempk)
//		}

//		if vv.Field(k).Kind() == reflect.Float32 {
//			key := strings.ToLower((vv.Type().Field(k).Name)[0:1]) + (vv.Type().Field(k).Name)[1:]
//			keys = append(keys, key)
//			var tempk = &kdb.K{kdb.KF, kdb.NONE, []float32{vv.Field(k).Interface().(float32)}}
//			values = append(values, tempk)
//		}
//		if vv.Field(k).Kind() == reflect.Float64 {
//			key := strings.ToLower((vv.Type().Field(k).Name)[0:1]) + (vv.Type().Field(k).Name)[1:]
//			keys = append(keys, key)
//			var tempk = &kdb.K{kdb.KF, kdb.NONE, []float64{vv.Field(k).Interface().(float64)}}
//			values = append(values, tempk)
//		}
//		if vv.Field(k).Kind() == reflect.Bool {
//			key := strings.ToLower((vv.Type().Field(k).Name)[0:1]) + (vv.Type().Field(k).Name)[1:]
//			keys = append(keys, key)
//			var tempk = &kdb.K{kdb.KF, kdb.NONE, []bool{vv.Field(k).Interface().(bool)}}
//			values = append(values, tempk)

//		}
//		if vv.Field(k).Kind() == reflect.String {
//			key := strings.ToLower((vv.Type().Field(k).Name)[0:1]) + (vv.Type().Field(k).Name)[1:]
//			keys = append(keys, key)
//			var tempk = &kdb.K{kdb.KS, kdb.NONE, []string{vv.Field(k).Interface().(string)}}
//			values = append(values, tempk)
//		}

//	}
//	table.Columns = keys
//	table.Data = values
//	return err, table
//}

/**
con *kdb.KDBConn  kdb连接
 v interface{}  需要操作的结构（kdb表对应的结构）
dbTableName string kdb表名
sql string  q语法
**/
//func UpsertByInterface(con *kdb.KDBConn, v interface{}, dbTableName string, sql string) error {
//	var err error
//	table := kdb.Table{}
//	err, table = MarshalTable(v, table)

//	if err != nil {
//		fmt.Println(err)
//	}

//	tab := &kdb.K{kdb.XT, kdb.NONE, table}

//	var err2 error
//	fmt.Println("K:", tab)
//	err2 = con.AsyncCall(sql, &kdb.K{-kdb.KS, kdb.NONE, dbTableName}, tab)
//	//	fmt.Println("==dopub== finished:", kline_data)
//	if err2 != nil {
//		fmt.Println("Subscribe: %s", err2.Error())
//		return err2
//	}
//	return nil
//}

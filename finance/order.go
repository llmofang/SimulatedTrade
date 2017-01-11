package finance

import (
	"fmt"
	"sync"
	"time"

	"github.com/smartwalle/container/smap"

	"reflect"

	"errors"
	"strings"

	kdb "github.com/sv/kdbgo"
)

type Response struct {
	sync.Mutex
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

func getTransaction() {

	for {
		var con *kdb.KDBConn
		var err error
		con, err = kdb.DialKDB("10.0.0.71", 5010, "")
		//	con, err = kdb.DialKDB("139.196.77.165", 5033, "")
		if err != nil {
			fmt.Printf("Failed to connect kdb: %s", err.Error())
			return

		}
		//err = con.AsyncCall(".u.sub", &kdb.K{-kdb.KS, kdb.NONE, "Transaction"}, &kdb.K{-kdb.KS, kdb.NONE, ""})
		//	err = con.AsyncCall(".u.sub", &kdb.K{-kdb.KS, kdb.NONE, "Market"}, &kdb.K{-kdb.KS, kdb.NONE, "603025"})
		err = con.AsyncCall(".u.sub", &kdb.K{-kdb.KS, kdb.NONE, "request"}, &kdb.K{-kdb.KS, kdb.NONE, ""})
		if err != nil {
			fmt.Println("Subscribe: %s", err.Error())
			return
		}

		// ignore type print output

		res, _, err := con.ReadMessage()

		if err != nil {
			fmt.Println("Error processing message: ", err.Error())
			return
		}

		data_list := res.Data.([]*kdb.K)
		table := data_list[2].Data.(kdb.Table)

		for i := 0; i < int(table.Data[0].Len()); i++ {
			kline_data := &Response{}
			kline_data2 := &ResponseInt64{}
			err := kdb.UnmarshalDict(table.Index(i), kline_data)

			if err != nil {
				fmt.Println("Failed to unmrshall dict ", err)
				continue
			}
			err2 := kdb.UnmarshalDict(table.Index(i), kline_data2)

			if err2 != nil {
				fmt.Println("Failed to unmrshall dict ", err2)
				continue
			}
			fmt.Println("get:", kline_data)
			fmt.Println("get2:", kline_data2)
			if kline_data.Askvol == 0 && kline_data2.Askvol != 0 {
				kline_data.Askvol = int32(kline_data2.Askvol)
				kline_data.Withdraw = int32(kline_data2.Withdraw)
			}
			//			if kline_data.Askvol != 0 && kline_data2.Askvol == 0 {
			//				kline_data2.Askvol = kline_data.Askvol
			//				kline_data2.Withdraw = kline_data.Withdraw
			//			}
			mapOrder.Set(kline_data.Qid, kline_data)

		}

	}
}

func doUpsert() {
	var con *kdb.KDBConn
	var err error
	con, err = kdb.DialKDB("127.0.0.1", 3900, "")
	//	con, err = kdb.DialKDB("139.196.77.165", 5033, "")
	if err != nil {
		fmt.Printf("Failed to connect kdb: %s", err.Error())
		return

	}
	// bulk insert
	sym := &kdb.K{kdb.KS, kdb.NONE, []string{"kx", "msft"}}
	qid := &kdb.K{kdb.KS, kdb.NONE, []string{"qqqq", "sdsdsds"}}
	accountname := &kdb.K{kdb.KS, kdb.NONE, []string{"ac3", "ac4"}}

	//	time := &kdb.K{kdb.KF, kdb.NONE, []time{time.Data, time.Data}}
	//	sizes := &kdb.K{kdb.KJ, kdb.NONE, []int64{1000, 2000}}
	tab := &kdb.K{kdb.XT, kdb.NONE, kdb.Table{[]string{"sym", "qid", "accountname"}, []*kdb.K{sym, qid, accountname}}}
	// insert tab sync
	bulkInsertRes, err := con.Call("upsert", &kdb.K{-kdb.KS, kdb.NONE, "response"}, tab)
	if err != nil {
		fmt.Println("Query failed:", err)
		return
	}
	fmt.Println(bulkInsertRes)
	// close connection
	con.Close()

}

func Dopub(kline_data *Response, sql string) {

	var con *kdb.KDBConn
	var err error
	//	con, err = kdb.DialKDB("10.0.0.71", 5010, "")
	con, err = kdb.DialKDB("127.0.0.1", 3900, "")
	if err != nil {
		fmt.Printf("Failed to connect kdb: %s", err.Error())
		return

	}
	sym := &kdb.K{kdb.KS, kdb.NONE, []string{kline_data.Sym}}
	qid := &kdb.K{kdb.KS, kdb.NONE, []string{kline_data.Qid}}
	accountname := &kdb.K{kdb.KS, kdb.NONE, []string{kline_data.Accountname}}
	ftest := getNumDate(kline_data.Time.Local(), kline_data.Time.Location())
	mytime := &kdb.K{kdb.KZ, kdb.NONE, []float64{float64(ftest)}}
	entrustno := &kdb.K{kdb.KI, kdb.NONE, []int32{int32(kline_data.Entrustno)}}
	stockcode := &kdb.K{kdb.KS, kdb.NONE, []string{kline_data.Stockcode}}
	askprice := &kdb.K{kdb.KF, kdb.NONE, []float64{kline_data.Askprice}}
	askvol := &kdb.K{kdb.KI, kdb.NONE, []int32{int32(kline_data.Askvol)}}
	bidprice := &kdb.K{kdb.KF, kdb.NONE, []float64{kline_data.Bidprice}}
	bidvol := &kdb.K{kdb.KI, kdb.NONE, []int32{int32(kline_data.Bidvol)}}
	withdraw := &kdb.K{kdb.KI, kdb.NONE, []int32{int32(kline_data.Withdraw)}}
	status := &kdb.K{kdb.KI, kdb.NONE, []int32{int32(kline_data.Status)}}
	tab := &kdb.K{kdb.XT, kdb.NONE, kdb.Table{[]string{"sym", "qid", "accountname", "time", "entrustno", "stockcode", "askprice", "askvol", "bidprice", "bidvol", "withdraw", "status"}, []*kdb.K{sym, qid, accountname, mytime, entrustno, stockcode, askprice, askvol, bidprice, bidvol, withdraw, status}}}

	var err2 error
	fmt.Println("K:", tab)
	err2 = con.AsyncCall(sql, &kdb.K{-kdb.KS, kdb.NONE, "response"}, tab)
	//	fmt.Println("==dopub== finished:", kline_data)
	if err2 != nil {
		fmt.Println("Subscribe: %s", err2.Error())
		return
	}

	//	var err3 error
	//	err3 = con.AsyncCall(sql, &kdb.K{-kdb.KS, kdb.NONE, "response1"}, tab)
	//	//	fmt.Println("==dopub== finished:", kline_data)
	//	if err3 != nil {
	//		fmt.Println("Subscribe: %s", err3.Error())
	//		return
	//	}
}

func getNumDate(datetime time.Time, local *time.Location) float64 {
	var qEpoch = time.Date(2000, time.January, 1, 0, 0, 0, 0, local)
	diff := ((float64)(datetime.UnixNano()-qEpoch.UnixNano()) / (float64)(864000*100000000))
	return diff
}

func MarshalTable(v interface{}, table kdb.Table) (e1 error, t kdb.Table) {
	var err error = nil
	var keys = []string{}
	var values = []*kdb.K{}
	vv := reflect.ValueOf(v)
	aa := reflect.ValueOf(v).Elem()
	if vv.Kind() != reflect.Ptr || vv.IsNil() {
		err = errors.New("Invalid target type. Should be non null pointer")
	}
	vv = reflect.Indirect(vv)

	for k := 0; k < vv.NumField(); k++ {
		//		fmt.Println("vv.Field(k)", vv.Field(k))
		//		fmt.Println("%s -- %v \n", vv.Type().Field(k).Name, vv.Field(k).Kind().String())
		//		fmt.Println("vv.Field(k).Type()", vv.Field(k).Type())
		if vv.Field(k).Kind() == reflect.Struct {
			typ := vv.Field(k).Type()
			if vv.Field(k).NumField() == 3 && typ.Field(0).Name == "sec" && typ.Field(1).Name == "nsec" {
				m := vv.Field(k).MethodByName("Local")
				rets := m.Call([]reflect.Value{})
				var t time.Time = rets[0].Interface().(time.Time)
				m2 := vv.Field(k).MethodByName("Location")
				rets2 := m2.Call([]reflect.Value{})
				var l *time.Location = rets2[0].Interface().(*time.Location)
				var timeFloat64 float64 = getNumDate(t, l)
				fmt.Println("timeFloat64,", timeFloat64)
				keys = append(keys, strings.ToLower(vv.Type().Field(k).Name))
				var tempk = &kdb.K{kdb.KZ, kdb.NONE, []float64{timeFloat64}}
				values = append(values, tempk)
			}

			continue
		}
		//		fmt.Printf("aa value :%s ", aa.Field(k))

		if vv.Field(k).Kind() == reflect.Int32 {
			key := strings.ToLower((vv.Type().Field(k).Name)[0:1]) + (vv.Type().Field(k).Name)[1:]
			keys = append(keys, key)
			var tempk = &kdb.K{kdb.KI, kdb.NONE, []int32{aa.Field(k).Interface().(int32)}}
			values = append(values, tempk)
		}

		if vv.Field(k).Kind() == reflect.Int64 {
			//

			key := strings.ToLower((vv.Type().Field(k).Name)[0:1]) + (vv.Type().Field(k).Name)[1:]

			keys = append(keys, key)

			var tempk *kdb.K
			if vv.Field(k).Type().String() == "time.Duration" {
				//				keys = append(keys, key)
				//				tempk = &kdb.K{kdb.KN, kdb.NONE, []time.Duration{aa.Field(k).Interface().(time.Duration)}}
				//				fmt.Printf(" tempk : ", tempk)
			} else {
				keys = append(keys, key)
				tempk = &kdb.K{kdb.KI, kdb.NONE, []int64{aa.Field(k).Interface().(int64)}}
			}

			values = append(values, tempk)
		}

		if vv.Field(k).Kind() == reflect.Float32 {
			key := strings.ToLower((vv.Type().Field(k).Name)[0:1]) + (vv.Type().Field(k).Name)[1:]
			keys = append(keys, key)
			var tempk = &kdb.K{kdb.KF, kdb.NONE, []float32{aa.Field(k).Interface().(float32)}}
			values = append(values, tempk)
		}
		if vv.Field(k).Kind() == reflect.Float64 {
			key := strings.ToLower((vv.Type().Field(k).Name)[0:1]) + (vv.Type().Field(k).Name)[1:]
			keys = append(keys, key)
			var tempk = &kdb.K{kdb.KF, kdb.NONE, []float64{aa.Field(k).Interface().(float64)}}
			values = append(values, tempk)
		}
		if vv.Field(k).Kind() == reflect.Bool {
			key := strings.ToLower((vv.Type().Field(k).Name)[0:1]) + (vv.Type().Field(k).Name)[1:]
			keys = append(keys, key)
			var tempk = &kdb.K{kdb.KF, kdb.NONE, []bool{aa.Field(k).Interface().(bool)}}
			values = append(values, tempk)

		}
		if vv.Field(k).Kind() == reflect.String {
			key := strings.ToLower((vv.Type().Field(k).Name)[0:1]) + (vv.Type().Field(k).Name)[1:]
			keys = append(keys, key)
			var tempk = &kdb.K{kdb.KS, kdb.NONE, []string{aa.Field(k).Interface().(string)}}
			values = append(values, tempk)
		}

		//vv.Type().Field(k).Name
		//		keys = append(keys, vv.Type().Field(k).Name)
		//		var tempk = &kdb.K{kdb.KC, kdb.NONE, "123"}
		//		values = append(values, tempk)
	}
	table.Columns = keys
	table.Data = values
	return err, table
}

/**
con *kdb.KDBConn  kdb连接
 v interface{}  需要操作的结构（kdb表对应的结构）
dbTableName string kdb表名
sql string  q语法
**/
func UpsertByInterface(con *kdb.KDBConn, v interface{}, dbTableName string, sql string) error {
	var err error
	table := kdb.Table{}
	err, table = MarshalTable(v, table)

	if err != nil {
		fmt.Println(err)
	}

	tab := &kdb.K{kdb.XT, kdb.NONE, table}

	var err2 error
	fmt.Println("K:", tab)
	err2 = con.AsyncCall(sql, &kdb.K{-kdb.KS, kdb.NONE, dbTableName}, tab)
	//	fmt.Println("==dopub== finished:", kline_data)
	if err2 != nil {
		fmt.Println("Subscribe: %s", err2.Error())
		return err2
	}
	return nil
}

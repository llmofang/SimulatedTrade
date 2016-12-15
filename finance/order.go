package main

import (
	"container/smap"
	"fmt"
	"sync"
	"time"

	kdb "github.com/sv/kdbgo"
)

type Response struct {
	sync.Mutex
	Sym         string
	Qid         string
	Accountname string
	Time        time.Time
	Entrustno   int64
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
		con, err = kdb.DialKDB("127.0.0.1", 3900, "")
		//	con, err = kdb.DialKDB("139.196.77.165", 5033, "")
		if err != nil {
			fmt.Printf("Failed to connect kdb: %s", err.Error())
			return

		}
		//err = con.AsyncCall(".u.sub", &kdb.K{-kdb.KS, kdb.NONE, "Transaction"}, &kdb.K{-kdb.KS, kdb.NONE, ""})
		//	err = con.AsyncCall(".u.sub", &kdb.K{-kdb.KS, kdb.NONE, "Market"}, &kdb.K{-kdb.KS, kdb.NONE, "603025"})
		err = con.AsyncCall(".u.sub", &kdb.K{-kdb.KS, kdb.NONE, "response"}, &kdb.K{-kdb.KS, kdb.NONE, ""})
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
			err := kdb.UnmarshalDict(table.Index(i), kline_data)

			if err != nil {
				fmt.Println("Failed to unmrshall dict ", err)
				continue
			}

			fmt.Println("get:", kline_data)
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

func dopub(kline_data *Response) {

	var con *kdb.KDBConn
	var err error
	con, err = kdb.DialKDB("127.0.0.1", 3900, "")
	//	con, err = kdb.DialKDB("139.196.77.165", 5033, "")
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
	err2 = con.AsyncCall("upd", &kdb.K{-kdb.KS, kdb.NONE, "response"}, tab)
	fmt.Println("==dopub== finished:", kline_data)
	if err2 != nil {
		fmt.Println("Subscribe: %s", err2.Error())
		return
	}
}

func getNumDate(datetime time.Time, local *time.Location) float64 {
	var qEpoch = time.Date(2000, time.January, 1, 0, 0, 0, 0, local)
	diff := ((float64)(datetime.UnixNano()-qEpoch.UnixNano()) / (float64)(864000*100000000))
	return diff
}

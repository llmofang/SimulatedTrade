package main

import (
	"fmt"

	kdb "github.com/sv/kdbgo"

	"time"
)

type Market struct {
	Sym                  string
	Time                 time.Time
	SzWindCode           string
	NActionDay           int32
	NTime                int32
	NStatus              int32
	NPreClose            int32
	NOpen                int32
	NHigh                int32
	NLow                 int32
	NMatch               int32
	NAskPrice1           int32
	NAskPrice2           int32
	NAskPrice3           int32
	NAskPrice4           int32
	NAskPrice5           float32
	NAskPrice6           float32
	NAskPrice7           float32
	NAskPrice8           float32
	NAskPrice9           float32
	NAskPrice10          float32
	NAskVol1             int32
	NAskVol2             int32
	NAskVol3             int32
	NAskVol4             int32
	NAskVol5             int32
	NAskVol6             int32
	NAskVol7             int32
	NAskVol8             int32
	NAskVol9             int32
	NAskVol10            int32
	NBidPrice1           float32
	NBidPrice2           float32
	NBidPrice3           float32
	NBidPrice4           float32
	NBidPrice5           float32
	NBidPrice6           float32
	NBidPrice7           float32
	NBidPrice8           float32
	NBidPrice9           float32
	NBidPrice10          float32
	NBidVol1             int32
	NBidVol2             int32
	NBidVol3             int32
	NBidVol4             int32
	NBidVol5             int32
	NBidVol6             int32
	NBidVol7             int32
	NBidVol8             int32
	NBidVol9             int32
	NBidVol10            int32
	NNumTrades           int32
	IVolume              int32
	Turnover             int32
	NTotalBidVol         int32
	NTotalAskVol         int32
	NWeightedAvgBidPrice int32
	NWeightedAvgAskPrice int32
	NIOPV                int32
	NYieldToMaturity     int32
	NHighLimited         int32
	NLowLimited          int32
	NSyl1                int32
	NSyl2                int32
	NSD2                 int32
}

var mapMarket map[string]Market = make(map[string]Market)

var marketChan chan int = make(chan int)

func getMarket() {

	var con *kdb.KDBConn
	var err error
	//	con, err = kdb.DialKDB("192.168.222.1", 3900, "")
	con, err = kdb.DialKDB("139.196.77.165", 5033, "")
	if err != nil {
		fmt.Printf("Failed to connect kdb: %s", err.Error())
		return

	}
	//err = con.AsyncCall(".u.sub", &kdb.K{-kdb.KS, kdb.NONE, "Transaction"}, &kdb.K{-kdb.KS, kdb.NONE, ""})
	err = con.AsyncCall(".u.sub", &kdb.K{-kdb.KS, kdb.NONE, "Market"}, &kdb.K{-kdb.KS, kdb.NONE, ""})

	if err != nil {
		fmt.Println("Subscribe: %s", err.Error())
		return
	}
	for {
		// ignore type print output
		res, _, err := con.ReadMessage()

		if err != nil {
			fmt.Println("Error processing message: ", err.Error())
			return
		}

		data_list := res.Data.([]*kdb.K)
		table := data_list[2].Data.(kdb.Table)
		fmt.Println("table.length :", table.Data[0].Len())
		for j := 0; j < table.Data[0].Len(); j++ {
			kline_data := new(Market)

			err := kdb.UnmarshalDict(table.Index(j), kline_data)
			if err != nil {
				fmt.Println("Failed to unmrshall dict ", err)
				continue
			}
			//			fmt.Println(kline_data)
			mapMarket[kline_data.Sym] = *kline_data

		}

		time.Sleep(time.Second * 3)
	}
	marketChan <- 0
}

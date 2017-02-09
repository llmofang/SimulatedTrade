package finance

import (
	"fmt"

	"github.com/llmofang/kdbutils"
	"github.com/llmofang/kdbutils/tbls"
)

var mapMarket map[string]*tbls.Market = make(map[string]*tbls.Market)

var marketChan chan int = make(chan int)

func getMarket(host string, port int) {

	fmt.Println("==getMarket  host:", host)
	fmt.Println("==getMarket  port:", port)

	kdb := kdbutils.MewKdb(host, port)

	kdb.Connect()

	kdb.Subscribe("Market", nil)

	ch := make(chan interface{}, 1000)
	table2struct := make(map[string]kdbutils.Factory_New)

	table2struct["Market"] = func() interface{} {
		return new(tbls.Market)
	}

	go kdb.SubscribedData2Channel(ch, table2struct)

	var data interface{}

	go func() {
		for {
			data = <-ch
			switch data.(type) {

			case *tbls.Market:
				market := data.(*tbls.Market)
				fmt.Println("data----market:", market)
				mapMarket[market.Sym] = market
			}
		}
	}()

}

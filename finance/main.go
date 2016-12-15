// finance project finance.go
package main

import (
	"fmt"
	"time"
)

var business_chan chan int = make(chan int)

func main() {
	//askprice 是卖价  bid是买  nmatch是市价
	fmt.Println("===main===")

	//	te := &Response{}
	//	te.Sym = "testtt"
	//	mapOrder["1"] = te

	//	fmt.Println("-==-=---=-=============:length:", len(mapOrder))

	//	delete(mapOrder, "1")
	//	re, ok := mapOrder["1"]
	//	if ok {
	//		fmt.Println(re)
	//	} else {
	//		fmt.Println("=== no in====")
	//	}
	//	fmt.Println("-==-=---=-=============:length:", len(mapOrder))
	fmt.Println(time.Now().Local())
	go getMarket()
	go getTransaction()
	fmt.Println("===over===")

	go dohandle()
	<-marketChan

}

func dohandle() {
	fmt.Println(time.Now())
	for {

		var mapOrderForDelete map[string]*Response = make(map[string]*Response)

		fmt.Println("marketList size:", len(mapMarket))
		fmt.Println("orderList size:", mapOrder.Len())
		//		fmt.Println("orderList :", mapOrder)
		for _, v_market := range mapMarket {

			for _, _order := range mapOrder.Values() {
				v_order := _order.(*Response)
				if v_market.Sym == v_order.Stockcode && v_order.Status != 4 {

					fmt.Println("order handle:", v_order)
					if v_order.Askvol > 0 {
						//卖,需小于市价才能成交
						if int(v_order.Askprice*10000) <= int(v_market.NMatch) {

							fmt.Println("ask deal available ask deal available ask deal available")
							v_order.Lock()
							v_order.Status = 4
							v_order.Time = time.Now()
							dopub(v_order)

							v_order.Unlock()
						}
					}
					if v_order.Bidvol > 0 {
						//买,需要大于市价
						if int(v_order.Bidprice*10000) >= int(v_market.NMatch) {
							fmt.Println("bid deal available")
							v_order.Lock()
							v_order.Status = 4
							v_order.Time = time.Now()
							dopub(v_order)
							v_order.Unlock()
						}
					}
					//因为在for循环里不能直接移除元素，套了个mapOrderForDelete来从订单map 删除
					mapOrderForDelete[v_order.Qid] = v_order
				}
			}
		}

		for _, v_order := range mapOrderForDelete {

			mapOrder.Remove(v_order.Qid)

		}
		mapOrderForDelete = nil
		time.Sleep(time.Second * 1)
	}

}

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
		fmt.Println("marketList size:", len(mapMarket))
		fmt.Println("orderList size:", len(mapOrder))
		for _, v_market := range mapMarket {

			for _, v_order := range mapOrder {

				if v_market.Sym == v_order.Stockcode {
					fmt.Println("order handle:", v_order)
					if v_order.Askvol > 0 {
						//卖,需小于市价才能成交
						if int(v_order.Askprice*10000) <= int(v_market.NMatch) {

							fmt.Println("ask deal available")
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
				}
			}
		}
		time.Sleep(time.Second * 1)
	}

}

// finance project finance.go
package finance

import (
	"fmt"
	"strconv"
	"time"
	"github.com/llmofang/SimulatedTrade/util/conf"
	"github.com/llmofang/SimulatedTrade/util/string"
	"github.com/llmofang/kdbutils"

)

var business_chan chan int = make(chan int)
var entrustno int64 = time.Now().Unix()

func DoMain() {
	//  nmatch是市价
	fmt.Println("===main===")

	myConfig := new(confutil.Config)
	myConfig.InitConfig("./transcation_config.txt")
	fmt.Println(myConfig.Read("default", "path"))
	fmt.Printf("%v", myConfig.Mymap)
	var filesArray []string
	for i := 1; i < 10; i++ {
		key := "path" + strconv.Itoa(i)
		path := myConfig.Read("default", key)
		if path != "" {
			filesArray = append(filesArray, path)
		}
	}

	portStr := myConfig.Read("default", "market_port")
	market_port := int(stringutil.StrToInt32(portStr))
	market_host := myConfig.Read("default", "market_host")

	fmt.Printf("market_host", market_host)
	fmt.Println("market_port", market_port)

	order_portStr := myConfig.Read("default", "order_port")
	order_port := int(stringutil.StrToInt32(order_portStr))
	order_host := myConfig.Read("default", "order_host")

	fmt.Printf("order_host", order_host)
	fmt.Println("order_port", order_port)

	if market_host != "" && market_port != 0 && order_host != "" && order_port != 0 {
		fmt.Println("---getMarket-LoadConfigData--- start")
		go getMarket(market_host, market_port)
		go getTransaction(order_host, order_port)

		go dohandle(order_host, order_port)
		<-marketChan
	}
	fmt.Println("===over===")

}

//func main() {
//	//  nmatch是市价
//	fmt.Println("===main===")

//	go getMarket()
//	go getTransaction()

//	go dohandle()
//	<-marketChan
//	fmt.Println("===over===")

//}

func dohandle(host string, port int) {
	fmt.Println("==thread   ==  dohandle==", time.Now())

	kdb := kdbutils.MewKdb(host, port)

	kdb.Connect()
	for {

		var mapOrderForDelete map[string]*Response = make(map[string]*Response)

		//		fmt.Println("marketList size:", len(mapMarket))
		fmt.Println("orderList size:", mapOrder.Len())
		//		fmt.Println("orderList :", mapOrder)
		for _, v_market := range mapMarket {

			for _, _order := range mapOrder.Values() {
				v_order := _order.(*Response)

				//撤单status=3 改成5
				if v_order.Status == 3 {
					fmt.Println("撤单status 3 ->5", v_order)
					v_order.Lock()
					v_order.Status = 5
					v_order.Withdraw = v_order.Askvol
					v_order.Time = time.Now()
					kdb.FuncTable("wsupd", "response", v_order)
					//					Dopub(v_order, "wsupd")
					mapOrderForDelete[v_order.Qid] = v_order
					fmt.Println("撤单状态已经5", v_order)
				} else if v_order.Status == 1 {
					//
					if v_market.Sym == v_order.Stockcode {

						fmt.Println("处理交易,状态1->4 :", v_order)
						if v_order.Askvol < 0 {
							//卖,需小于市价才能成交
							if int(v_order.Askprice*10000) <= int(v_market.NMatch) {

								//								fmt.Println("ask deal available ")
								v_order.Lock()
								v_order.Status = 4
								v_order.Bidprice = v_order.Askprice
								v_order.Bidvol = v_order.Askvol
								v_order.Time = time.Now()
								kdb.FuncTable("wsupd", "response", v_order)
								//								Dopub(v_order, "wsupd")
								v_order.Unlock()
								fmt.Println("交易完成状态已经4 :", v_order)
							}
						}
						if v_order.Askvol > 0 {
							//买,需要大于市价
							if int(v_order.Askprice*10000) >= int(v_market.NMatch) {
								//								fmt.Println("ask  order handle:", v_order)
								//								fmt.Println("market:", v_market)
								//								fmt.Println("ask deal available")
								v_order.Lock()
								v_order.Status = 4
								v_order.Bidprice = v_order.Askprice
								v_order.Bidvol = v_order.Askvol
								v_order.Time = time.Now()
								kdb.FuncTable("wsupd", "response", v_order)
								//								Dopub(v_order, "wsupd")
								v_order.Unlock()
								fmt.Println("交易完成状态已经4 :", v_order)
							}
						}

					}

					//因为在for循环里不能直接移除元素，套了个mapOrderForDelete来从订单map 删除
					mapOrderForDelete[v_order.Qid] = v_order
				} else if v_order.Status == 0 {
					//所有ask的交易都要先改成已报状态1，加上委托号（entrustno），不然挂单无法撤单
					fmt.Println("状态 ->1", v_order)

					v_order.Lock()
					v_order.Status = 1
					entrustno = entrustno + 1
					v_order.Entrustno = int32(entrustno)
					v_order.Time = time.Now()

					kdb.FuncTable("wsupd", "response", v_order)
					//					Dopub(v_order, "wsupd")
					v_order.Unlock()
					fmt.Println("状态已经改成1  ", v_order)
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

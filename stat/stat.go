package stat

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/smartwalle/container/smap"

	kdb "github.com/sv/kdbgo"
)

type Response struct {
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
	Entrustno   int64
	Stockcode   string
	Askprice    float64
	Askvol      int64
	Bidprice    float64
	Bidvol      int64
	Withdraw    int64
	Status      int64
}

type STK struct {
	sync.Mutex
	SpaceStk   SpaceSTK
	ProfitStk  ProfitSTK
	orderArray []*Response
}

//仓位统计
type SpaceSTK struct {
	Sym          string
	Accountname  string
	Stockcode    string
	SpaceVol     int32   //仓位
	OnlineProfit float64 //浮动盈利
	AvgPrice     float64 //均价
}

//利润统计
type ProfitSTK struct {
	Sym         string
	Accountname string
	Stockcode   string
	PastProfit  float64 //已成利润

	BidCount    int32   //交易笔数
	BidNum      int32   //股数
	BidMoneySum float64 //交易额
	TotalTax    float64 //总费用
}

type StaticsResult struct { //统计仓位  统计利润都用这个
	sync.Mutex
	Sym          string
	Qid          string
	Accountname  string
	Time         time.Time
	Entrustno    int32
	Stockcode    string
	Askprice     float64
	Askvol       int32
	Bidprice     float64
	Bidvol       int32
	Withdraw     int32
	Status       int32
	RemainVol    int32
	SpaceProfit  int32   //仓位盈利
	OnlineProfit float64 //浮动盈利
	PastProfit   float64 //已成利润
	AvgPrice     float64 //均价
	BidCount     int32   //交易笔数
	BidNum       int32   //股数
	BidMoneySum  float64 //交易额
	TotalTax     float64 //总费用
}
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

// Map<String,Map<String,SET<RESPONSE>>>
//var mapALLOrder smap.Map = smap.New(true)

// Map<String,Map<String,Map<String,StaticsResult>>> 仓位的统计存放容器
var mapResult smap.Map = smap.New(true)

// Map<String,Map<String,StaticsResult>> 利润的统计存放容器
//var mapProfit smap.Map = smap.New(true)

var marketChan chan int = make(chan int)
var orderChan chan int = make(chan int)
var tChan chan int = make(chan int)

func DoMain() {
	//nmatch是市价
	fmt.Println("==stat=main===")
	SelectTransaction()
	go GetMarket()

	go GetTransaction()

	printMap()
	//	<-marketChan
	fmt.Println("==stat=over===")

}

func SelectTransaction() {
	fmt.Println("==SelectTransaction==")
	var con *kdb.KDBConn
	var err error
	con, err = kdb.DialKDB("139.224.9.75", 52800, "")
	//	con, err = kdb.DialKDB("139.196.77.165", 5033, "")
	if err != nil {
		fmt.Printf("Failed to connect kdb: %s", err.Error())
		return

	}

	res, err := con.Call("0!select from response")
	if err != nil {
		fmt.Println("Subscribe: %s", err.Error())
		return
	}

	// ignore type print output
	//	fmt.Println("res:", res)

	table := res.Data.(kdb.Table)
	//	fmt.Println("table:", table)

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

		if kline_data.Askvol == 0 && kline_data2.Askvol != 0 {
			kline_data.Askvol = int32(kline_data2.Askvol)
			kline_data.Withdraw = int32(kline_data2.Withdraw)
			kline_data.Status = int32(kline_data2.Status)
			kline_data.Bidvol = int32(kline_data2.Bidvol)
			kline_data.Entrustno = int32(kline_data2.Entrustno)
		}

		handleData(kline_data)
	}
	//	fmt.Println("==SelectTransaction  is over ==")
}

func handleData(kline_data *Response) {
	if kline_data.Sym == "liuyiqi" {
		fmt.Println("select:", kline_data)
		//		fmt.Println("==handleData1111111111==", kline_data)
		//		if kline_data.Status == 4 || kline_data.Status == 5 {
		user := kline_data.Sym
		account := kline_data.Accountname
		//			fmt.Println("==account==", account)

		//			var stat *StaticsResult
		stat := &STK{}
		p := ProfitSTK{}
		s := SpaceSTK{}
		stat.ProfitStk = p
		stat.SpaceStk = s

		arr := []*Response{}
		stat.orderArray = arr
		stat.ProfitStk.Sym = kline_data.Sym
		stat.ProfitStk.Accountname = kline_data.Accountname
		stat.ProfitStk.Stockcode = kline_data.Stockcode

		stat.SpaceStk.Sym = kline_data.Sym
		stat.SpaceStk.Accountname = kline_data.Accountname
		stat.SpaceStk.Stockcode = kline_data.Stockcode

		var acc_map smap.Map
		if mapResult.Exists(user) {
			acc_map = (mapResult.Value(user)).(smap.Map)

			if acc_map.Exists(account) {
				stock_map := acc_map.Value(account).(smap.Map)
				if stock_map.Exists(kline_data.Stockcode) {

					stat = (stock_map.Value(kline_data.Stockcode)).(*STK)

				} else {

					stock_map.Set(kline_data.Stockcode, stat)

				}

			} else {

				stock_map := smap.New(true)
				stock_map.Set(kline_data.Stockcode, stat)
				acc_map.Set(account, stock_map)

			}
		} else {

			stock_map := smap.New(true)
			stock_map.Set(kline_data.Stockcode, stat)
			acc_map = smap.New(true)

			acc_map.Set(account, stock_map)

			mapResult.Set(user, acc_map)

		}
		//			stat.orderArray = append(stat.orderArray, kline_data)
		DoCalculateSTK(kline_data, stat)
	}
	//	}
}

func GetTransaction() {

	for {
		var con *kdb.KDBConn
		var err error
		con, err = kdb.DialKDB("127.0.0.1", 3900, "")
		//	con, err = kdb.DialKDB("139.196.77.165", 5033, "")
		if err != nil {
			fmt.Printf("Failed to connect kdb: %s", err.Error())
			return

		}

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
		fmt.Println("data_list:", data_list)
		table := data_list[2].Data.(kdb.Table)
		fmt.Println("table:", table)

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
			//			fmt.Println("get:", kline_data)
			//			fmt.Println("get2:", kline_data2)
			if kline_data.Askvol == 0 && kline_data2.Askvol != 0 {
				kline_data.Askvol = int32(kline_data2.Askvol)
				kline_data.Withdraw = int32(kline_data2.Withdraw)
				kline_data.Status = int32(kline_data2.Status)
				kline_data.Bidvol = int32(kline_data2.Bidvol)
			}
			handleData(kline_data)

		}

	}
}

func GetMarket() {
	for {
		fmt.Println("==GetMarket==", time.Now())
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
		err = con.AsyncCall(".u.sub", &kdb.K{-kdb.KS, kdb.NONE, "Market"}, &kdb.K{-kdb.KS, kdb.NONE, ""})
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
			kline_data := &Market{}
			err := kdb.UnmarshalDict(table.Index(i), kline_data)

			if err != nil {
				fmt.Println("Failed to unmrshall dict ", err)
				continue
			}
			fmt.Println("getMarket:", kline_data)

			for _, user_map := range mapResult.Values() {
				for _, account_map := range (user_map.(smap.Map)).Values() {
					for _, stock_map := range (account_map.(smap.Map)).Values() {
						stat := stock_map.(*STK)
						if stat.SpaceStk.Stockcode == kline_data.Sym {
							DoRefresh(float64(kline_data.NMatch/10000), stat)
						}
					}

				}
			}

		}

	}
	marketChan <- 0
}

//新的统计方法，把订单数组每个都重新算一遍
func DoCalculateSTK(newOrder *Response, stk *STK) {

	fmt.Println("---DoCalculateSTK    newOrder:", newOrder)
	fmt.Println("---DoCalculateSTK    stk:", stk)
	//清除
	stk.SpaceStk.AvgPrice = 0
	stk.SpaceStk.OnlineProfit = 0
	stk.SpaceStk.SpaceVol = 0
	stk.ProfitStk.BidCount = 0
	stk.ProfitStk.BidMoneySum = 0
	stk.ProfitStk.BidNum = 0
	stk.ProfitStk.PastProfit = 0
	stk.ProfitStk.TotalTax = 0

	//之前的全部统计一遍
	for _, order := range stk.orderArray {

		if order.Bidvol != 0 && (order.Status == 2 || order.Status == 5 || order.Status == 4) {
			CalculateSingle(order, stk)
		}

	}
	//先统计新订单，再更新订单数组
	if newOrder.Status == 4 {
		CalculateSingle(newOrder, stk)
		var index int
		flag := false
		for i, order := range stk.orderArray {
			//			fmt.Println("iiiii   ", i)
			if newOrder.Entrustno == order.Entrustno && order.Status != 4 {
				index = i
				flag = true
				break
			}
		}
		if flag {
			updateArray(stk, index, newOrder)
		} else {
			stk.orderArray = append(stk.orderArray, newOrder)
		}
	} else if newOrder.Status == 2 || newOrder.Status == 5 {
		var index int
		flag := false
		for i, order := range stk.orderArray {
			if newOrder.Entrustno == order.Entrustno && order.Status != 4 {
				//算增量
				fmt.Println("---算增量----")
				x := &Response{}
				x.Bidvol = newOrder.Bidvol - order.Bidvol
				x.Bidprice = (newOrder.Bidprice*float64(newOrder.Bidvol) - order.Bidprice*float64(order.Bidvol)) / float64(newOrder.Bidvol-order.Bidvol)

				CalculateSingle(x, stk)
				index = i
				flag = true
				break
			}

		}
		if flag {
			updateArray(stk, index, newOrder)
		} else {
			stk.orderArray = append(stk.orderArray, newOrder)
		}
	} else {
		stk.orderArray = append(stk.orderArray, newOrder)
	}
}

func CalculateSingle(newOrder *Response, stat *STK) {

	fmt.Println("CalculateSingle---  vol:", newOrder.Bidvol, "   price:", newOrder.Bidprice, "  status:", newOrder.Status)

	stat.Lock()
	//StaticsResult为实时统计对象，每一个交易完成，刷下统计
	if newOrder.Bidvol != 0 {
		//每次买入刷新均价。然后每次实时价格减去均价不断出现浮动盈利
		//算仓位 不管买还是卖，仓位都是相加减

		var spaceTemp int32 = stat.SpaceStk.SpaceVol //临时对象记录下之前的仓位量
		var avgTemp float64 = stat.SpaceStk.AvgPrice //临时对象记录下之前的均价
		//卖的大于原有仓位
		var flag bool = false

		if AbsInt(newOrder.Bidvol) >= AbsInt(stat.SpaceStk.SpaceVol) {
			flag = true
		}

		stat.SpaceStk.SpaceVol = stat.SpaceStk.SpaceVol + newOrder.Bidvol
		fmt.Println("算仓位", stat.SpaceStk.SpaceVol)
		if newOrder.Bidvol > 0 {
			//算均价
			if spaceTemp < 0 {
				if flag {
					stat.SpaceStk.AvgPrice = Abs(newOrder.Bidprice)
				}

			} else {
				stat.SpaceStk.AvgPrice = Abs((stat.SpaceStk.AvgPrice*(float64(spaceTemp)) + newOrder.Bidprice*float64(newOrder.Bidvol)) / float64(stat.SpaceStk.SpaceVol))
			}

		} else {
			if spaceTemp > 0 {
				if flag {
					stat.SpaceStk.AvgPrice = Abs(newOrder.Bidprice)
				}

			} else {
				stat.SpaceStk.AvgPrice = Abs((stat.SpaceStk.AvgPrice*(float64(spaceTemp)) + newOrder.Bidprice*float64(newOrder.Bidvol)) / float64(stat.SpaceStk.SpaceVol))
			}
		}

		fmt.Println("算均价", stat.SpaceStk.AvgPrice)
		//算费用  买是万三  卖是千一加上万三
		var stattax float64
		if newOrder.Bidvol > 0 {
			stattax = Abs(float64(newOrder.Bidprice*float64(newOrder.Bidvol))) * 3 / 10000

		} else {
			stattax = Abs(float64(newOrder.Bidprice*float64(newOrder.Bidvol))) * 13 / 10000
		}
		fmt.Println("之前费用", stat.ProfitStk.TotalTax, "  本次费用  ", stattax)
		stat.ProfitStk.TotalTax = stat.ProfitStk.TotalTax + stattax
		stat.ProfitStk.TotalTax = Float64Fmt(stat.ProfitStk.TotalTax, 2)
		fmt.Println("算费用", stat.ProfitStk.TotalTax)

		//算利润

		var soldNum int32 = AbsInt(newOrder.Bidvol) //本笔卖出的量
		if flag {
			//卖的大于原有仓位

			soldNum = AbsInt(spaceTemp)

		} else {
			soldNum = AbsInt(newOrder.Bidvol)
		}
		if newOrder.Bidvol > 0 {
			if spaceTemp < 0 {
				g := (avgTemp - newOrder.Bidprice) * float64(soldNum)
				fmt.Println("ggggggggggggggain:", g, "soldNum", soldNum)
				stat.ProfitStk.PastProfit = stat.ProfitStk.PastProfit + g - stattax
			} else {
				stat.ProfitStk.PastProfit = stat.ProfitStk.PastProfit - stattax
			}

		} else if newOrder.Bidvol < 0 {
			if spaceTemp > 0 {
				g := (newOrder.Bidprice - avgTemp) * float64(soldNum)
				fmt.Println("ggggggggggggggain:", g, "soldNum", soldNum)
				stat.ProfitStk.PastProfit = stat.ProfitStk.PastProfit + g - stattax
			} else {
				stat.ProfitStk.PastProfit = stat.ProfitStk.PastProfit - stattax
			}

		}

		stat.ProfitStk.PastProfit = Float64Fmt(stat.ProfitStk.PastProfit, 2)
		fmt.Println("算利润", stat.ProfitStk.PastProfit)

		//算交易笔数
		stat.ProfitStk.BidCount = stat.ProfitStk.BidCount + 1

		//算交易股数
		//		fmt.Println("AbsInt(stat.ProfitStk.BidNum)  ::", AbsInt(stat.ProfitStk.BidNum), "  soldNum", soldNum)
		stat.ProfitStk.BidNum = stat.ProfitStk.BidNum + AbsInt(newOrder.Bidvol)
		fmt.Println("after  stat.ProfitStk.BidNum  ", stat.ProfitStk.BidNum)
		//算交易额

		stat.ProfitStk.BidMoneySum = stat.ProfitStk.BidMoneySum + Abs(float64(AbsInt(newOrder.Bidvol))*newOrder.Bidprice)

	}
	stat.Unlock()
}

func DoRefresh(nMatch float64, stat *STK) {
	stat.Lock()
	//非交易统计，每次实时价格减去均价和费用不断出现浮动盈利
	stat.SpaceStk.OnlineProfit = (float64(stat.SpaceStk.SpaceVol) * (nMatch - stat.SpaceStk.AvgPrice)) - (Abs(float64(nMatch*float64(stat.SpaceStk.SpaceVol))) * 13 / 10000)
	stat.SpaceStk.OnlineProfit = Float64Fmt(stat.SpaceStk.OnlineProfit, 64)
	stat.Unlock()
}

func gotest(i int) {
	fmt.Println("this is :", i)

	time.Sleep(time.Second * 1)
	tChan <- 0
}

func printMap() {
	for {
		//		fmt.Println("map:::", mapResult)
		fmt.Println("用户       账户         票     仓位     均价     浮盈   利润    笔数    股数      交易额     费用   ")

		for _, user_map := range mapResult.Values() {
			//累积每个用户的总浮动盈亏和 总利润
			var totalOnlineProfit float64
			var totalProfit float64
			for _, account_map := range (user_map.(smap.Map)).Values() {

				for _, stock_map := range (account_map.(smap.Map)).Values() {

					stat := stock_map.(*STK)
					totalOnlineProfit = totalOnlineProfit + stat.SpaceStk.OnlineProfit
					totalProfit = totalProfit + stat.ProfitStk.PastProfit
					fmt.Println(stat.SpaceStk.Sym, "  ", stat.SpaceStk.Accountname, "  ", stat.SpaceStk.Stockcode, "  ", stat.SpaceStk.SpaceVol, "   ", stat.SpaceStk.AvgPrice, "   ", stat.SpaceStk.OnlineProfit, "   ", stat.ProfitStk.PastProfit, "  ", stat.ProfitStk.BidCount, "  ", stat.ProfitStk.BidNum, "  ", stat.ProfitStk.BidMoneySum, "   ", stat.ProfitStk.TotalTax)

				}

			}
			fmt.Println("总浮动盈亏:", totalOnlineProfit, "总利润:", totalProfit)
		}

		time.Sleep(time.Second * 20)
	}
}

//
func Abs(f float64) float64 {
	if f < 0 {
		return float64(-f)
	}
	return float64(f)
}

func AbsInt(f int32) int32 {
	if f < 0 {
		return int32(-f)
	}
	return int32(f)
}
func Float64Fmt(f float64, prec int) float64 {
	a := strconv.FormatFloat(f, 'f', prec, 64)
	ff, err := strconv.ParseFloat(a, 64)
	if err != nil {
		fmt.Println(err)
	}
	return ff
}

func updateArray(stk *STK, index int, newOrder *Response) {
	//去除原先的同一委托订单 加入新的订单放入末尾
	fmt.Println("stk: %v  index %v  neworder  %v", stk, index, newOrder)
	if len(stk.orderArray) == 0 {
		stk.orderArray = append(stk.orderArray, newOrder)
	} else {
		if index == len(stk.orderArray)-1 {
			stk.orderArray[index] = newOrder
		} else {
			stk.orderArray = append(stk.orderArray[:index], stk.orderArray[index+1:]...)
			stk.orderArray = append(stk.orderArray, newOrder)
		}
	}
}

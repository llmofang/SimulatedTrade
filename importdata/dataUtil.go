package importdata

import (
	"bufio"
	"encoding/csv"
	//	"finance"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
	"github.com/llmofang/SimulatedTrade/util/conf"
	"github.com/llmofang/SimulatedTrade/util/string"

	"github.com/llmofang/kdbutils"
	"github.com/llmofang/kdbutils/tbls"
)

//此方法已经被READCSV取代
func ReadLine(fileName string) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	buf := bufio.NewReader(f)
	for {

		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		handler(line)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	return nil
}

func ReadCsv(fileName string, secNum int64, myKDB *kdbutils.Kdb) {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()
	reader := csv.NewReader(file)
	for {
		reader.TrimLeadingSpace = true
		record, err := reader.Read()

		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error:", err)
			return
		}

		handlerArray(record, myKDB)
		time.Sleep(time.Second * time.Duration(secNum))
	}

}

func handlerArray(strArray []string, myKDB *kdbutils.Kdb) {
	//windcode, date（nActionDay）, time, price(nmatch), volume(ivolume), turnover(iturnover), match_items(nNumTrades), interest(XX), trade_flag, bs_flag, acc_volume(??), acc_turnover(??),high(nhigh), low(nlow), open(nopen), pre_close(npreclose), bid_price1,bid_price2,bid_price3,bid_price4,bid_price5,bid_price6,bid_price7,bid_price8,bid_price9,bid_price10,bid_volume1,bid_volume2,bid_volume3,bid_volume4,bid_volume5,bid_volume6,bid_volume7,bid_volume8,bid_volume9,bid_volume10,ask_price1,ask_price2,ask_price3,ask_price4,ask_price5,ask_price6,ask_price7,ask_price8,ask_price9,ask_price10,bid_volume1,bid_volume2,bid_volume3,bid_volume4,bid_volume5,bid_volume6,bid_volume7,bid_volume8,bid_volume9,bid_volume10,ask_av_price()（nWeightedAvgAskPrice）, bid_av_price（nWeightedAvgBidPrice）, total_ask_volume(nTotalaskvolume), total_bid_volume(ntotalbidvolume)
	//code,wind_code,name,    date,time,price,volume,turover,match_items8,    interest,trade_flag,bs_flag,accvolume,accturover,high,low,open,pre_close17,   ask10,ask9,ask8,ask7,ask6,ask5,ask4,ask3,ask2,ask1,                       bid1,bid2,bid3,bid4,bid5,bid6,bid7,bid8,bid9,bid10,                     asize10,asize9,asize8,asize7,asize6,asize5,asize4,asize3,asize2,asize1,bsize1,bsize2,bsize3,bsize4,bsize5,bsize6,bsize7,bsize8,bsize9,bsize10,ask_av_price,bid_av_price,total_ask_volume,total_bid_volume
	//300218,300218.SZ,安利股份,20160923,92503000,256500,495300,12704445,104,  0,0,                32,495300,        12704445,256500,256500,256500,256700,    259900,259800,259000,258900,258000,257500,256800,256700,256600,256500,    256100,255000,254800,254200,254000,253200,252500,252200,251200,251100,  4900,5500,7000,3000,8800,1200,400000,586100,6400,296400,500,1500,600,400,100,4300,100,4300,4300,4300,261100,249300,1737409,38000
	//	p := fmt.Println
	//	p("strArray ", strArray)
	if strArray[0] == "code" {
		return
	}

	m := tbls.Market2{}
	m.Sym = strArray[0]
	m.SzWindCode = strArray[1]

	yearstr := (strArray[3])[0:4]
	monthstr := (strArray[3])[4:6]
	daystr := (strArray[3])[6:8]
	strlength := len(strArray[4])
	secondstr := strArray[4][strlength-5 : strlength-3]
	minutestr := strArray[4][strlength-7 : strlength-5]
	hourstr := strArray[4][0 : strlength-7]

	withNanos := yearstr + "-" + monthstr + "-" + daystr + " " + hourstr + ":" + minutestr + ":" + secondstr

	t, _ := time.Parse("2006-01-02 15:04:05", withNanos)
	m.Time = t

	m.NActionDay = stringutil.StrToInt32(yearstr)
	m.NMatch = stringutil.StrToInt32(strArray[5])
	m.IVolume = stringutil.StrToInt32(strArray[6])
	m.ITurnover = stringutil.StrToInt32(strArray[7])
	m.NNumTrades = stringutil.StrToInt32(strArray[8])
	m.NHigh = stringutil.StrToInt32(strArray[14])
	m.NLow = stringutil.StrToInt32(strArray[15])
	m.NOpen = stringutil.StrToInt32(strArray[16])
	m.NPreClose = stringutil.StrToInt32(strArray[17])

	m.NAskPrice10 = stringutil.StrToInt32(strArray[18])
	m.NAskPrice9 = stringutil.StrToInt32(strArray[19])
	m.NAskPrice8 = stringutil.StrToInt32(strArray[20])
	m.NAskPrice7 = stringutil.StrToInt32(strArray[21])
	m.NAskPrice6 = stringutil.StrToInt32(strArray[22])
	m.NAskPrice5 = stringutil.StrToInt32(strArray[23])
	m.NAskPrice4 = stringutil.StrToInt32(strArray[24])
	m.NAskPrice3 = stringutil.StrToInt32(strArray[25])
	m.NAskPrice2 = stringutil.StrToInt32(strArray[26])
	m.NAskPrice1 = stringutil.StrToInt32(strArray[27])

	m.NBidPrice1 = stringutil.StrToInt32(strArray[28])
	m.NBidPrice2 = stringutil.StrToInt32(strArray[29])
	m.NBidPrice3 = stringutil.StrToInt32(strArray[30])
	m.NBidPrice4 = stringutil.StrToInt32(strArray[31])
	m.NBidPrice5 = stringutil.StrToInt32(strArray[32])
	m.NBidPrice6 = stringutil.StrToInt32(strArray[33])
	m.NBidPrice7 = stringutil.StrToInt32(strArray[34])
	m.NBidPrice8 = stringutil.StrToInt32(strArray[35])
	m.NBidPrice9 = stringutil.StrToInt32(strArray[36])
	m.NBidPrice10 = stringutil.StrToInt32(strArray[37])

	m.NAskVol10 = stringutil.StrToInt32(strArray[38])
	m.NAskVol9 = stringutil.StrToInt32(strArray[39])
	m.NAskVol8 = stringutil.StrToInt32(strArray[40])
	m.NAskVol7 = stringutil.StrToInt32(strArray[41])
	m.NAskVol6 = stringutil.StrToInt32(strArray[42])
	m.NAskVol5 = stringutil.StrToInt32(strArray[43])
	m.NAskVol4 = stringutil.StrToInt32(strArray[44])
	m.NAskVol3 = stringutil.StrToInt32(strArray[45])
	m.NAskVol2 = stringutil.StrToInt32(strArray[46])
	m.NAskVol1 = stringutil.StrToInt32(strArray[47])

	m.NBidVol1 = stringutil.StrToInt32(strArray[48])
	m.NBidVol2 = stringutil.StrToInt32(strArray[49])
	m.NBidVol3 = stringutil.StrToInt32(strArray[50])
	m.NBidVol4 = stringutil.StrToInt32(strArray[51])
	m.NBidVol5 = stringutil.StrToInt32(strArray[52])
	m.NBidVol6 = stringutil.StrToInt32(strArray[53])
	m.NBidVol7 = stringutil.StrToInt32(strArray[54])
	m.NBidVol8 = stringutil.StrToInt32(strArray[55])
	m.NBidVol9 = stringutil.StrToInt32(strArray[56])
	m.NBidVol10 = stringutil.StrToInt32(strArray[57])

	m.NWeightedAvgAskPrice = stringutil.StrToInt32(strArray[58])
	m.NWeightedAvgBidPrice = stringutil.StrToInt32(strArray[59])
	m.NTotalAskVol = stringutil.StrToInt32(strArray[60])
	m.NTotalBidVol = stringutil.StrToInt32(strArray[61])

	arr := []tbls.Market2{}
	arr = append(arr, m)
	myKDB.FuncTable("upsert", "Market", arr)

}

func handler(line string) {
	fmt.Println("xxx:", line)
	//	strArray := strings.Split(line, ",")
	//	fmt.Println("straaaaaa:", strArray)

}

func Read(path string) string {
	fi, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer fi.Close()
	fd, err := ioutil.ReadAll(fi)
	fmt.Println(string(fd))
	return string(fd)
}

//根据json配置文件来读取CSV导入KDB,多个数据文件同时并行且定时。原本要借用cim的util.conf,发现不合适
func LoadCsvImportData(configfilePath string) {
	fmt.Println("----LoadCsvImportData--- start")
	myConfig := new(confutil.Config)
	myConfig.InitConfig(configfilePath)
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

	timeStr := myConfig.Read("default", "time")
	timeNum := int64(stringutil.StrToInt32(timeStr))
	portStr := myConfig.Read("default", "port")
	port := int(stringutil.StrToInt32(portStr))
	host := myConfig.Read("default", "host")

	fmt.Printf("host", host)
	fmt.Println("port", port)

	if host != "" && port != 0 {

		if timeNum == 0 {
			timeNum = 1
		}

		var business_chan chan int = make(chan int)

		for x := 0; x < len(filesArray); x++ {

			go handlerThread(x, filesArray[x], timeNum, host, port)
		}
		business_chan <- 0
		fmt.Println("----LoadCsvImportData--- doing")

		<-business_chan
		fmt.Println("----LoadCsvImportData--- finish")

	} else {
		fmt.Println("error: no  host and port in conf")
	}

}

func handlerThread(i int, filepath string, secNum int64, host string, port int) {
	//	fmt.Println("this is thead ", i, "now is  ", time.Now())
	myKDB := kdbutils.MewKdb(host, port)

	myKDB.Connect()

	fmt.Println("this is thead ", i, "now is  ", time.Now())
	ReadCsv(filepath, secNum, myKDB)

}

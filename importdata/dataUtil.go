package importdata

import (
	"bufio"
	"finance"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	//	"stat"
	//	"strconv"
	"strings"
	"time"
)

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

func handler(line string) {
	//windcode, date, time, price(nmatch), volume(ivolume), turnover(iturnover), match_items(??), interest(??), trade_flag, bs_flag, acc_volume(??), acc_turnover(??),high(nhigh), low(nlow), open(nopen), pre_close(npreclose), bid_price1,bid_price2,bid_price3,bid_price4,bid_price5,bid_price6,bid_price7,bid_price8,bid_price9,bid_price10,bid_volume1,bid_volume2,bid_volume3,bid_volume4,bid_volume5,bid_volume6,bid_volume7,bid_volume8,bid_volume9,bid_volume10,ask_price1,ask_price2,ask_price3,ask_price4,ask_price5,ask_price6,ask_price7,ask_price8,ask_price9,ask_price10,bid_volume1,bid_volume2,bid_volume3,bid_volume4,bid_volume5,bid_volume6,bid_volume7,bid_volume8,bid_volume9,bid_volume10,ask_av_price(), bid_av_price, total_ask_volume(nTotalaskvolume), total_bid_volume(ntotalbidvolume)
	p := fmt.Println
	p("xxx:", line)
	strArray := strings.Split(line, ",")
	if strArray[0] == "windcode" {
		return
	}
	m := &finance.Market{}
	//	for i, v := range strArray {
	//		fmt.Println("i:%d,v:%v", i, v)

	//	}
	m.SzWindCode = strArray[0]
	//strArray[1] ==20161010   strArray[2]==95424000
	yearstr := (strArray[1])[0:4]
	monthstr := (strArray[1])[4:6]
	daystr := (strArray[1])[6:8]
	strlength := len(strArray[2])
	secondstr := strArray[2][strlength-5 : strlength-3]
	minutestr := strArray[2][strlength-7 : strlength-5]
	hourstr := strArray[2][0 : strlength-7]
	p(secondstr, minutestr, hourstr)
	withNanos := yearstr + "-" + monthstr + "-" + daystr + " " + hourstr + ":" + minutestr + ":" + secondstr

	t, _ := time.Parse("2006-01-02 15:04:05", withNanos)
	m.Time = t

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

func DoMain() {
	//	err := Read("D:\\stock\\000001.csv")
	//	fmt.Println("", err)
	//	f := handle()
	err := ReadLine("D:\\stock\\000001.csv")
	//		finance.Market
	fmt.Println("", err)

}

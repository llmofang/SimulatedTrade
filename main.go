package main

import (
	//	"finance"
	"fmt"
	//	"stat"
	"github.com/llmofang/SimulatedTrade/importdata"
	"time"

	//	kdb "github.com/sv/kdbgo"

)

func main() {
	fmt.Println(time.Now())

	//	stat.DoMain()
	//	do := &finance.Response{}
	//	do.Sym = "sym220"
	//	do.Qid = "qid111"
	//	do.Time = time.Now()
	//	do.Entrustno = 119988733
	//	do.Stockcode = "stockcode1222"
	//	do.Accountname = "acount_1111"
	//	do.Askprice = 15.4
	//	do.Askvol = 780
	//	do.Bidprice = 18.9
	//	do.Bidvol = 1988
	//	do.Withdraw = 44444411
	//	do.Status = 4

	//	var con *kdb.KDBConn
	//	var err error

	//	con, err = kdb.DialKDB("127.0.0.1", 3900, "")
	//	if err != nil {
	//		fmt.Printf("Failed to connect kdb: %s", err.Error())
	//		return

	//	}
	//	finance.UpsertByInterface(con, do, "response", "upd")

	//	finance.Dopub(do, "upd")

	//	importdata.DoMain()
	//strArray[1] ==20161010

	//	st := &Stu{}
	//	t := time.Date(2017, time.January, 11, 15, 45, 0, 0, time.Local)
	//	end_time := time.Now()
	//	st.Mytime = end_time.Sub(t)
	//	st.Sym = "aaa"
	//	var con *kdb.KDBConn
	//	var err error

	//	con, err = kdb.DialKDB("127.0.0.1", 3900, "")
	//	if err != nil {
	//		fmt.Printf("Failed to connect kdb: %s", err.Error())
	//		return

	//	}
	//	finance.UpsertByInterface(con, st, "stu", "upsert")
	fmt.Println("xxx")
	fmt.Println("xxx")
	fmt.Println(time.Now())
	importdata.LoadCsvImportData("./config.txt")
}

type Stu struct {
	Sym    string
	Mytime time.Duration
}

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	"net"
	"net/http"
	"strconv"
	"time"
)


const (
	USERNAME = "root"
	PASSWORD = "123456"
	NETWORK  = "tcp"
	SERVER   = "127.0.0.1"
	PORT     = 3306
	DATABASE = "test"
	RedisProxy = "127.0.0.1:6379"
)


type OfflineBuilding struct {
	Id   int64   `db:"id"`
	Name string  `db:"building_name"`
	Lat  float64 `db:"lat"`
	Lon  float64 `db:"lon"`
	Addr string  `db:"addr"`
}

DataClient = redis.NewClient(&redis.Options{
	Addr:        RedisProxy,
	Password:    "test",
	PoolSize:    512,
	PoolTimeout: time.Second * time.Duration(5)})


func main() {

	go queryMulti()
	//http server
	l, err := net.Listen("tcp", ":5566")
	if err != nil {
		fmt.Println(err)
	}
	defer l.Close()
	http.HandleFunc("/siftings", http.HandlerFunc(mHandler))
	http.Serve(l, nil)

}

func queryMulti() {

	//init mysql
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", USERNAME, PASSWORD, NETWORK, SERVER, PORT, DATABASE)
	DB, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("Open mysql failed,err:%v\n", err)
		return
	}
	DB.SetConnMaxLifetime(100 * time.Second) //最大连接周期，超过时间的连接就close
	DB.SetMaxOpenConns(100)                  //设置最大连接数
	DB.SetMaxIdleConns(16)
	for {

		offlineBuilding := new(OfflineBuilding)

		sql_ := fmt.Sprintf("SELECT b.id,b.building_name,b.lat,b.lon ,b.addr FROM offline_building b ")

		rows, err := DB.Query(sql_)
		defer func() {
			if rows != nil {
				rows.Close()
			}
		}()
		if err != nil {
			fmt.Printf("Query failed,err:%v", err)
			return
		}
		//从mysql中获取楼宇的位置信息并以 geo 的结构 存储到redis中
		for rows.Next() {
			err = rows.Scan(&offlineBuilding.Id, &offlineBuilding.Name, &offlineBuilding.Lat, &offlineBuilding.Lon, &offlineBuilding.Addr)
			if err != nil {
				fmt.Printf("Scan failed,err:%v", err, " row: ", rows)
				continue
			}
			result := *offlineBuilding
			fmt.Println(result)

			DataClient.GeoAdd("siftings", &redis.GeoLocation{Latitude: result.Lat, Longitude: result.Lon, Name: strconv.FormatInt(result.Id, 10)}).Val()

		}
		time.Sleep(time.Hour * 12)
	}

}

type Result struct {
	Data []int64 `json:"data"`
}

func mHandler(w http.ResponseWriter, r *http.Request) {

	defer func() {
		if err := recover(); err != nil {
			fmt.Println("error...")
			w.WriteHeader(http.StatusBadGateway)
		}
	}()

	defer r.Body.Close()
	if r.Method != "GET" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	result := Result{}
	resarr := []int64{}
	r.ParseForm()

	lon := r.Form.Get("lon")
	lat := r.Form.Get("lat")
	rad := r.Form.Get("rad")

	lon_f, _ := strconv.ParseFloat(lon, 64)
	lat_f, _ := strconv.ParseFloat(lat, 64)
	rad_f, _ := strconv.ParseFloat(rad, 64)

	fmt.Println("query param, ", "lon: ", lon_f, " lat: ", lat_f, " rad: ", rad_f)
	query := &redis.GeoRadiusQuery{
		Radius: rad_f,
		Unit:   `km`,
	}

	redis_val, err := DataClient.GeoRadius("siftings", lon_f, lat_f, query).Result()
	if err != nil {
		fmt.Println("geo redis error: ", err)
	}
	for _, v := range redis_val {
		bid, _ := strconv.ParseInt(v.Name, 10, 64)
		resarr = append(resarr, bid)
	}
	result.Data = resarr
	resu, _ := json.Marshal(result)
	fmt.Fprint(w, string(resu))
	fmt.Println("response data: ", string(resu))

}

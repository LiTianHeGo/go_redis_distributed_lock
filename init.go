package go_redis_distributed_lock

import (
	"fmt"
	"log"

	"github.com/go-redis/redis"
)

var Rclient *redis.Client

func init() {
	Rclient = ReturnsRclient()
}

func ReturnsRclient() *redis.Client {
	var err error

	// 创建链接
	Db := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", "IP", "Port"),
		Password: "Password", // no password set
		DB:       0,          // use default DB
	})
	_, err = Db.Ping().Result()
	if err != nil {
		log.Fatalf("redis连接失败:%v \n", err)
	}
	return Db
}

package utils

import (
	"log"

	"github.com/go-redis/redis"
)

func NewRclient(addr, pwd string) *redis.Client {
	var err error

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pwd,
		DB:       0, // use default DB
	})
	_, err = client.Ping().Result()
	if err != nil {
		log.Fatalf("redis连接失败:%v \n", err)
	}
	return client
}

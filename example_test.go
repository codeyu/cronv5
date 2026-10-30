package cron_test

import (
	"context"
	"fmt"
	"time"

	"github.com/codeyu/cronv5"
)

func Example() {
	c := cron.New().Quiet().Build()
	_, _ = c.AddJob("30 * * * *", func() { fmt.Println("Every hour on the half hour") })
	_, _ = c.AddJob("@hourly", func() { fmt.Println("Every hour") })
	_, _ = c.AddJob("@every 1h30m", func() { fmt.Println("Every hour thirty") })
	c.Start()
	_, _ = c.AddJob("@daily", func() { fmt.Println("Every day") })
	fmt.Println(len(c.Entries()))
	<-c.Stop()
	// Output: 4
}

func ExampleCron_Shutdown() {
	c := cron.New().Quiet().Build()
	c.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		fmt.Println("some jobs were cancelled:", err)
	}
	fmt.Println("stopped")
	// Output: stopped
}

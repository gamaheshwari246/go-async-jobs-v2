package main

import (
	"testing"

	"github.com/hibiken/asynq"
)

func TestLoadSchedule(t *testing.T) {
	config, err := loadSchedule(func(k string) string { return "" })
	if err != nil || config.Cron != "*/1 * * * *" || config.Job.Queue != "default" || config.Job.Type != "demo.sleep" {
		t.Fatalf("unexpected defaults %+v %v", config, err)
	}
	custom := map[string]string{"CRON_SCHEDULE": "0 9 * * *", "CRON_PRIORITY": "9", "CRON_PAYLOAD": "{\"duration_ms\":30}"}
	config, err = loadSchedule(func(k string) string { return custom[k] })
	if err != nil || config.Cron != "0 9 * * *" || config.Job.Queue != "critical" {
		t.Fatalf("unexpected custom: %+v %v", config, err)
	}
	for _, field := range []string{"CRON_PRIORITY", "CRON_PAYLOAD", "CRON_TASK_TYPE"} {
		bad := map[string]string{"CRON_PRIORITY": "not-int", "CRON_PAYLOAD": "not-json", "CRON_TASK_TYPE": "not-registered"}
		_, err := loadSchedule(func(k string) string {
			if k == field {
				return bad[k]
			}
			return ""
		})
		if err == nil {
			t.Fatalf("accepted invalid %s", field)
		}
	}
}
func TestInvalidCronExpression(t *testing.T) {
	s := asynq.NewScheduler(asynq.RedisClientOpt{Addr: "127.0.0.1:6379"}, nil)
	_, err := s.Register("not a cron expression", asynq.NewTask("demo.sleep", []byte("{}")))
	if err == nil {
		t.Fatal("invalid cron accepted")
	}
}

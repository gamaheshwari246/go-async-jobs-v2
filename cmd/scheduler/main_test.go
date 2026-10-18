package main
import (
    "testing"
    "time"
    "example.com/go-async-jobs/internal/spec"
)
func TestLoadSchedule(t *testing.T) {
    cfg,err:=loadSchedule(func(k string) string {return ""})
    if err!=nil || cfg.Cron!="*/1 * * * *" || cfg.Job.Queue!="default" || cfg.Job.Type!="demo.sleep" {t.Fatalf("unexpected defaults %+v %v",cfg,err)}
    custom:=map[string]string{"CRON_SCHEDULE":"0 9 * * *","CRON_PRIORITY":"9","CRON_PAYLOAD":"{\"duration_ms\":30}"}
    cfg,err=loadSchedule(func(k string) string {return custom[k]})
    if err!=nil || cfg.Cron!="0 9 * * *" || cfg.Job.Queue!="critical" {t.Fatalf("unexpected custom %+v %v",cfg,err)}
    for _,field:=range []string{"CRON_PRIORITY","CRON_PAYLOAD","CRON_TASK_TYPE","CRON_SCHEDULE"} {
        bad:=map[string]string{"CRON_PRIORITY":"not-int","CRON_PAYLOAD":"not-json","CRON_TASK_TYPE":"not-registered","CRON_SCHEDULE":"invalid cron"}
        _,err:=loadSchedule(func(k string) string {if k==field{return bad[k]};return ""})
        if err==nil {t.Fatalf("accepted invalid %s",field)}
    }
}
func TestAlignedTicksAcrossReplicas(t *testing.T){
    for _,expr:=range []string{"*/1 * * * *","@every 3s","@hourly"} {
        a,err:=parseSchedule(expr);if err!=nil{t.Fatal(err)}
        b,err:=parseSchedule(expr);if err!=nil{t.Fatal(err)}
        instant:=time.Date(2026,10,18,12,0,1,400000000,time.UTC)
        x:=a.Next(instant)
        y:=b.Next(instant.In(time.FixedZone("IST",19800)))
        if !x.Equal(y) || !x.After(instant){t.Fatalf("%s differs: %s vs %s",expr,x,y)}
    }
    if _,err:=parseSchedule("@every 100ms");err==nil {t.Fatal("invalid interval accepted")}
}
func TestTaskIDStability(t *testing.T) {
    job:=spec.Validated{Type:"demo.sleep",Queue:"default",Retries:3,Timeout:time.Second,Body:[]byte("{\"payload\":{}}")}
    cfg:=scheduleConfig{Cron:"*/1 * * * *",Job:job}
    tick:=time.Date(2026,10,18,12,0,0,0,time.UTC)
    id:=taskID(cfg,tick)
    if id!=taskID(cfg,tick.In(time.FixedZone("IST",19800))) {t.Fatal("timezone changed task ID")}
    if id==taskID(cfg,tick.Add(time.Minute)) {t.Fatal("separate ticks collided")}
    cfg.Job.Retries=2
    if id==taskID(cfg,tick) {t.Fatal("different options collided")}
}

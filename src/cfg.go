package main

import (
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v3"
	"os"
	"path"
	"runtime"
	"strings"
)

const appName = "TA-Consumer"

var cfg struct {
	Forward struct {
		Url         string `yaml:"url"`
		Concurrency int    `yaml:"concurrency"`
		MaxRetries  int    `yaml:"max-retries"`
		Compress    bool   `yaml:"compress"`
	} `yaml:"forward"`

	Log struct {
		Dir   string `yaml:"dir"`
		Level string `yaml:"level"`
		Name  string `yaml:"name"`
	} `yaml:"log"`

	Kafka struct {
		Brokers string `yaml:"brokers"`
		GroupId string `yaml:"group-id"`
		Topic   string `yaml:"topic"`
	}

	AppId map[string]string `yaml:"app-id"`
}

func init() {
	log.SetOutput(os.Stdout)
	log.SetReportCaller(false)
	log.SetFormatter(&log.TextFormatter{
		ForceColors:     true,
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
		CallerPrettyfier: func(f *runtime.Frame) (function string, file string) {
			return "", fmt.Sprintf(" %s:%d", path.Base(f.File), f.Line)
		},
	})
	ptrHelp := flag.Bool("h", false, "print this help")
	ptrCfg := flag.String("c", "etc/ta-consumer.yml", "config file")
	ptrDaemon := flag.Bool("d", false, "run as daemon")
	ptrCmd := flag.String("s", "", "[stop | restart | status]")
	flag.Parse()
	if *ptrHelp {
		flag.PrintDefaults()
		os.Exit(0)
	}
	if err := parseYaml(*ptrCfg, &cfg); err != nil {
		fmt.Printf("%sload %s err: %v%s\n", RED, *ptrCfg, err, NC)
		os.Exit(1)
	}
	initLog()

	if len(cfg.AppId) == 0 {
		log.Fatal("app-id is required")
	}
	if cfg.Kafka.Brokers == "" {
		log.Fatal("Kafka brokers is required")
	}
	if cfg.Kafka.Topic == "" {
		cfg.Kafka.Topic = "ta-data"
	}
	if cfg.Kafka.GroupId == "" {
		cfg.Kafka.GroupId = "em-ta-consumer"
	}

	pidFile := path.Join(cfg.Log.Dir, "pid")
	proc := findProc(pidFile)
	switch *ptrCmd {
	case "stop":
		if !isAlive(proc) {
			fmt.Printf("%s%s is NOT running%s\n", YELLOW, appName, NC)
			os.Exit(1)
		}
		kill(proc)
		fmt.Printf("%s%s stopped OK%s\n", BLUE, appName, NC)
		os.Exit(0)
	case "start":
		*ptrDaemon = true
	case "restart":
		if isAlive(proc) {
			kill(proc)
			fmt.Printf("%s%s stopped OK%s\n", BLUE, appName, NC)
		}
		*ptrDaemon = true
	case "status":
		if isAlive(proc) {
			fmt.Printf("%s%s is running with PID %s%d%s\n", BLUE, appName, GREEN, proc.Pid, NC)
		} else {
			fmt.Printf("%s%s is %sNOT%s running%s\n", BLUE, appName, RED, BLUE, NC)
		}
		os.Exit(0)
	case "":
	default:
		flag.PrintDefaults()
		os.Exit(1)
	}

	if isAlive(proc) {
		fmt.Printf("%s%s is running, can't start, PID%s %d%s\n", RED, appName, GREEN, proc.Pid, NC)
		os.Exit(1)
	}
	if *ptrDaemon {
		daemonize(pidFile)
	}
}

func parseYaml(path string, out any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return yaml.NewDecoder(file).Decode(out)
}

func initLog() {
	switch strings.ToLower(cfg.Log.Level) {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}
	if cfg.Log.Dir != "" {
		if err := os.MkdirAll(cfg.Log.Dir, 0755); err != nil {
			log.Fatal("make log dir err:", err)
		}
	}
	if cfg.Log.Name == "" {
		return
	}
	log.SetOutput(&lumberjack.Logger{
		Filename:   path.Join(cfg.Log.Dir, cfg.Log.Name),
		LocalTime:  true,
		MaxSize:    64, // megabytes
		MaxAge:     7,  // days
		MaxBackups: 7,
		Compress:   true,
	})
}

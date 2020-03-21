package main

import (
	"bufio"
	"context"
	"fmt"
	ui "github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/widgets"
	orm "github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientgo "k8s.io/client-go/kubernetes"
	typeappsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	"k8s.io/client-go/tools/clientcmd"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	KUBE_CONFIG = `k8s config`//yaml
	QUERY_REPAYMENT_CHANNEL_SQL = "select repayment_channel,channel_nickname from repayment_channel_config order by repayment_channel"
	CHANNEL_REGEXP              = `REPAYMENT_(?P<channel>.+)_DISABLED`
)

//操作
const (
	OPEN_CONSUME Operation = iota + 1
	CLOSE_CONSUME
	OPEND_REPAYMENT
	CLOSE_REPAYMENT
	OPEN_ALL
	CLOSE_ALL
)

//通道状态
const (
	CONSUME_CLOSED   ChannelStatus = "CONSUME"
	REPAYMENT_CLOSED ChannelStatus = "REPAYMENT"
	ALL_CLOSED       ChannelStatus = "ALL"
	OPEN             ChannelStatus = "OPEN"
)

var (
	deploymentClient              typeappsv1.DeploymentInterface
	deployment                    *appsv1.Deployment
	DB                            *orm.DB
	zhStatusMap                   ChannelStatusMap
	originRepaymentChannelInfoMap = make(map[ChannelName]*RepaymentChannelInfo)
	updateRepaymentChannelInfoMap = make(map[ChannelName]*RepaymentChannelInfo)
	opMap                         OpMap
)

type Operation = int
type ChannelStatus string
type ChannelName string

type ChannelStatusMap map[ChannelStatus]string

func (m *ChannelStatusMap) getOrDefault(s *ChannelStatus) string {
	if n, ok := (*m)[*s]; ok {
		return n
	} else {
		return "正常交易"
	}
}

type OpMap map[Operation]func(ChannelName)

func init() {
	//初始化集群配置
	initK8s()
	//初始化数据库连接
	initDB()
	//初始化通道状态
	initStatus()
	//初始化操作方法
	initOpMap()
}

func main() {
	defer DB.Close()
	var infos []string
	envs := channelEnvs()
	for _, config := range findAllRepaymentConfig() {
		info := &RepaymentChannelInfo{
			RepaymentConfig: config,
		}
		if envVar, ok := envs[config.RepaymentChannel]; ok && isStatus(&envVar.Value) {
			info.ChannelStatus = ChannelStatus(envVar.Value)
		} else {
			info.ChannelStatus = OPEN
		}
		originRepaymentChannelInfoMap[config.RepaymentChannel] = info
		infos = append(infos, info.ToString())
	}
	sort.Strings(infos)
	drawList(infos)
}

func initOpMap() {
	opMap = make(OpMap, 6)
	opMap[OPEN_CONSUME] = func(name ChannelName) {
		info := originRepaymentChannelInfoMap[name]
		switch info.ChannelStatus {
		case CONSUME_CLOSED:
			info.ChannelStatus = OPEN
			updateRepaymentChannelInfoMap[name] = info
		case ALL_CLOSED:
			info.ChannelStatus = REPAYMENT_CLOSED
			updateRepaymentChannelInfoMap[name] = info
		}
	}
	opMap[CLOSE_CONSUME] = func(name ChannelName) {
		info := originRepaymentChannelInfoMap[name]
		switch info.ChannelStatus {
		case REPAYMENT_CLOSED:
			info.ChannelStatus = ALL_CLOSED
			updateRepaymentChannelInfoMap[name] = info
		case OPEN:
			info.ChannelStatus = CONSUME_CLOSED
			updateRepaymentChannelInfoMap[name] = info
		}
	}
	opMap[OPEND_REPAYMENT] = func(name ChannelName) {
		info := originRepaymentChannelInfoMap[name]
		switch info.ChannelStatus {
		case REPAYMENT_CLOSED:
			info.ChannelStatus = OPEN
			updateRepaymentChannelInfoMap[name] = info
		case ALL_CLOSED:
			info.ChannelStatus = CONSUME_CLOSED
			updateRepaymentChannelInfoMap[name] = info
		}
	}
	opMap[CLOSE_REPAYMENT] = func(name ChannelName) {
		info := originRepaymentChannelInfoMap[name]
		switch info.ChannelStatus {
		case CONSUME_CLOSED:
			info.ChannelStatus = ALL_CLOSED
			updateRepaymentChannelInfoMap[name] = info
		case OPEN:
			info.ChannelStatus = REPAYMENT_CLOSED
			updateRepaymentChannelInfoMap[name] = info
		}
	}
	opMap[OPEN_ALL] = func(name ChannelName) {
		info := originRepaymentChannelInfoMap[name]
		switch info.ChannelStatus {
		case CONSUME_CLOSED, REPAYMENT_CLOSED, ALL_CLOSED:
			info.ChannelStatus = OPEN
			updateRepaymentChannelInfoMap[name] = info
		}
	}
	opMap[CLOSE_ALL] = func(name ChannelName) {
		info := originRepaymentChannelInfoMap[name]
		switch info.ChannelStatus {
		case OPEN, REPAYMENT_CLOSED, CONSUME_CLOSED:
			info.ChannelStatus = ALL_CLOSED
			updateRepaymentChannelInfoMap[name] = info
		}
	}
}
func initStatus() {
	zhStatusMap = make(ChannelStatusMap, 3)
	zhStatusMap[CONSUME_CLOSED] = "消费关闭"
	zhStatusMap[REPAYMENT_CLOSED] = "代付关闭"
	zhStatusMap[ALL_CLOSED] = "交易关闭"
}
func initDB() {
	url := "db url"
	db, err := orm.Open("mysql", url)
	exit(err)
	db.DB().SetMaxIdleConns(10)
	db.DB().SetMaxOpenConns(10)
	DB = db
}
func initK8s() {
	config, err := clientcmd.NewClientConfigFromBytes([]byte(KUBE_CONFIG))
	exit(err)
	clientConfig, err := config.ClientConfig()
	exit(err)
	clientsSet, err := clientgo.NewForConfig(clientConfig)
	exit(err)
	namespace := "quhuan"
	deploymentClient = clientsSet.AppsV1().Deployments(namespace)
	deployName := "repayment-admin-jobs"
	deployment, err = deploymentClient.Get(context.TODO(), deployName, metav1.GetOptions{})
	exit(err)
}

type RepaymentConfig struct {
	RepaymentChannel ChannelName `gorm:"primary_key"`
	ChannelNickname  string
}

func (*RepaymentConfig) TableName() string {
	return "repayment_channel_config"
}

func (r *RepaymentConfig) ToString() string {
	var sb strings.Builder
	sb.WriteString(string(r.RepaymentChannel))
	sb.WriteString(" <=> ")
	sb.WriteString(r.ChannelNickname)
	return sb.String()
}

type RepaymentChannelInfo struct {
	RepaymentConfig
	ChannelStatus
}

func (r *RepaymentChannelInfo) ToString() string {
	var sb strings.Builder
	sb.WriteString(r.RepaymentConfig.ToString())
	sb.WriteString(" <=> ")
	sb.WriteString(zhStatusMap.getOrDefault(&r.ChannelStatus))
	return sb.String()
}

//判断是不是有效的通道状态
func isStatus(s *string) bool {
	return ChannelStatus(*s) == CONSUME_CLOSED ||
		ChannelStatus(*s) == REPAYMENT_CLOSED ||
		ChannelStatus(*s) == ALL_CLOSED ||
		ChannelStatus(*s) == OPEN
}

func findAllRepaymentConfig() []RepaymentConfig {
	var result []RepaymentConfig
	DB.Find(&result)
	return result
}

//遇错记录并退出
func exit(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

//从deployment环境变量中提取通道名
func extractChannelName(env *string) (string, bool) {
	// 使用命名分组，显得更清晰
	re := regexp.MustCompile(CHANNEL_REGEXP)
	match := re.FindStringSubmatch(*env)
	if len(match) == 0 {
		return "", false
	}
	return match[1], true
}

//绘制列表
func drawList(rows []string) {
	if err := ui.Init(); err != nil {
		log.Fatalf("failed to initialize termui: %v", err)
	}
	defer ui.Close()
	l := widgets.NewList()
	l.Rows = rows
	l.Title = "通道列表"
	l.SetRect(0, 30, 80, 7)
	l.TextStyle = ui.NewStyle(ui.ColorYellow)

	p := widgets.NewParagraph()
	p.Title = "按键指导"
	p.Text = `1.开启消费交易==2.关闭消费交易
3.开启代付交易==4.关闭代付交易
5.开启所有交易==6.关闭所有交易 
q.放弃修改并退出==s.确认修改并退出`
	p.SetRect(0, 0, 80, 6)
	p.TextStyle.Fg = ui.ColorWhite
	p.BorderStyle.Fg = ui.ColorCyan

	ui.Render(l, p)

	ech := ui.PollEvents()
	for {
		e := <-ech
		switch e.ID {
		case "q", "<C-c>":
			return
		case "<Down>":
			l.ScrollDown()
		case "<Up>":
			l.ScrollUp()
		case "1", "2", "3", "4", "5", "6":
			id := e.ID
			if i, err := strconv.Atoi(id); err == nil {
				name := extractChannelNameFromListRow(l)
				if f, ok := opMap[i]; ok {
					f(ChannelName(name))
					var infos []string
					for _, info := range originRepaymentChannelInfoMap {
						infos = append(infos, info.ToString())
					}
					sort.Strings(infos)
					l.Rows = infos
				}
			}
		case "s":
			doUpdate()
			return
		}

		ui.Render(l)
	}
}

//从表行中提取通道名
func extractChannelNameFromListRow(l *widgets.List) string {
	row := l.Rows[l.SelectedRow]
	split := strings.Split(row, " <=> ")
	return split[0]
}

//获取通道相关的环境变量
func channelEnvs() map[ChannelName]corev1.EnvVar {
	containers := deployment.Spec.Template.Spec.Containers
	envVars := containers[0].Env
	result := make(map[ChannelName]corev1.EnvVar, len(envVars))
	for _, envVar := range envVars {
		if name, ok := extractChannelName(&envVar.Name); ok {
			result[ChannelName(name)] = envVar
		}
	}
	return result
}

//更新
func doUpdate() {
	checkAndRecordTime()
	containers := deployment.Spec.Template.Spec.Containers
	envVars := containers[0].Env
	var newEnvs []corev1.EnvVar
	toDelKeys := make([]ChannelName, 1)
	for _, envVar := range envVars {
		if name, ok := extractChannelName(&envVar.Name); ok { //环境变量是通道操作
			if info, ok := updateRepaymentChannelInfoMap[ChannelName(name)]; ok { //在更新列表中
				if info.ChannelStatus == ChannelStatus(envVar.Value) { //状态不变,保留环境变量,从更新列表中移除
					newEnvs = append(newEnvs, envVar)
					toDelKeys = append(toDelKeys, info.RepaymentChannel)
				} else if !isStatus(&envVar.Value) { //无效的通道环境变量值,表示通道开启,不写入环境变量
					toDelKeys = append(toDelKeys, info.RepaymentChannel)
				}
			} else if isStatus(&envVar.Value) { //不在更新列表中直接保留
				newEnvs = append(newEnvs, envVar)
			}
		} else { //不是通道操作直接保留
			newEnvs = append(newEnvs, envVar)
		}
	}
	for _, delKey := range toDelKeys {
		delete(updateRepaymentChannelInfoMap, delKey)
	}
	for name, info := range updateRepaymentChannelInfoMap {
		if info.ChannelStatus == OPEN { //在更新列表,但状态为开启的,直接不写入环境变量
			continue
		}
		var sb strings.Builder
		sb.WriteString("REPAYMENT_")
		sb.WriteString(string(name))
		sb.WriteString("_DISABLED")
		envVar := corev1.EnvVar{
			Name:  sb.String(),
			Value: string(info.ChannelStatus),
		}
		newEnvs = append(newEnvs, envVar)
	}
	containers[0].Env = newEnvs
	_, err := deploymentClient.Update(context.TODO(), deployment, metav1.UpdateOptions{})
	exit(err)
	//清空
	updateRepaymentChannelInfoMap = make(map[ChannelName]*RepaymentChannelInfo)
}

//检查频率,记录交易时间
func checkAndRecordTime() {
	recordFilePath := filepath.Join(string(filepath.Separator), "tmp", ".km_last_update.txt")
	timeFormat := "2006-01-02 15:04:05"
	now := time.Now()
	if f, err := os.Open(recordFilePath); err == nil {
		scanner := bufio.NewScanner(f)
		if scanner.Scan() {
			lastUpdateTime, err := time.Parse(timeFormat, scanner.Text())
			exit(err)
			duration := now.Sub(lastUpdateTime)
			if duration < time.Minute*5 {
				log.Fatal("更新频率过高")
			}
		}
		_ = f.Close()
	}
	if f, err := os.OpenFile(recordFilePath, os.O_TRUNC|os.O_WRONLY, 0755); err == nil {
		_, _ = fmt.Fprintf(f, now.Format(timeFormat))
		_ = f.Close()
	}
}

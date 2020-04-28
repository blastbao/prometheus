// Copyright 2013 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/version"

	"github.com/blastbao/prometheus/config"
	"github.com/blastbao/prometheus/discovery/targetgroup"
	"github.com/blastbao/prometheus/pkg/labels"
	"github.com/blastbao/prometheus/pkg/relabel"
	"github.com/prometheus/alertmanager/api/v2/models"
)

const (
	contentTypeJSON = "application/json"
)

// String constants for instrumentation.
const (
	namespace         = "prometheus"
	subsystem         = "notifications"
	alertmanagerLabel = "alertmanager"
)

var userAgent = fmt.Sprintf("Prometheus/%s", version.Version)

// Alert is a generic representation of an alert in the Prometheus eco-system.
type Alert struct {

	// Label value pairs for purpose of aggregation, matching, and disposition dispatching.
	// This must minimally include an "alertname" label.
	Labels labels.Labels `json:"labels"`

	// Extra key/value information which does not define alert identity.
	Annotations labels.Labels `json:"annotations"`

	// The known time range for this alert. Both ends are optional.
	StartsAt     time.Time `json:"startsAt,omitempty"`
	EndsAt       time.Time `json:"endsAt,omitempty"`
	GeneratorURL string    `json:"generatorURL,omitempty"`
}

// Name returns the name of the alert. It is equivalent to the "alertname" label.
func (a *Alert) Name() string {
	return a.Labels.Get(labels.AlertName)
}

// Hash returns a hash over the alert. It is equivalent to the alert labels hash.
func (a *Alert) Hash() uint64 {
	return a.Labels.Hash()
}

func (a *Alert) String() string {
	s := fmt.Sprintf("%s[%s]", a.Name(), fmt.Sprintf("%016x", a.Hash())[:7])
	if a.Resolved() {
		return s + "[resolved]"
	}
	return s + "[active]"
}

// Resolved returns true iff the activity interval ended in the past.
func (a *Alert) Resolved() bool {
	return a.ResolvedAt(time.Now())
}

// ResolvedAt returns true iff the activity interval ended before
// the given timestamp.
func (a *Alert) ResolvedAt(ts time.Time) bool {
	if a.EndsAt.IsZero() {
		return false
	}
	return !a.EndsAt.After(ts)
}

// Manager is responsible for dispatching alert notifications to an alert manager service.
// Manager 负责向 alert manager 发送 alert。
type Manager struct {

	queue []*Alert				// 告警队列
	opts  *Options				// 配置

	metrics *alertMetrics		// metrics

	more   chan struct{}		// 是否有新告警的信号
	mtx    sync.RWMutex
	ctx    context.Context
	cancel func()

	alertmanagers map[string]*alertmanagerSet
	logger        log.Logger
}


// Options are the configurable parameters of a Handler.
type Options struct {
	QueueCapacity  int
	ExternalLabels labels.Labels
	RelabelConfigs []*relabel.Config

	// Used for sending HTTP requests to the Alertmanager.
	// 用于向 alert manager 发送 HTTP 请求。
	Do func(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error)

	// 用于注册 metrics
	Registerer prometheus.Registerer
}

type alertMetrics struct {
	latency                 *prometheus.SummaryVec
	errors                  *prometheus.CounterVec
	sent                    *prometheus.CounterVec
	dropped                 prometheus.Counter
	queueLength             prometheus.GaugeFunc
	queueCapacity           prometheus.Gauge
	alertmanagersDiscovered prometheus.GaugeFunc
}

func newAlertMetrics(r prometheus.Registerer, queueCap int, queueLen, alertmanagersDiscovered func() float64) *alertMetrics {
	m := &alertMetrics{
		latency: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  namespace,
			Subsystem:  subsystem,
			Name:       "latency_seconds",
			Help:       "Latency quantiles for sending alert notifications.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
			[]string{alertmanagerLabel},
		),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "errors_total",
			Help:      "Total number of errors sending alert notifications.",
		},
			[]string{alertmanagerLabel},
		),
		sent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "sent_total",
			Help:      "Total number of alerts sent.",
		},
			[]string{alertmanagerLabel},
		),
		dropped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "dropped_total",
			Help:      "Total number of alerts dropped due to errors when sending to Alertmanager.",
		}),
		queueLength: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queue_length",
			Help:      "The number of alert notifications in the queue.",
		}, queueLen),
		queueCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queue_capacity",
			Help:      "The capacity of the alert notifications queue.",
		}),
		alertmanagersDiscovered: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "prometheus_notifications_alertmanagers_discovered",
			Help: "The number of alertmanagers discovered and active.",
		}, alertmanagersDiscovered),
	}

	m.queueCapacity.Set(float64(queueCap))

	if r != nil {
		r.MustRegister(
			m.latency,
			m.errors,
			m.sent,
			m.dropped,
			m.queueLength,
			m.queueCapacity,
			m.alertmanagersDiscovered,
		)
	}

	return m
}

// do 函数用来发送 http 请求
func do(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(req.WithContext(ctx))
}

// NewManager is the manager constructor.
func NewManager(o *Options, logger log.Logger) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	// 初始化 http 请求客户端
	if o.Do == nil {
		o.Do = do
	}

	if logger == nil {
		logger = log.NewNopLogger()
	}

	// 构建 notifier 的 Manager 对象
	n := &Manager{
		queue:  make([]*Alert, 0, o.QueueCapacity), 	// 初始化告警队列
		ctx:    ctx,									// 利用context协同处理
		cancel: cancel,									// 级联式 cancel ctx 相关的协程
		more:   make(chan struct{}, 1),					// 是否有告警信息的信号管道
		opts:   o,										// 其他一些参数，包含告警队列总容量、label重置配置等
		logger: logger,									// 日志
	}

	// 返回告警队列的当前长度
	queueLenFunc := func() float64 { return float64(n.queueLen()) }

	// 获取处于 active 的 alertmanager 地址总数, 用于 notifier 发送告警信息
	alertmanagersDiscoveredFunc := func() float64 { return float64(len(n.Alertmanagers())) }

	// 生成 notifier 相关的一些 metrics，注册到 prometheus
	n.metrics = newAlertMetrics(
		o.Registerer,						// 注册的机制是: prometheus.Registerer
		o.QueueCapacity,					// 队列容量
		queueLenFunc,						// 队列当前长度
		alertmanagersDiscoveredFunc,		// 处于 active 的 alertmanager 地址个数
	)

	return n
}

// ApplyConfig updates the status state as the new config requires.




//# alerting:
//#   alertmanagers:
//#   - scheme: http
//#     static_configs:
//#     - targets:
//#       - 'am1:9093'
//#		  - 'am1:9094'
//#       - 'am1:9095'
//#		  - 'am1:9096'
//#   - scheme: http
//#     static_configs:
//#     - targets:
//#       - 'am2:9093'
//#		  - 'am2:9094'
//#       - 'am2:9095'
//#		  - 'am2:9096'

func (n *Manager) ApplyConfig(conf *config.Config) error {
	n.mtx.Lock()
	defer n.mtx.Unlock()

	// 配置文件 prometheus.yml 中 global 下的 external_labels ，用于外部系统标签的，不是用于 metrics 数据。
	n.opts.ExternalLabels = conf.GlobalConfig.ExternalLabels

	// 配置文件 prometheus.yml 中 alertingl 下 alert_relabel_configs ，动态修改 alert 属性的规则配置
	n.opts.RelabelConfigs = conf.AlertingConfig.AlertRelabelConfigs

	// map[config-i] => alertmanagerSet
	amSets := make(map[string]*alertmanagerSet)

	// 遍历告警相关的配置，即配置文件 prometheus.yml 的 alerting
	for k, cfg := range conf.AlertingConfig.AlertmanagerConfigs.ToMap() {

		// 把 alerting 下每个配置项，转换成 *alertmanagerSet 实例
		ams, err := newAlertmanagerSet(cfg, n.logger, n.metrics)
		if err != nil {
			return err
		}

		// 保存配置项到 amSet 中
		amSets[k] = ams
	}

	n.alertmanagers = amSets

	return nil
}

const maxBatchSize = 64

// 获取当前队列长队。
func (n *Manager) queueLen() int {
	n.mtx.RLock()
	defer n.mtx.RUnlock()
	return len(n.queue)
}

// 从 n.queue 中取出一批 alerts 以供发送，最多取出 maxBatchSize 个。
func (n *Manager) nextBatch() []*Alert {
	n.mtx.Lock()
	defer n.mtx.Unlock()

	var alerts []*Alert

	if len(n.queue) > maxBatchSize {
		alerts = append(make([]*Alert, 0, maxBatchSize), n.queue[:maxBatchSize]...)
		n.queue = n.queue[maxBatchSize:]
	} else {
		alerts = append(make([]*Alert, 0, len(n.queue)), n.queue...)
		n.queue = n.queue[:0]
	}

	return alerts
}

// Run dispatches notifications continuously.
func (n *Manager) Run(tsets <-chan map[string][]*targetgroup.Group) {

	for {
		select {
		// 退出信号
		case <-n.ctx.Done():
			return
		// 当告警服务有更新，重新加载它们。
		case ts := <-tsets:
			n.reload(ts)
		// 新告警信号：当有新告警信息 alerts 到达时，会通过 setMore() 函数触发本管道。
		case <-n.more:
		}

		// 从告警队列中批量获取告警信息
		alerts := n.nextBatch()

		// 发送告警 alerts 到 n.alertmanagers 上，判断是否发送成功，失败则上报。
		if !n.sendAll(alerts...) {
			n.metrics.dropped.Add(float64(len(alerts)))
		}

		// If the queue still has items left, kick off the next iteration.
		// 若告警队列中还有告警信息，则再次触发 n.more 信号
		if n.queueLen() > 0 {
			n.setMore()
		}
	}
}

// 若检测到告警服务有变动，则会调用 reload 方法，同步告警服务。
func (n *Manager) reload(tgs map[string][]*targetgroup.Group) {
	n.mtx.Lock()
	defer n.mtx.Unlock()

	// 遍历发生变更的配置组
	for id, tgroup := range tgs {

		// 判断配置组 id 是否已经在注册过，若未曾注册，直接忽略
		am, ok := n.alertmanagers[id]
		if !ok {
			level.Error(n.logger).Log("msg", "couldn't sync alert manager set", "err", fmt.Sprintf("invalid id:%v", id))
			continue
		}

		// 同步
		am.sync(tgroup)
	}
}

// Send queues the given notification requests for processing.
// Panics if called on a handler that is not running.
//
// Send 方法把 notifier.Alert 类型的告警信息添加到告警队列 n.queue 中，然后通知后台发送协程来发送它们。
func (n *Manager) Send(alerts ...*Alert) {

	n.mtx.Lock()
	defer n.mtx.Unlock()

	// Attach external labels before relabelling and sending.
	// 遍历 alerts ，在 relabel 和 send 之前把 n.opts.ExternalLabels 中的外部标签添加到告警上。（若冲突，不覆盖原始标签）
	for _, a := range alerts {
		lb := labels.NewBuilder(a.Labels)
		for _, l := range n.opts.ExternalLabels {
			if a.Labels.Get(l.Name) == "" {
				lb.Set(l.Name, l.Value)
			}
		}
		a.Labels = lb.Labels()
	}

	// 根据配置文件 prometheus.yml 的 alert_relabel_configs 下的 relabel_config 对告警的 label 进行重置（relabel）。
	alerts = n.relabelAlerts(alerts)
	if len(alerts) == 0 {
		return
	}

	// Queue capacity should be significantly larger than a single alert batch could be.
	// 若待告警信息的数量大于队列总容量，则移除待告警信息中最早的告警信息，依据的规则是先进先移除
	if d := len(alerts) - n.opts.QueueCapacity; d > 0 {
		alerts = alerts[d:]
		level.Warn(n.logger).Log("msg", "Alert batch larger than queue capacity, dropping alerts", "num_dropped", d)
		n.metrics.dropped.Add(float64(d))
	}

	// If the queue is full, remove the oldest alerts in favor of newer ones.
	// 若队列中已有的告警信息和待发送的告警信息大于队列的总容量，则从队列中移除最早的告警信息, 依据是先进先移除
	if d := (len(n.queue) + len(alerts)) - n.opts.QueueCapacity; d > 0 {
		n.queue = n.queue[d:]

		level.Warn(n.logger).Log("msg", "Alert notification queue full, dropping alerts", "num_dropped", d)
		n.metrics.dropped.Add(float64(d))
	}

	// 把处理后的待发送 alerts 添加到队列
	n.queue = append(n.queue, alerts...)

	// Notify sending goroutine that there are alerts to be processed.
	// 通知负责发送 alerts 的 goroutine 来处理这些新添加的 alerts 。
	n.setMore()
}

func (n *Manager) relabelAlerts(alerts []*Alert) []*Alert {
	var relabeledAlerts []*Alert

	for _, alert := range alerts {
		labels := relabel.Process(alert.Labels, n.opts.RelabelConfigs...)
		if labels != nil {
			alert.Labels = labels
			relabeledAlerts = append(relabeledAlerts, alert)
		}
	}
	return relabeledAlerts
}

// setMore signals that the alert queue has items.
//
// setMore 表示警报队列包含新添加的告警需要处理。
// setMore 方法相当于一个触发器，向管道 n.more 发送触发信息, 告知通 notifierManager 有告警信息需要处理，
// setMore 方法 是连接规则管理(ruleManager)和通知管理(notifierManager)的桥梁。
func (n *Manager) setMore() {
	// If we cannot send on the channel, it means the signal already exists and has not been consumed yet.
	// 如果写管道发生阻塞，意味着管道上有消息尚未被消费。
	select {
	case n.more <- struct{}{}:
	default:
	}
}

// Alertmanagers returns a slice of Alertmanager URLs.
func (n *Manager) Alertmanagers() []*url.URL {

	n.mtx.RLock()
	amSets := n.alertmanagers
	n.mtx.RUnlock()

	var res []*url.URL

	for _, ams := range amSets {

		ams.mtx.RLock()
		for _, am := range ams.ams {
			res = append(res, am.url())
		}
		ams.mtx.RUnlock()

	}

	return res
}

// DroppedAlertmanagers returns a slice of Alertmanager URLs.
func (n *Manager) DroppedAlertmanagers() []*url.URL {
	n.mtx.RLock()
	amSets := n.alertmanagers
	n.mtx.RUnlock()

	var res []*url.URL

	for _, ams := range amSets {
		ams.mtx.RLock()
		for _, dam := range ams.droppedAms {
			res = append(res, dam.url())
		}
		ams.mtx.RUnlock()
	}

	return res
}

// sendAll sends the alerts to all configured Alertmanagers concurrently.
// It returns true if the alerts could be sent successfully to at least one Alertmanager.
func (n *Manager) sendAll(alerts ...*Alert) bool {

	if len(alerts) == 0 {
		return true
	}

	begin := time.Now()

	// 获取当前最新的 alertmanagers 服务列表
	n.mtx.RLock()
	amSets := n.alertmanagers
	n.mtx.RUnlock()

	var (
		wg sync.WaitGroup
		numSuccess uint64
		// v1Payload and v2Payload represent 'alerts' marshaled for Alertmanager API v1 or v2.
		// Marshaling happens below. Reference here is for caching between for loop iterations.
		v1Payload, v2Payload []byte
	)

	// 遍历 alertmanagers 服务列表，发送告警 alerts 到每个  alertmanager
	for _, ams := range amSets {

		ams.mtx.RLock()

		// 把告警 alerts 按 json 格式编码成二进制数据 payload
		var (
			payload []byte
			err     error
		)

		// 不同版本的数据序列化方式不同
		switch ams.cfg.APIVersion {
		case config.AlertmanagerAPIVersionV1:
			{
				if v1Payload == nil {
					v1Payload, err = json.Marshal(alerts)
					if err != nil {
						level.Error(n.logger).Log("msg", "Encoding alerts for Alertmanager API v1 failed", "err", err)
						return false
					}
				}
				payload = v1Payload
			}
		case config.AlertmanagerAPIVersionV2:
			{
				if v2Payload == nil {
					openAPIAlerts := alertsToOpenAPIAlerts(alerts)

					v2Payload, err = json.Marshal(openAPIAlerts)
					if err != nil {
						level.Error(n.logger).Log("msg", "Encoding alerts for Alertmanager API v2 failed", "err", err)
						return false
					}
				}
				payload = v2Payload
			}
		default:
			{
				level.Error(n.logger).Log(
					"msg", fmt.Sprintf("Invalid Alertmanager API version '%v', expected one of '%v'", ams.cfg.APIVersion, config.SupportedAlertmanagerAPIVersions),
					"err", err,
				)
				return false
			}
		}

		// 起一个协程负责往当前 alertmanager 发送告警信息
		for _, am := range ams.ams {
			wg.Add(1)

			// 设置发送超时
			ctx, cancel := context.WithTimeout(n.ctx, time.Duration(ams.cfg.Timeout))
			defer cancel()

			go func(client *http.Client, url string) {
				// 发送 http 请求
				if err := n.sendOne(ctx, client, url, payload); err != nil {
					level.Error(n.logger).Log("alertmanager", url, "count", len(alerts), "msg", "Error sending alert", "err", err)
					n.metrics.errors.WithLabelValues(url).Inc()
				} else {
					atomic.AddUint64(&numSuccess, 1)
				}
				n.metrics.latency.WithLabelValues(url).Observe(time.Since(begin).Seconds())
				n.metrics.sent.WithLabelValues(url).Add(float64(len(alerts)))
				wg.Done()
			}(ams.client, am.url().String())
		}

		ams.mtx.RUnlock()
	}

	// 等待所有发送完成
	wg.Wait()

	return numSuccess > 0
}

func alertsToOpenAPIAlerts(alerts []*Alert) models.PostableAlerts {


	openAPIAlerts := models.PostableAlerts{}

	for _, a := range alerts {


		start := strfmt.DateTime(a.StartsAt)
		end := strfmt.DateTime(a.EndsAt)


		openAPIAlerts = append(openAPIAlerts, &models.PostableAlert{
			Annotations: labelsToOpenAPILabelSet(a.Annotations),
			EndsAt:      end,
			StartsAt:    start,
			Alert: models.Alert{
				GeneratorURL: strfmt.URI(a.GeneratorURL),
				Labels:       labelsToOpenAPILabelSet(a.Labels),
			},
		})
	}

	return openAPIAlerts
}

func labelsToOpenAPILabelSet(modelLabelSet labels.Labels) models.LabelSet {
	apiLabelSet := models.LabelSet{}
	for _, label := range modelLabelSet {
		apiLabelSet[label.Name] = label.Value
	}

	return apiLabelSet
}

// 发送 http 请求
func (n *Manager) sendOne(ctx context.Context, c *http.Client, url string, b []byte) error {
	req, err := http.NewRequest("POST", url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Content-Type", contentTypeJSON)
	resp, err := n.opts.Do(ctx, c, req)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	// Any HTTP status 2xx is OK.
	if resp.StatusCode/100 != 2 {
		return errors.Errorf("bad response status %s", resp.Status)
	}

	return nil
}

// Stop shuts down the notification handler.
func (n *Manager) Stop() {
	level.Info(n.logger).Log("msg", "Stopping notification manager...")
	n.cancel()
}


// alertmanager holds Alertmanager endpoint information.
type alertmanager interface {
	url() *url.URL
}

type alertmanagerLabels struct{ labels.Labels }

const pathLabel = "__alerts_path__"

func (a alertmanagerLabels) url() *url.URL {
	return &url.URL{
		Scheme: a.Get(model.SchemeLabel),	// "http" or "https"
		Host:   a.Get(model.AddressLabel),	// address 服务地址
		Path:   a.Get(pathLabel),			// alerts 推送地址
	}
}

// alertmanagerSet contains a set of Alertmanagers discovered via a group of service
// discovery definitions that have a common configuration on how alerts should be sent.
//
type alertmanagerSet struct {
	cfg    *config.AlertmanagerConfig	// prometheus.yml 中告警相关的配置
	client *http.Client					// http 客户端
	metrics *alertMetrics				// 告警服务注册到 prometheus 的指标，参照方法: newAlertMetrics
	mtx        sync.RWMutex				// 同步锁
	ams        []alertmanager 			// 处于 active 的 alertmanager
	droppedAms []alertmanager 			// 处于 dropped 状态的 alertmanager
	logger     log.Logger 				// 日志
}

func newAlertmanagerSet(cfg *config.AlertmanagerConfig, logger log.Logger, metrics *alertMetrics) (*alertmanagerSet, error) {

	// 构造 http client
	client, err := config_util.NewClientFromConfig(cfg.HTTPClientConfig, "alertmanager", false)
	if err != nil {
		return nil, err
	}

	s := &alertmanagerSet{
		client:  client,
		cfg:     cfg,
		logger:  logger,
		metrics: metrics,
	}
	return s, nil
}



// sync extracts a deduplicated set of Alertmanager endpoints from a list of target groups definitions.

// sync 方法实现具体的同步操作，先遍历服务发现的告警服务，结合配置文件中 alerting 中 relabel_configs 信息，
// 为每个激活状态的 alertmanager 构建实例，并根据得到的告警服务，再利用 url 进行去重处理．

// 其中 sync 方法中的 alertmanagerFromGroup 处理逻辑比较简单，然后根据配置文件中alerting中relabel_configs的SourceLabels和Action，每个告警服务中进行匹配，若匹配成功，则标记该告警服务为Active，否则标记为Dropped．





// targetgroup.Group 是一组具有公共标签集的 targets，公共标签集如（region, env...)。
//
//
func (s *alertmanagerSet) sync(tgs []*targetgroup.Group) {

	// 处于激活状态的 alertmanager
	allAms := []alertmanager{}

	// 处于已丢弃状态的告警服务
	allDroppedAms := []alertmanager{}

	// 遍历服务发现组
	for _, tg := range tgs {

		ams, droppedAms, err := alertmanagerFromGroup(tg, s.cfg)
		if err != nil {
			level.Error(s.logger).Log("msg", "Creating discovered Alertmanagers failed", "err", err)
			continue
		}

		allAms = append(allAms, ams...)
		allDroppedAms = append(allDroppedAms, droppedAms...)
	}


	s.mtx.Lock()
	defer s.mtx.Unlock()


	// Set new Alertmanagers and deduplicate them along their unique URL.

	// 清空之前保存的告警服务，并通过每个告警的URL, 实现去重
	s.ams = []alertmanager{}
	s.droppedAms = []alertmanager{}
	s.droppedAms = append(s.droppedAms, allDroppedAms...)
	seen := map[string]struct{}{}

	//进行去重处理
	for _, am := range allAms {
		us := am.url().String()
		if _, ok := seen[us]; ok {
			continue
		}

		// This will initialize the Counters for the AM to 0.
		s.metrics.sent.WithLabelValues(us)
		s.metrics.errors.WithLabelValues(us)

		seen[us] = struct{}{}
		s.ams = append(s.ams, am)
	}
}


// 构造 alertmanager 的 alerts 推送地址
func postPath(pre string, v config.AlertmanagerAPIVersion) string {
	alertPushEndpoint := fmt.Sprintf("/api/%v/alerts", string(v))
	return path.Join("/", pre, alertPushEndpoint)
}

// alertmanagerFromGroup extracts a list of alertmanagers from a target group and an associated AlertmanagerConfig.
//
//
func alertmanagerFromGroup(tg *targetgroup.Group, cfg *config.AlertmanagerConfig) ([]alertmanager, []alertmanager, error) {

	var res []alertmanager
	var droppedAlertManagers []alertmanager

	// 遍历 targets ，每个 target 由一个标签集 model.LabelSet 来代表。
	for _, tgLabelSet := range tg.Targets {

		// 标签组合:  tg.Labels + tgLabelSet + "__scheme__" + "__alerts_path__"

		// 创建数组来存储组合后的新标签集
		lbls := make([]labels.Label, 0, len(tgLabelSet)+2+len(tg.Labels))

		// 存储 tgLabelSet 的标签集
		for ln, lv := range tgLabelSet {
			lbls = append(lbls, labels.Label{Name: string(ln), Value: string(lv)})
		}

		// Set configured scheme as the initial scheme label for overwrite.
		// 存储 "__scheme__" 标签
		lbls = append(lbls, labels.Label{Name: model.SchemeLabel, Value: cfg.Scheme})
		// 存储 "__alerts_path__" 标签
		lbls = append(lbls, labels.Label{Name: pathLabel, Value: postPath(cfg.PathPrefix, cfg.APIVersion)})

		// Combine target labels with target group labels.
		// 存储 tg.Labels 标签集，如果发生冲突，则忽略。
		for ln, lv := range tg.Labels {
			if _, ok := tgLabelSet[ln]; !ok {
				lbls = append(lbls, labels.Label{Name: string(ln), Value: string(lv)})
			}
		}

		// 把 lbls 进行 relabel 得到 lset，如果 relabel 之后没有标签剩下，就视当前 target 为 dropped
		lset := relabel.Process(labels.New(lbls...), cfg.RelabelConfigs...)
		if lset == nil {
			droppedAlertManagers = append(droppedAlertManagers, alertmanagerLabels{lbls})
			continue
		}


		// lset 是 labels.Labels 类型，也即 []labels.Label 类型，所支持的操作比较有限。
		// 这里把 lset 从 labels.Labels 封装成 labels.Builder，以支持更高级的操作，如后续的 Set/Del 操作，如同操作 map 一样简单。
		lb := labels.NewBuilder(lset)

		// addPort checks whether we should add a default port to the address.
		// If the address is not valid, we don't append a port either.
		//
		// addPort 检查是否应向地址尾部添加默认端口号。如果地址无效，则不需附加端口，返回 false 。
		addPort := func(s string) bool {
			// If we can split, a port exists and we don't have to add one.
			// 如果可以 split ，则存在端口，不必添加。
			if _, _, err := net.SplitHostPort(s); err == nil {
				return false
			}
			// If adding a port makes it valid, the previous error was not due to an invalid address and we can append a port.
			// 如果添加端口（随便添加个端口号）使其有效，则上一个错误不是由于地址无效造成的，可以添加端口。
			_, _, err := net.SplitHostPort(s + ":1234")
			// 如果添加端口号，仍然报错，则原地址是无效地址，返回 false。
			return err == nil
		}

		// 获取 target 的地址
		addr := lset.Get(model.AddressLabel)

		// If it's an address with no trailing port, infer it based on the used scheme.
		// 需要在 addr 尾部需要添加 port
		if addPort(addr) {

			// Addresses reaching this point are already wrapped in [] if necessary.


			// 获取 target 的 address 的 schema
			switch lset.Get(model.SchemeLabel) {
			case "http", "":
				addr = addr + ":80"
			case "https":
				addr = addr + ":443"
			default:
				return nil, nil, errors.Errorf("invalid scheme: %q", cfg.Scheme)
			}
			// 更新 target 的 address 标签
			lb.Set(model.AddressLabel, addr)
		}

		// 检查地址的合法性
		if err := config.CheckTargetAddress(model.LabelValue(addr)); err != nil {
			return nil, nil, err
		}

		// Meta labels are deleted after relabelling.
		// Other internal labels propagate to the target which decides whether they will be part of their label set.

		// 移除 lset 中那些以 "__meta_" 为前缀的标签
		for _, l := range lset {
			if strings.HasPrefix(l.Name, model.MetaLabelPrefix) {
				lb.Del(l.Name)
			}
		}


		res = append(res, alertmanagerLabels{lset})

	}
	return res, droppedAlertManagers, nil
}

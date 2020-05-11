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

package rules

import (
	"context"
	html_template "html/template"
	"math"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/blastbao/prometheus/pkg/labels"
	"github.com/blastbao/prometheus/pkg/rulefmt"
	"github.com/blastbao/prometheus/pkg/timestamp"
	"github.com/blastbao/prometheus/pkg/value"
	"github.com/blastbao/prometheus/promql"
	"github.com/blastbao/prometheus/promql/parser"
	"github.com/blastbao/prometheus/storage"
)

// RuleHealth describes the health state of a rule.
type RuleHealth string

// The possible health states of a rule based on the last execution.
const (
	HealthUnknown RuleHealth = "unknown"
	HealthGood    RuleHealth = "ok"
	HealthBad     RuleHealth = "err"
)

// Constants for instrumentation.
const namespace = "prometheus"

// Metrics for rule evaluation.
type Metrics struct {
	evalDuration        prometheus.Summary
	iterationDuration   prometheus.Summary
	iterationsMissed    prometheus.Counter
	iterationsScheduled prometheus.Counter
	evalTotal           *prometheus.CounterVec
	evalFailures        *prometheus.CounterVec
	groupInterval       *prometheus.GaugeVec
	groupLastEvalTime   *prometheus.GaugeVec
	groupLastDuration   *prometheus.GaugeVec
	groupRules          *prometheus.GaugeVec
}

// NewGroupMetrics creates a new instance of Metrics and registers it with the provided registerer,
// if not nil.
func NewGroupMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		evalDuration: prometheus.NewSummary(
			prometheus.SummaryOpts{
				Namespace:  namespace,
				Name:       "rule_evaluation_duration_seconds",
				Help:       "The duration for a rule to execute.",
				Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			}),
		iterationDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:  namespace,
			Name:       "rule_group_duration_seconds",
			Help:       "The duration of rule group evaluations.",
			Objectives: map[float64]float64{0.01: 0.001, 0.05: 0.005, 0.5: 0.05, 0.90: 0.01, 0.99: 0.001},
		}),
		iterationsMissed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "rule_group_iterations_missed_total",
			Help:      "The total number of rule group evaluations missed due to slow rule group evaluation.",
		}),
		iterationsScheduled: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "rule_group_iterations_total",
			Help:      "The total number of scheduled rule group evaluations, whether executed or missed.",
		}),
		evalTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "rule_evaluations_total",
				Help:      "The total number of rule evaluations.",
			},
			[]string{"rule_group"},
		),
		evalFailures: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "rule_evaluation_failures_total",
				Help:      "The total number of rule evaluation failures.",
			},
			[]string{"rule_group"},
		),
		groupInterval: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "rule_group_interval_seconds",
				Help:      "The interval of a rule group.",
			},
			[]string{"rule_group"},
		),
		groupLastEvalTime: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "rule_group_last_evaluation_timestamp_seconds",
				Help:      "The timestamp of the last rule group evaluation in seconds.",
			},
			[]string{"rule_group"},
		),
		groupLastDuration: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "rule_group_last_duration_seconds",
				Help:      "The duration of the last rule group evaluation.",
			},
			[]string{"rule_group"},
		),
		groupRules: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "rule_group_rules",
				Help:      "The number of rules.",
			},
			[]string{"rule_group"},
		),
	}

	if reg != nil {
		reg.MustRegister(
			m.evalDuration,
			m.iterationDuration,
			m.iterationsMissed,
			m.iterationsScheduled,
			m.evalTotal,
			m.evalFailures,
			m.groupInterval,
			m.groupLastEvalTime,
			m.groupLastDuration,
			m.groupRules,
		)
	}

	return m
}

// QueryFunc processes PromQL queries.
type QueryFunc func(ctx context.Context, q string, t time.Time) (promql.Vector, error)

// EngineQueryFunc returns a new query function that executes instant queries against the given engine.
//
// It converts scalar into vector results.
func EngineQueryFunc(engine *promql.Engine, q storage.Queryable) QueryFunc {

	return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {

		// 构造查询器
		q, err := engine.NewInstantQuery(q, qs, t)
		if err != nil {
			return nil, err
		}

		// 执行查询
		res := q.Exec(ctx)
		if res.Err != nil {
			return nil, res.Err
		}

		// 查询结果处理
		switch v := res.Value.(type) {
		// return vector directly.
		case promql.Vector:
			return v, nil
		// convert from	scalar to vector.
		case promql.Scalar:
			return promql.Vector{
				promql.Sample{
					Point:  promql.Point(v),
					Metric: labels.Labels{},
				},
			}, nil
		default:
			return nil, errors.New("rule result is not a vector or scalar")
		}

	}
}

// A Rule encapsulates a vector expression which is evaluated at a specified
// interval and acted upon (currently either recorded or used for alerting).
type Rule interface {

	Name() string

	// Labels of the rule.
	Labels() labels.Labels

	// eval evaluates the rule, including any associated recording or alerting actions.
	// eval 评估规则，包括任何相关的记录或警报操作。
	Eval(context.Context, time.Time, QueryFunc, *url.URL) (promql.Vector, error)

	// String returns a human-readable string representation of the rule.
	String() string

	// SetLastErr sets the current error experienced by the rule.
	SetLastError(error)

	// LastErr returns the last error experienced by the rule.
	LastError() error

	// SetHealth sets the current health of the rule.
	SetHealth(RuleHealth)

	// Health returns the current health of the rule.
	Health() RuleHealth

	SetEvaluationDuration(time.Duration)

	// GetEvaluationDuration returns last evaluation duration.
	// NOTE: Used dynamically by rules.html template.
	GetEvaluationDuration() time.Duration
	SetEvaluationTimestamp(time.Time)

	// GetEvaluationTimestamp returns last evaluation timestamp.
	// NOTE: Used dynamically by rules.html template.
	GetEvaluationTimestamp() time.Time

	// HTMLSnippet returns a human-readable string representation of the rule,
	// decorated with HTML elements for use the web frontend.
	HTMLSnippet(pathPrefix string) html_template.HTML
}

// Group is a set of rules that have a logical relation.
type Group struct {

	//
	name string

	//
	file string

	//
	interval time.Duration

	//
	rules []Rule

	//
	seriesInPreviousEval []map[string]labels.Labels // One per Rule.

	//
	staleSeries []labels.Labels

	//
	opts *ManagerOptions

	mtx sync.Mutex

	evaluationDuration time.Duration

	evaluationTimestamp time.Time

	shouldRestore bool

	done chan bool

	terminated chan struct{}

	managerDone chan struct{}

	logger log.Logger

	metrics *Metrics
}

type GroupOptions struct {
	Name          string
	File          string
	Interval      time.Duration
	Rules         []Rule
	ShouldRestore bool
	Opts          *ManagerOptions
	done          chan struct{}
}

// NewGroup makes a new Group with the given name, options, and rules.
func NewGroup(o GroupOptions) *Group {

	metrics := o.Opts.Metrics

	if metrics == nil {
		metrics = NewGroupMetrics(o.Opts.Registerer)
	}

	key := groupKey(o.File, o.Name)
	metrics.evalTotal.WithLabelValues(key)
	metrics.evalFailures.WithLabelValues(key)
	metrics.groupLastEvalTime.WithLabelValues(key)
	metrics.groupLastDuration.WithLabelValues(key)
	metrics.groupRules.WithLabelValues(key).Set(float64(len(o.Rules)))
	metrics.groupInterval.WithLabelValues(key).Set(o.Interval.Seconds())

	return &Group{
		name:                 o.Name,
		file:                 o.File,
		interval:             o.Interval,
		rules:                o.Rules,
		shouldRestore:        o.ShouldRestore,
		opts:                 o.Opts,
		seriesInPreviousEval: make([]map[string]labels.Labels, len(o.Rules)),
		done:                 make(chan bool),
		managerDone:          o.done,
		terminated:           make(chan struct{}),
		logger:               log.With(o.Opts.Logger, "group", o.Name),
		metrics:              metrics,
	}
}

// Name returns the group name.
func (g *Group) Name() string { return g.name }

// File returns the group's file.
func (g *Group) File() string { return g.file }

// Rules returns the group's rules.
func (g *Group) Rules() []Rule { return g.rules }

// Interval returns the group's interval.
func (g *Group) Interval() time.Duration { return g.interval }


func (g *Group) run(ctx context.Context) {

	defer close(g.terminated)

	// Wait an initial amount to have consistently slotted intervals.


	// 确定下一次执行 g.Eval() 的时刻
	evalTimestamp := g.evalTimestamp().Add(g.interval)

	// 等待首次执行
	select {
	case <-time.After(time.Until(evalTimestamp)):
	case <-g.done:
		return
	}

	ctx = promql.NewOriginContext(ctx, map[string]interface{}{
		"ruleGroup": map[string]string{
			"file": g.File(),
			"name": g.Name(),
		},
	})


	iter := func() {
		g.metrics.iterationsScheduled.Inc()
		start := time.Now()
		// [!] 执行 Eval() 评估 group 中的每个 rule，获取新告警。
		g.Eval(ctx, evalTimestamp)
		timeSinceStart := time.Since(start)
		// 上报 iterationDuration 。
		g.metrics.iterationDuration.Observe(timeSinceStart.Seconds())
		// 计算规则所用的时间（秒）。
		g.setEvaluationDuration(timeSinceStart)
		// 最近一次计算规则的时间戳。
		g.setEvaluationTimestamp(start)
	}


	// The assumption here is that since the ticker was started after having waited for `evalTimestamp` to pass,
	// the ticks will trigger soon after each `evalTimestamp + N * g.interval` occurrence.

	// 创建定时器
	tick := time.NewTicker(g.interval)
	defer tick.Stop()


	makeStale := func(s bool) {

		// false ?
		if !s {
			return
		}

		//
		go func(now time.Time) {

			for _, rule := range g.seriesInPreviousEval {
				for _, r := range rule {
					g.staleSeries = append(g.staleSeries, r)
				}
			}

			// That can be garbage collected at this point.
			g.seriesInPreviousEval = nil

			// Wait for 2 intervals to give the opportunity to renamed rules to insert new series in the tsdb.
			// At this point if there is a renamed rule, it should already be started.

			select {
			case <-g.managerDone:
			case <-time.After(2 * g.interval):
				g.cleanupStaleSeries(now)
			}

		}(time.Now())
	}


	// 首次执行 g.Eval()，同步调用，可能耗时较久，超过 g.interval 。
	iter()



	if g.shouldRestore {


		// If we have to restore, we wait for another Eval to finish.


		// The reason behind this is, during first eval (or before it)
		// we might not have enough data scraped, and recording rules would not
		// have updated the latest values, on which some alerts might depend.

		select {
		case stale := <-g.done:
			makeStale(stale)
			return

		//
		case <-tick.C:
			missed := (time.Since(evalTimestamp) / g.interval) - 1
			if missed > 0 {
				g.metrics.iterationsMissed.Add(float64(missed))
				g.metrics.iterationsScheduled.Add(float64(missed))
			}
			evalTimestamp = evalTimestamp.Add((missed + 1) * g.interval)
			iter()
		}

		g.RestoreForState(time.Now())
		g.shouldRestore = false
	}



	//（理想情况）按 g.interval 定时间隔执行 g.Eval()
	for {

		select {
		case stale := <-g.done:
			makeStale(stale)
			return
		case <-tick.C:

			// 要保证 evalTimestamp 一定是按 g.interval 的整数倍增长，但调用 g.Eval() 是同步阻塞的，
			// 一次 g.Eval() 调用可能执行很久，因此两次 g.Eval() 调用的实际间隔是不确定的。

			// 计算自上次 g.Eval() 到 now() 间隔了多少个 g.interval ，取整数
			missed := (time.Since(evalTimestamp) / g.interval) - 1
			if missed > 0 {
				g.metrics.iterationsMissed.Add(float64(missed))
				g.metrics.iterationsScheduled.Add(float64(missed))
			}

			// 这里更新本次 evalTimestamp 时间，它也是  g.interval 的整数倍
			evalTimestamp = evalTimestamp.Add( (missed + 1) * g.interval )

			// 执行 g.Eval()
			iter()

		}
	}





}

func (g *Group) stopAndMakeStale() {
	g.done <- true
	<-g.terminated
}

func (g *Group) stop() {
	close(g.done)
	<-g.terminated
}

func (g *Group) hash() uint64 {
	l := labels.New(
		labels.Label{Name: "name", Value: g.name},
		labels.Label{Name: "file", Value: g.file},
	)
	return l.Hash()
}

// AlertingRules returns the list of the group's alerting rules.
func (g *Group) AlertingRules() []*AlertingRule {

	g.mtx.Lock()
	defer g.mtx.Unlock()

	var alerts []*AlertingRule
	for _, rule := range g.rules {
		if alertingRule, ok := rule.(*AlertingRule); ok {
			alerts = append(alerts, alertingRule)
		}
	}

	sort.Slice(alerts, func(i, j int) bool {
		return alerts[i].State() > alerts[j].State() || (alerts[i].State() == alerts[j].State() && alerts[i].Name() < alerts[j].Name())
	})

	return alerts
}

// HasAlertingRules returns true if the group contains at least one AlertingRule.
func (g *Group) HasAlertingRules() bool {
	g.mtx.Lock()
	defer g.mtx.Unlock()

	for _, rule := range g.rules {
		if _, ok := rule.(*AlertingRule); ok {
			return true
		}
	}
	return false
}

// GetEvaluationDuration returns the time in seconds it took to evaluate the rule group.
func (g *Group) GetEvaluationDuration() time.Duration {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	return g.evaluationDuration
}

// setEvaluationDuration sets the time in seconds the last evaluation took.
func (g *Group) setEvaluationDuration(dur time.Duration) {
	g.metrics.groupLastDuration.WithLabelValues(groupKey(g.file, g.name)).Set(dur.Seconds())

	g.mtx.Lock()
	defer g.mtx.Unlock()
	g.evaluationDuration = dur
}

// GetEvaluationTimestamp returns the time the last evaluation of the rule group took place.
func (g *Group) GetEvaluationTimestamp() time.Time {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	return g.evaluationTimestamp
}

// setEvaluationTimestamp updates evaluationTimestamp to the timestamp of when the rule group was last evaluated.
func (g *Group) setEvaluationTimestamp(ts time.Time) {
	g.metrics.groupLastEvalTime.WithLabelValues(groupKey(g.file, g.name)).Set(float64(ts.UnixNano()) / 1e9)

	g.mtx.Lock()
	defer g.mtx.Unlock()
	g.evaluationTimestamp = ts
}

// evalTimestamp returns the immediately preceding consistently slotted evaluation time.
//
//
func (g *Group) evalTimestamp() time.Time {
	var (
		offset = int64(g.hash() % uint64(g.interval))
		now    = time.Now().UnixNano()
		adjNow = now - offset
		base   = adjNow - (adjNow % int64(g.interval))
	)
	return time.Unix(0, base+offset).UTC()
}

func nameAndLabels(rule Rule) string {
	// 格式: rule_name{"key"="value", ..., }
	return rule.Name() + rule.Labels().String()
}

// CopyState copies the alerting rule and staleness related state from the given group.
//
//
//
//
// Rules are matched based on their name and labels.
//
// If there are duplicates, the first is matched with the first, second with the second etc.
func (g *Group) CopyState(from *Group) {

	g.evaluationDuration = from.evaluationDuration

	ruleMap := make(map[string][]int, len(from.rules))



	for fromIdx, fromRule := range from.rules {

		// 规则格式: rule_name{"key"="value", ..., }
		nameAndLabels := nameAndLabels(fromRule)

		//
		l := ruleMap[nameAndLabels]
		ruleMap[nameAndLabels] = append(l, fromIdx)
	}


	for i, rule := range g.rules {

		nameAndLabels := nameAndLabels(rule)
		indexes := ruleMap[nameAndLabels]

		if len(indexes) == 0 {
			continue
		}

		fi := indexes[0]
		g.seriesInPreviousEval[i] = from.seriesInPreviousEval[fi]
		ruleMap[nameAndLabels] = indexes[1:]

		ar, ok := rule.(*AlertingRule)
		if !ok {
			continue
		}

		far, ok := from.rules[fi].(*AlertingRule)
		if !ok {
			continue
		}

		for fp, a := range far.active {
			ar.active[fp] = a
		}
	}

	// Handle deleted and unmatched duplicate rules.
	g.staleSeries = from.staleSeries
	for fi, fromRule := range from.rules {

		nameAndLabels := nameAndLabels(fromRule)

		l := ruleMap[nameAndLabels]

		if len(l) != 0 {

			for _, series := range from.seriesInPreviousEval[fi] {

				g.staleSeries = append(g.staleSeries, series)

			}

		}
	}
}

// Eval runs a single evaluation cycle in which all rules are evaluated sequentially.
func (g *Group) Eval(ctx context.Context, ts time.Time) {

	// 按序依次评估 group 中的每个 rule，通过调用 rule.Eval() 函数产生告警结果，
	// 然后判断是否是告警规则，如果是，则调用 NotifyFunc 产生告警通知。
	for i, rule := range g.rules {

		select {
		case <-g.done:
			return
		default:
		}


		func(i int, rule Rule) {

			sp, ctx := opentracing.StartSpanFromContext(ctx, "rule")
			sp.SetTag("name", rule.Name())

			defer func(t time.Time) {
				sp.Finish()
				// 计算执行耗时。
				since := time.Since(t)
				// 上报 evalDuration 。
				g.metrics.evalDuration.Observe(since.Seconds())
				// 计算规则所用的时间（秒）。
				rule.SetEvaluationDuration(since)
				// 最近一次计算规则的时间戳。
				rule.SetEvaluationTimestamp(t)

			}(time.Now())

			// 上报 evalTotal
			g.metrics.evalTotal.WithLabelValues(groupKey(g.File(), g.Name())).Inc()

			// [!] 调用 rule.Eval() 进行告警查询
			vector, err := rule.Eval(ctx, ts, g.opts.QueryFunc, g.opts.ExternalURL)
			if err != nil {
				// Canceled queries are intentional termination of queries.
				// This normally happens on shutdown and thus we skip logging of any errors here.
				if _, ok := err.(promql.ErrQueryCanceled); !ok {
					level.Warn(g.logger).Log("msg", "Evaluating rule failed", "rule", rule, "err", err)
				}
				// 上报 evalFailures
				g.metrics.evalFailures.WithLabelValues(groupKey(g.File(), g.Name())).Inc()
				return
			}

			// [!] 检查如果是 `警报规则`，则调用 rule.sendAlerts() 将需要发送通知的告警发送到 alertManager。
			if ar, ok := rule.(*AlertingRule); ok {
				ar.sendAlerts(ctx, ts, g.opts.ResendDelay, g.interval, g.opts.NotifyFunc)
			}

			var (
				numOutOfOrder = 0
				numDuplicates = 0
			)

			app := g.opts.Appendable.Appender()
			seriesReturned := make(map[string]labels.Labels, len(g.seriesInPreviousEval[i]))
			defer func() {
				if err := app.Commit(); err != nil {
					level.Warn(g.logger).Log("msg", "Rule sample appending failed", "err", err)
					return
				}
				g.seriesInPreviousEval[i] = seriesReturned
			}()

			// 将本次返回的告警数据保存到 storage 中。
			for _, s := range vector {
				if _, err := app.Add(s.Metric, s.T, s.V); err != nil {
					switch errors.Cause(err) {
					case storage.ErrOutOfOrderSample:
						numOutOfOrder++
						level.Debug(g.logger).Log("msg", "Rule evaluation result discarded", "err", err, "sample", s)
					case storage.ErrDuplicateSampleForTimestamp:
						numDuplicates++
						level.Debug(g.logger).Log("msg", "Rule evaluation result discarded", "err", err, "sample", s)
					default:
						level.Warn(g.logger).Log("msg", "Rule evaluation result discarded", "err", err, "sample", s)
					}
				} else {
					// 保存成功，添加到 seriesReturned 中
					seriesReturned[s.Metric.String()] = s.Metric
				}
			}


			if numOutOfOrder > 0 {
				level.Warn(g.logger).Log("msg", "Error on ingesting out-of-order result from rule evaluation", "numDropped", numOutOfOrder)
			}

			if numDuplicates > 0 {
				level.Warn(g.logger).Log("msg", "Error on ingesting results from rule evaluation with different value but same timestamp", "numDropped", numDuplicates)
			}

			// 比较近两次执行规则 i 返回的告警数据，如果上次某个数据在此次消失了，就从 storage 中删除它。
			for metric, lset := range g.seriesInPreviousEval[i] {

				if _, ok := seriesReturned[metric]; !ok {

					// Series no longer exposed, mark it stale.
					// 标记为 stale，等于删除
					_, err = app.Add(lset, timestamp.FromTime(ts), math.Float64frombits(value.StaleNaN))

					// 如果出错，忽略
					switch errors.Cause(err) {
					case nil:
					case storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp:
						// Do not count these in logging, as this is expected if series is exposed from a different rule.
					default:
						level.Warn(g.logger).Log("msg", "Adding stale sample failed", "sample", metric, "err", err)
					}
				}
			}
		}(i, rule)
	}

	g.cleanupStaleSeries(ts)
}

func (g *Group) cleanupStaleSeries(ts time.Time) {

	if len(g.staleSeries) == 0 {
		return
	}

	app := g.opts.Appendable.Appender()

	for _, s := range g.staleSeries {

		// Rule that produced series no longer configured, mark it stale.
		_, err := app.Add(s, timestamp.FromTime(ts), math.Float64frombits(value.StaleNaN))
		switch errors.Cause(err) {
		case nil:
		case storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp:
			// Do not count these in logging, as this is expected if series is exposed from a different rule.
		default:
			level.Warn(g.logger).Log("msg", "Adding stale sample for previous configuration failed", "sample", s, "err", err)
		}

	}

	if err := app.Commit(); err != nil {
		level.Warn(g.logger).Log("msg", "Stale sample appending for previous configuration failed", "err", err)
	} else {
		g.staleSeries = nil
	}
}


// RestoreForState restores the 'for' state of the alerts by looking up last ActiveAt from storage.
//




// --rules.alert.for-outage-tolerance=1h
//   Max time to tolerate prometheus outage for restoring "for" state of alert.
//
// 该标志指定 Prometheus 对宕机的容忍时间。
// 如果 Prometheus 的宕机时间比该标识指定的时间长，则不会恢复警报状态。
// 因此，请确保根据您的需要更改 for-outage-tolerance 的值，或者尽快重启 Prometheus 。


// --rules.alert.for-grace-period=10m
//   Minimum duration between alert and restored "for" state.
//   This is maintained only for alerts with configured "for" time greater than grace period.



//
// 1. 在每次 eval 评估 `警报规则` 的过程中，我们会使用指标名为 ALERTS_FOR_STATE 的时序数据记录 alert 的状态（首次触发的 ActiveAt ），
// 并带有该 alert 的所有标签。这与任何其他时间序列一样，但仅存储在本地。
//
// 2. 当 Prometheus 重启时，在第二次执行 eval() 之后，会启动任务来恢复活跃警报的状态。
//
// 3. 对于每个正处于活动状态的 alert ，将查找其对应的 ALERTS_FOR_STATE 状态时序数据。
// 时间戳和最后一个样本的值告诉我们 Prometheus 是什么时候宕机的、警报最后一次触发是什么时候。
//
// 4. 如果持续时间（for）为 D ，警报在 X 时刻变为活动状态，而 Prometheus 在 Y 时刻宕机（Y>X），此时，警报还需要等待 D - (Y - X) 的时间间隔。
//
// refer:
// 1. https://ganeshvernekar.com/gsoc-2018/persist-for-state/



//	--rules.alert.for-outage-tolerance=1h
//		Max time to tolerate prometheus outage for restoring "for" state of alert.
//	--rules.alert.for-grace-period=10m
//		Minimum duration between alert and restored "for" state. This is maintained onlyfor alerts with configured "for" time greater than grace period.
//
// 这两个参数与 proemtheus 停止然后恢复运行有关，prometheus 内部记录了所有 active 状态的信息，
// 包括 active 状态和时间以及规则信息，当 prometheus 重启后：
//
// 1. 检测所有规则的状态，发现有active状态的规则
// 2. 找到内部记录的以前的active的信息（默认最多往前找rules.alert.for-outage-tolerance=1h）
// 3. 计算报警规则for配置的时间段（D）-（prometheus停止的时间（Y）-active开始的时间（X））
// 4. 当active状态持续够D-(Y-X)：分两种情况
//		4.1 如果D<10m，马上firing
//		4.2 如果D>10m，那么D-(Y-X)>10m后才firing
//




func (g *Group) RestoreForState(ts time.Time) {

	// We allow restoration only if alerts were active before after certain time.
	maxtMS := int64(model.TimeFromUnixNano(ts.UnixNano()))
	mint := ts.Add(-g.opts.OutageTolerance)
	mintMS := int64(model.TimeFromUnixNano(mint.UnixNano()))

	// returns a new Querier on the storage.
	q, err := g.opts.TSDB.Querier(g.opts.Context, mintMS, maxtMS)
	if err != nil {
		level.Error(g.logger).Log("msg", "Failed to get Querier", "err", err)
		return
	}

	defer func() {
		if err := q.Close(); err != nil {
			level.Error(g.logger).Log("msg", "Failed to close Querier", "err", err)
		}
	}()


	for _, rule := range g.Rules() {

		// 检查是否为 `警告规则`，若不是，则 continue
		alertRule, ok := rule.(*AlertingRule)
		if !ok {
			continue
		}

		// 取出 alert 从 “Pending” 转为 “Firing” 状态前需要持续的时间，也即 `for` 时间间隔 D 。
		alertHoldDuration := alertRule.HoldDuration()

		// 如果 `for` 时间间隔 D < ForGracePeriod，则立即 firing 。
		if alertHoldDuration < g.opts.ForGracePeriod {

			// If alertHoldDuration is already less than grace period,
			// we would not like to make it wait for `g.opts.ForGracePeriod` time before firing.

			// [!] Hence we skip restoration, which will make it wait for alertHoldDuration.
			alertRule.SetRestored(true)
			continue
		}



		// 遍历 alertRule.active 中每个活跃的 alert
		alertRule.ForEachActiveAlert(func(a *Alert) {

			// 把 a 从 alert 构造成 ALERTS_FOR_STATE 指标，去 storage 查询该指标关联的时序数据。
			smpl := alertRule.forStateSample(a, time.Now(), 0)
			var matchers []*labels.Matcher
			for _, l := range smpl.Metric {
				mt, err := labels.NewMatcher(labels.MatchEqual, l.Name, l.Value)
				if err != nil {
					panic(err)
				}
				matchers = append(matchers, mt)
			}

			// 查询 alert 关联的 ALERTS_FOR_STATE 时序数据
			sset, err, _ := q.Select(false, nil, matchers...)
			if err != nil {
				level.Error(g.logger).Log("msg", "Failed to restore 'for' state",
					labels.AlertName, alertRule.Name(), "stage", "Select", "err", err)
				return
			}

			// 遍历查询结果，若存在和 a 匹配的告警状态数据，则查询成功，否则，直接返回。
			seriesFound := false
			var s storage.Series
			for sset.Next() {
				// Query assures that smpl.Metric is included in sset.At().Labels(),
				// hence just checking the length would act like equality.
				// (This is faster than calling labels.Compare again as we already have some info).
				if len(sset.At().Labels()) == len(smpl.Metric) {
					s = sset.At()
					seriesFound = true
					break
				}
			}

			if !seriesFound {
				return
			}


			// Series found for the 'for' state.
			var t int64
			var v float64
			it := s.Iterator()
			// 不断迭代，取出最新结果，保存到 t/v 里
			for it.Next() {
				t, v = it.At()
			}
			if it.Err() != nil {
				level.Error(g.logger).Log("msg", "Failed to restore 'for' state", labels.AlertName, alertRule.Name(), "stage", "Iterator", "err", it.Err())
				return
			}

			// ？
			if value.IsStaleNaN(v) { // Alert was not active.
				return
			}

			// 把 t 从 ms 转为 s
			downAt := time.Unix(t/1000, 0).UTC()
			// 把 v 从 ms 转为 s
			restoredActiveAt := time.Unix(int64(v), 0).UTC()
			// 计算该 alert 已经 pending 的时间，即 Y - X 。
			timeSpentPending := downAt.Sub(restoredActiveAt)
			// 计算还需 pending 的时间，即 D - (Y - X)
			timeRemainingPending := alertHoldDuration - timeSpentPending

			// 如果 D - (Y - X) <= 0 ，则 Y - X >= D，也即在 alert 刚好 firing 的时刻 prometheus 宕机了， 什么都不做。
			if timeRemainingPending <= 0 {

				// It means that alert was firing when prometheus went down.
				//
				// In the next Eval, the state of this alert will be set back to
				// firing again if it's still firing in that Eval.
				//
				// Nothing to be done in this case.

			// 如果 D - (Y - X) < ForGracePeriod ，
			} else if timeRemainingPending < g.opts.ForGracePeriod {

				// (new) restoredActiveAt = (ts + m.opts.ForGracePeriod) - alertHoldDuration
				//                            /* new firing time */      /* moving back by hold duration */
				//
				// Proof of correctness:
				// firingTime = restoredActiveAt.Add(alertHoldDuration)
				//            = ts + m.opts.ForGracePeriod - alertHoldDuration + alertHoldDuration
				//            = ts + m.opts.ForGracePeriod
				//
				// Time remaining to fire = firingTime.Sub(ts)
				//                        = (ts + m.opts.ForGracePeriod) - ts
				//                        = m.opts.ForGracePeriod

				restoredActiveAt = ts.Add(g.opts.ForGracePeriod).Add(-alertHoldDuration)


			// 如果 D - (Y - X) <= ForGracePeriod ，
			} else {

				// By shifting ActiveAt to the future (ActiveAt + some_duration),
				// the total pending time from the original ActiveAt
				// would be `alertHoldDuration + some_duration`.
				// Here, some_duration = downDuration.

				downDuration := ts.Sub(downAt)
				restoredActiveAt = restoredActiveAt.Add(downDuration)
			}

			a.ActiveAt = restoredActiveAt
			level.Debug(g.logger).Log("msg", "'for' state restored",
				labels.AlertName, alertRule.Name(), "restored_time", a.ActiveAt.Format(time.RFC850), "labels", a.Labels.String())

		})

		//
		alertRule.SetRestored(true)
	}

}





// Equals return if two groups are the same.
func (g *Group) Equals(ng *Group) bool {

	if g.name != ng.name {
		return false
	}

	if g.file != ng.file {
		return false
	}

	if g.interval != ng.interval {
		return false
	}

	if len(g.rules) != len(ng.rules) {
		return false
	}

	for i, gr := range g.rules {
		if gr.String() != ng.rules[i].String() {
			return false
		}
	}

	return true
}

// The Manager manages recording and alerting rules.
type Manager struct {
	opts     *ManagerOptions
	groups   map[string]*Group
	mtx      sync.RWMutex
	block    chan struct{}
	done     chan struct{}

	restored bool

	logger log.Logger
}

// NotifyFunc sends notifications about a set of alerts generated by the given expression.
type NotifyFunc func(ctx context.Context, expr string, alerts ...*Alert)

// ManagerOptions bundles options for the Manager.
type ManagerOptions struct {
	ExternalURL     *url.URL
	QueryFunc       QueryFunc
	NotifyFunc      NotifyFunc
	Context         context.Context
	Appendable      storage.Appendable
	TSDB            storage.Storage
	Logger          log.Logger
	Registerer      prometheus.Registerer
	OutageTolerance time.Duration
	ForGracePeriod  time.Duration
	ResendDelay     time.Duration

	Metrics *Metrics
}

// NewManager returns an implementation of Manager, ready to be started by calling the Run method.
func NewManager(o *ManagerOptions) *Manager {

	if o.Metrics == nil {
		o.Metrics = NewGroupMetrics(o.Registerer)
	}

	m := &Manager{
		groups: map[string]*Group{},
		opts:   o,
		block:  make(chan struct{}),
		done:   make(chan struct{}),
		logger: o.Logger,
	}

	o.Metrics.iterationsMissed.Inc()

	return m
}

// Run starts processing of the rule manager.
func (m *Manager) Run() {
	close(m.block)
}

// Stop the rule manager's rule evaluation cycles.
func (m *Manager) Stop() {

	m.mtx.Lock()
	defer m.mtx.Unlock()

	level.Info(m.logger).Log("msg", "Stopping rule manager...")


	for _, eg := range m.groups {
		eg.stop()
	}


	// Shut down the groups waiting multiple evaluation intervals to write staleness markers.
	close(m.done)


	level.Info(m.logger).Log("msg", "Rule manager stopped")
}

// Update the rule manager's state as the config requires.
//
//
// If loading the new rules failed the old rule set is restored.
//
//
func (m *Manager) Update(interval time.Duration, files []string, externalLabels labels.Labels) error {

	m.mtx.Lock()
	defer m.mtx.Unlock()

	// 从配置文件 files 加载规则组，每个配置文件可能包含多个组，key = file_name + group_name 。
	groups, errs := m.LoadGroups(interval, externalLabels, files...)
	if errs != nil {
		for _, e := range errs {
			level.Error(m.logger).Log("msg", "loading groups failed", "err", e)
		}
		return errors.New("error loading rules, previous rule set restored")
	}

	//
	m.restored = true

	var wg sync.WaitGroup


	// 遍历每个组
	for _, newg := range groups {

		//If there is an old group with the same identifier,
		//check if new group equals with the old group, if yes then skip it.
		//If not equals, stop it and wait for it to finish the current iteration.
		//Then copy it into the new group.



		// 如果当前组 newg 已存在于 m.groups 中（ oldg ），且 oldg 和 newg 完全相同，则 continue 。
		gn := groupKey(newg.file, newg.name)
		oldg, ok := m.groups[gn]
		delete(m.groups, gn)
		if ok && oldg.Equals(newg) {
			groups[gn] = oldg
			continue
		}

		// 否则，

		wg.Add(1)
		go func(newg *Group) {

			// 若存在 oldg ，停掉它，并把其内部状态拷贝到 newg 中。
			if ok {
				oldg.stop()
				newg.CopyState(oldg)
			}

			//
			go func() {

				// Wait with starting evaluation until the rule manager is told to run.
				// This is necessary to avoid running queries against a bootstrapping storage.

				<-m.block
				newg.run(m.opts.Context)
			}()

			wg.Done()

		}(newg)
	}


	// Stop remaining old groups.
	wg.Add(len(m.groups))


	for n, oldg := range m.groups {


		go func(n string, g *Group) {


			g.stopAndMakeStale()

			if m := g.metrics; m != nil {
				m.evalTotal.DeleteLabelValues(n)
				m.evalFailures.DeleteLabelValues(n)
				m.groupInterval.DeleteLabelValues(n)
				m.groupLastEvalTime.DeleteLabelValues(n)
				m.groupLastDuration.DeleteLabelValues(n)
				m.groupRules.DeleteLabelValues(n)
			}

			wg.Done()


		}(n, oldg)
	}


	wg.Wait()
	m.groups = groups

	return nil
}

// LoadGroups reads groups from a list of files.
func (m *Manager) LoadGroups(
	interval time.Duration,
	externalLabels labels.Labels,
	filenames ...string,			//文件列表
) (
	map[string]*Group,
	[]error,
) {

	groups := make(map[string]*Group)
	shouldRestore := !m.restored

	// 逐个配置文件进行解析，构造规则组 Groups 。
	for _, fn := range filenames {

		// 解析规则文件
		rgs, errs := rulefmt.ParseFile(fn)
		if errs != nil {
			return nil, errs
		}

		// 遍历组
		for _, rg := range rgs.Groups {

			itv := interval
			if rg.Interval != 0 {
				itv = time.Duration(rg.Interval)
			}

			rules := make([]Rule, 0, len(rg.Rules))

			// 遍历规则
			for _, r := range rg.Rules {

				// 构造查询语句解析器
				expr, err := parser.ParseExpr(r.Expr.Value)
				if err != nil {
					return nil, []error{errors.Wrap(err, fn)}
				}

				// 1. 如果告警规则名非空，则构造 & 保存 `告警规则`
				if r.Alert.Value != "" {
					rules = append(rules, NewAlertingRule(
						r.Alert.Value,
						expr,
						time.Duration(r.For),
						labels.FromMap(r.Labels),
						labels.FromMap(r.Annotations),
						externalLabels,
						m.restored,
						log.With(m.logger, "alert", r.Alert),
					))
					continue
				}

				// 2. 否则，则构造 & 保存 `记录规则`
				rules = append(rules, NewRecordingRule(
					r.Record.Value,
					expr,
					labels.FromMap(r.Labels),
				))
			}


			// 保存组信息：file+name => Group
			groups[groupKey(fn, rg.Name)] = NewGroup(GroupOptions{
				Name:          rg.Name,			// 组名
				File:          fn,				// 配置文件
				Interval:      itv,				// 执行间隔
				Rules:         rules,			// 规则列表
				ShouldRestore: shouldRestore,
				Opts:          m.opts,
				done:          m.done,
			})
		}
	}

	return groups, nil
}

// Group names need not be unique across filenames.
func groupKey(file, name string) string {
	return file + ";" + name
}

// RuleGroups returns the list of manager's rule groups.
//
// 返回所有的规则组，按字典序排序
func (m *Manager) RuleGroups() []*Group {

	m.mtx.RLock()
	defer m.mtx.RUnlock()

	rgs := make([]*Group, 0, len(m.groups))
	for _, g := range m.groups {
		rgs = append(rgs, g)
	}

	sort.Slice(rgs, func(i, j int) bool {
		if rgs[i].file != rgs[j].file {
			return rgs[i].file < rgs[j].file
		}
		return rgs[i].name < rgs[j].name
	})

	return rgs
}

// Rules returns the list of the manager's rules.
//
// 返回所有规则
func (m *Manager) Rules() []Rule {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	var rules []Rule
	for _, g := range m.groups {
		rules = append(rules, g.rules...)
	}

	return rules
}

// AlertingRules returns the list of the manager's alerting rules.
//
// 返回所有 `告警规则`
func (m *Manager) AlertingRules() []*AlertingRule {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	alerts := []*AlertingRule{}
	for _, rule := range m.Rules() {
		if alertingRule, ok := rule.(*AlertingRule); ok {
			alerts = append(alerts, alertingRule)
		}
	}

	return alerts
}

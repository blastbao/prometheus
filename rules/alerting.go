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
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	html_template "html/template"

	yaml "gopkg.in/yaml.v2"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"

	"github.com/blastbao/prometheus/pkg/labels"
	"github.com/blastbao/prometheus/pkg/rulefmt"
	"github.com/blastbao/prometheus/pkg/timestamp"
	"github.com/blastbao/prometheus/promql"
	"github.com/blastbao/prometheus/promql/parser"
	"github.com/blastbao/prometheus/template"
	"github.com/blastbao/prometheus/util/strutil"
)

const (

	// AlertMetricName is the metric name for synthetic alert timeseries.
	alertMetricName = "ALERTS"

	// AlertForStateMetricName is the metric name for 'for' state of alert.
	alertForStateMetricName = "ALERTS_FOR_STATE"

	// AlertNameLabel is the label name indicating the name of an alert.
	alertNameLabel = "alertname"

	// AlertStateLabel is the label name indicating the state of an alert.
	alertStateLabel = "alertstate"

)

// AlertState denotes the state of an active alert.
type AlertState int



// 告警状态：
// 	Inactive：这里什么都没有发生。
// 	Pending：已触发阈值，但未满足告警持续时间（即rule中的for字段）。
// 	Firing：已触发阈值且满足告警持续时间。

const (

	// StateInactive is the state of an alert that is neither firing nor pending.
	StateInactive AlertState = iota

	// StatePending is the state of an alert that has been active for less than the configured threshold duration.
	StatePending

	// StateFiring is the state of an alert that has been active for longer than the configured threshold duration.
	StateFiring

)

func (s AlertState) String() string {
	switch s {
	case StateInactive:
		return "inactive"
	case StatePending:
		return "pending"
	case StateFiring:
		return "firing"
	}
	panic(errors.Errorf("unknown alert state: %s", s.String()))
}



// Alert is the user-level representation of a single instance of an alerting rule.
// Alert 是一条警报规则的用户层面表示。
type Alert struct {

	// 状态
	State AlertState

	// 标签，会被添加到告警中
	Labels      labels.Labels

	// 注解，告警的补充和描述
	Annotations labels.Labels

	// The value at the last evaluation of the alerting expression.
	// 告警表达式最后一次求值时的值。
	Value float64

	// The interval during which the condition of this alert held true.
	ActiveAt   time.Time
	FiredAt    time.Time
	ResolvedAt time.Time 	// ResolvedAt will be 0 to indicate a still active alert.
	LastSentAt time.Time
	ValidUntil time.Time
}

// 是否需要发送
func (a *Alert) needsSending(ts time.Time, resendDelay time.Duration) bool {

	// 告警处于 StatePending 状态，返回 false 。
	if a.State == StatePending {
		return false
	}

	// 至此，告警只能处于 StateInactive 或者 StateFiring 状态。

	// if an alert has been resolved since the last send, resend it.
	// 告警在上次发送通知后，变成 resolve 状态，可以发送通知。
	if a.ResolvedAt.After(a.LastSentAt) {
		return true
	}

	// 告警在上次发送通知后，至少要等 resendDelay 的时间，再发送新的通知。
	return a.LastSentAt.Add(resendDelay).Before(ts)
}



// An AlertingRule generates alerts from its vector expression.
//
// 警报规则：AlertingRule 根据矢量表达式 vector 来生成 Alert。
type AlertingRule struct {

	// The name of the alert.
	// 告警名
	name string

	// The vector expression from which to generate alerts.
	// 告警表达式
	vector parser.Expr

	// The duration for which a labelset needs to persist in the expression
	// output vector before an alert transitions from Pending to Firing state.
	//
	// alert 从 “Pending” 转换为 “Firing” 状态前需要持续的时间。
	holdDuration time.Duration

	// Extra labels to attach to the resulting alert sample vectors.
	//
	// 需要附加到结果集上的标签。
	labels labels.Labels

	// Non-identifying key/value pairs.
	//
	// 告警注解
	annotations labels.Labels

	// External labels from the global config.
	//
	// 全局标签
	externalLabels map[string]string


	// true if old state has been restored.
	//
	// We start persisting samples for ALERT_FOR_STATE only after the restoration.
	//
	// ???
	restored bool

	// Protects the below.
	mtx sync.Mutex

	// Time in seconds taken to evaluate rule.
	//
	// 计算规则所用的时间（秒）。
	evaluationDuration time.Duration

	// Timestamp of last evaluation of rule.
	//
	// 最近一次计算规则的时间戳。
	evaluationTimestamp time.Time

	// The health of the alerting rule.
	//
	// 警报规则的运行状况。
	health RuleHealth

	// The last error seen by the alerting rule.
	//
	// 警报规则看到的最后一个错误。
	lastError error

	// A map of alerts which are currently active (Pending or Firing), keyed by the fingerprint of the labelset they correspond to.
	//
	// [!] 当前处于活动状态（Pending or Firing）的警报。
	active map[uint64]*Alert

	logger log.Logger
}

// NewAlertingRule constructs a new AlertingRule.
//
// 构造告警规则
func NewAlertingRule(

	name string,
	vec parser.Expr,
	hold time.Duration,
	labels,
	annotations,
	externalLabels labels.Labels,
	restored bool,
	logger log.Logger,

) *AlertingRule {

	// 把 externalLabels 从 slice 转换成 map 。
	el := make(map[string]string, len(externalLabels))
	for _, lbl := range externalLabels {
		el[lbl.Name] = lbl.Value
	}

	//
	return &AlertingRule{
		name:           name,
		vector:         vec,
		holdDuration:   hold,
		labels:         labels,
		annotations:    annotations,
		externalLabels: el,
		health:         HealthUnknown,
		active:         map[uint64]*Alert{},
		logger:         logger,
		restored:       restored,
	}
}

// Name returns the name of the alerting rule.
func (r *AlertingRule) Name() string {
	return r.name
}

// SetLastError sets the current error seen by the alerting rule.
func (r *AlertingRule) SetLastError(err error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.lastError = err
}

// LastError returns the last error seen by the alerting rule.
func (r *AlertingRule) LastError() error {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.lastError
}

// SetHealth sets the current health of the alerting rule.
func (r *AlertingRule) SetHealth(health RuleHealth) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.health = health
}

// Health returns the current health of the alerting rule.
func (r *AlertingRule) Health() RuleHealth {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.health
}

// Query returns the query expression of the alerting rule.
func (r *AlertingRule) Query() parser.Expr {
	return r.vector
}

// HoldDuration returns the hold duration of the alerting rule.
func (r *AlertingRule) HoldDuration() time.Duration {
	return r.holdDuration
}

// Labels returns the labels of the alerting rule.
func (r *AlertingRule) Labels() labels.Labels {
	return r.labels
}

// Annotations returns the annotations of the alerting rule.
func (r *AlertingRule) Annotations() labels.Labels {
	return r.annotations
}


// ALERTS{alertname="<alert name>", alertstate="pending|firing", <additional alert labels>}
//
//
//
func (r *AlertingRule) sample(alert *Alert, ts time.Time) promql.Sample {

	// r.labels 是 labels.Labels 类型，也即 []labels.Label 切片类型，所支持的操作比较有限。
	// 这里把 r.labels 从 []labels.Label 封装成 labels.Builder，以支持更高级的操作，如 Set/Del 操作，如同操作 map 一样简单。

	lb := labels.NewBuilder(r.labels)

	// 把 alert.Labels 添加到 r.labels 中，若冲突则覆盖。
	for _, l := range alert.Labels {
		lb.Set(l.Name, l.Value)
	}

	// 添加额外标签
	lb.Set(labels.MetricName, alertMetricName)		// 设置指标名为  __name__ = "ALERTS"
	lb.Set(labels.AlertName, r.name)				// 告警名："alertname" = r.name
	lb.Set(alertStateLabel, alert.State.String())	// 告警状态："alertstate" = alert.State

	// 构造一个样本点，由 标签集合、时间戳、值 构成。
	s := promql.Sample{
		Metric: lb.Labels(),
		Point:  promql.Point{
					T: timestamp.FromTime(ts),	// 时间戳（毫秒）
					V: 1,						// 值
				},
	}
	return s
}

// forStateSample returns the sample for ALERTS_FOR_STATE.
//
// refer:
// 1. https://ganeshvernekar.com/gsoc-2018/persist-for-state/
// 2. https://github.com/prometheus/prometheus/pull/4061
// 3. https://github.com/prometheus/prometheus/issues/422
func (r *AlertingRule) forStateSample(alert *Alert, ts time.Time, v float64) promql.Sample {

	// 这里把 r.labels 从 []labels.Label 封装成 labels.Builder，以支持更高级的操作
	lb := labels.NewBuilder(r.labels)

	// 把 alert.Labels 添加到 r.labels 中，若冲突则覆盖。
	for _, l := range alert.Labels {
		lb.Set(l.Name, l.Value)
	}

	// 添加 "__name__" 和 "alertname" 标签
	lb.Set(labels.MetricName, alertForStateMetricName)  // 设置指标名为 __name__ = "ALERTS_FOR_STATE"
	lb.Set(labels.AlertName, r.name)					// 设置告警名为 alertname = r.name

	// 构造一个样本点，由 标签集合、时间戳、值 构成。
	s := promql.Sample{
		Metric: lb.Labels(),
		Point:  promql.Point{
					T: timestamp.FromTime(ts),
					V: v,
				},
	}
	return s
}

// SetEvaluationDuration updates evaluationDuration to the duration it took to evaluate the rule on its last evaluation.
func (r *AlertingRule) SetEvaluationDuration(dur time.Duration) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.evaluationDuration = dur
}

// GetEvaluationDuration returns the time in seconds it took to evaluate the alerting rule.
func (r *AlertingRule) GetEvaluationDuration() time.Duration {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.evaluationDuration
}


// SetEvaluationTimestamp updates evaluationTimestamp to the timestamp of when the rule was last evaluated.
func (r *AlertingRule) SetEvaluationTimestamp(ts time.Time) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.evaluationTimestamp = ts
}


// GetEvaluationTimestamp returns the time the evaluation took place.
func (r *AlertingRule) GetEvaluationTimestamp() time.Time {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.evaluationTimestamp
}

// SetRestored updates the restoration state of the alerting rule.
//
// SetRestored 更新警报规则的还原状态。
func (r *AlertingRule) SetRestored(restored bool) {
	r.restored = restored
}


// resolvedRetention is the duration for which a resolved alert instance
// is kept in memory state and consequently repeatedly sent to the AlertManager.
//
// resolvedRetention 是已解析的警报实例保持在内存状态并因此多次发送到 AlertManager 的持续时间。
const resolvedRetention = 15 * time.Minute


// Eval evaluates the rule expression and then creates pending alerts and fires or removes previously pending alerts accordingly.
// Eval 对规则表达式求值，然后创建挂起的警报并触发或删除先前挂起的警报。

func (r *AlertingRule) Eval(ctx context.Context, ts time.Time, query QueryFunc, externalURL *url.URL) (promql.Vector, error) {

	// 执行告警查询，返回一组 []promql.Sample 采样点，每个采样点即为一个告警点。
	samples, err := query(ctx, r.vector.String(), ts)

	// 如果查询出错，则设置健康状态为 "Bad"、保存错误信息，然后返回。
	if err != nil {
		r.SetHealth(HealthBad)
		r.SetLastError(err)
		return nil, err
	}

	// 这里对 r 进行加锁，所以后面对 r 的成员变量可以随意操作。
	r.mtx.Lock()
	defer r.mtx.Unlock()

	// Create pending alerts for any new vector elements in the alert expression or update the expression value for existing elements.
	// 为警报表达式中的任何新向量元素创建挂起警报，或更新现有元素的表达式值。

	// resultFPs 用来存放本次 query() 返回的告警点，key 是告警点 label(s) 的 hash 值。
	resultFPs := map[uint64]struct{}{}

	var alerts = make(map[uint64]*Alert, len(samples))

	// 遍历各个样本点，为每个样本点构造 alert 对象，并存储到 resultFPs 和 alerts 中。
	// 	1. 将 sample.Metric 从 []labels.Label 转换成 map[label_name][label_value] 格式。
	// 	2. 根据 sample.Metric、r.externalLabels, sample.V 构造告警内容模版数据 tmplData。
	// 	3. ......
	//	4. ......
	//
	for _, sample := range samples {

		// Provide the alert information to the template.
		// 向模板中填充警报信息。

		// 把 sample.Metric 从 []labels.Label 转换成 map[label_name][label_value]
		metrics := make(map[string]string, len(sample.Metric))
		for _, lbl := range sample.Metric {
			metrics[lbl.Name] = lbl.Value
		}

		// 模板数据，会填充到下面的 defs 中
		tmplData := template.AlertTemplateData(metrics, r.externalLabels, sample.V)

		// Inject some convenience variables that are easier to remember for users who are not used to Go's templating system.
		// 为那些不习惯使用 Go 模板的用户注入一些更容易记住的变量。
		defs := []string{
			"{{$labels := .Labels}}",					// metrics
			"{{$externalLabels := .ExternalLabels}}",	// r.externalLabels
			"{{$value := .Value}}",						// sample.V
		}

		// 此函数用于将标签值进行模版填充。
		expand := func(text string) string {
			// 创建模板
			tmpl := template.NewTemplateExpander(
				ctx,										// ctx
				strings.Join(append(defs, text), ""),	// 告警内容，由模版(defs)和附加内容(text)组成
				"__alert_"+r.Name(),						// 告警名
				tmplData,									// 模版数据，会填充到告警内容的 defs 部分中
				model.Time(timestamp.FromTime(ts)),			// 时间戳
				template.QueryFunc(query),					// 查询语句
				externalURL,								// 外链 url
			)
			// 执行模板填充
			result, err := tmpl.Expand()
			if err != nil {
				result = fmt.Sprintf("<error expanding template: %s>", err)
				level.Warn(r.logger).Log("msg", "Expanding alert template failed", "err", err, "data", tmplData)
			}
			return result
		}

		// 移除标签名
		lb := labels.NewBuilder(sample.Metric).Del(labels.MetricName)
		// r.labels 中存储了需要附加到结果集上的标签，这里添加它们
		for _, l := range r.labels {
			lb.Set(l.Name, expand(l.Value))
		}
		// 设置告警标签
		lb.Set(labels.AlertName, r.Name())

		// 构造注解信息
		annotations := make(labels.Labels, 0, len(r.annotations))
		for _, a := range r.annotations {
			annotations = append(annotations, labels.Label{Name: a.Name, Value: expand(a.Value)})
		}

		// 从 labels.Builder 转换回 labels.Labels 类型
		lbs := lb.Labels()

		// 计算 hash 值
		h := lbs.Hash()

		// 保存 hash 值
		resultFPs[h] = struct{}{}

		// 如果 h 值对应的 alert 已经存在，则遇到重复的 sample ，设置健康状态为 "Bad" 、保存错误信息，返回。
		if _, ok := alerts[h]; ok {
			err = fmt.Errorf("vector contains metrics with the same labelset after applying alert labels")
			// We have already acquired the lock above hence using SetHealth and SetLastError will deadlock.
			r.health = HealthBad
			r.lastError = err
			return nil, err
		}

		// 为当前样本点 sample 构造 alert 对象并保存到 alerts 中。
		alerts[h] = &Alert{
			Labels:      lbs,
			Annotations: annotations,
			ActiveAt:    ts,				// [!] 设置告警的触发时间
			State:       StatePending,
			Value:       sample.V,
		}
	}

	// [!] 遍历新触发的告警 alerts ，来更新 r.active[] 中已触发的告警的状态。
	//
	// 我们知道，r.active 中会包含 Inactive 状态的告警，这些告警是此前触发的，会在 r.active 中保存至少 resolvedRetention 时间。
	// 如果这些 Inactive 状态的告警，在本次被重复触发，需要更新 r.active 中该告警的信息，这里采用的是直接覆盖掉旧的告警。
	//
	for h, a := range alerts {

		// Check whether we already have alerting state for the identifying label set.
		// Update the last value and annotations if so, create a new alert entry otherwise.

		// 如果告警已经存在于 r.active 中，且告警状态不等于 StateInactive ，则说明此告警已存在且处于活跃态，
		// 需要对 r.active 中对应告警的 Value 和 Annotations 进行更新。
		//
		// 否则，说明此告警是新产生的、或者原先的告警状态是 StateInactive 非活跃状态，而此时该告警又变为活跃态，
		// 则需要用新触发的告警 a 覆盖 r.active[h] 中的旧告警。

		if alert, ok := r.active[h]; ok && alert.State != StateInactive {
			alert.Value = a.Value
			alert.Annotations = a.Annotations
			continue
		}

		// 直接覆盖
		r.active[h] = a
	}






	var vec promql.Vector


	// Check if any pending alerts should be removed or fire now. Write out alert timeseries.
	//
	// 遍历所有已触发的告警（r.active），根据其是否是重复触发（resultFPs），而需要清理或者变更状态。

	for fp, a := range r.active {

		// resultFPs 中存储了本次 query() 返回的告警，代表当前新触发的告警。


		// 如果告警 a 不存在于 resultFPs 中，意味着它不是本次触发的告警，检查它能否被清理：
		//
		// 1. 如果 a 的状态是 pending ，而此次未触发，意味着 pending 状态可以解除，因此将其从 r.active 中删除。
		// 2. 如果 a.ResolvedAt 不为 0 ，则其已恢复，若 now() - a.ResolvedAt 已经超过一定时间(15min)，意味着它没有持续触发，可将其从 r.active 中删除。
		// 3. 如果 a 的状态不为 StateInactive ，则其为 StatePending 或者 StateFiring 状态，由于此次为触发，则其状态可以变更为 StateInactive，同时设置 ResolvedAt 为当前时间戳。

		if _, ok := resultFPs[fp]; !ok {

			// If the alert was previously firing,
			// keep it around for a given retention time so it is reported as resolved to the AlertManager.
			//
			// 如果 alert 此前被触发过，需将其保存在 r.active 中一段时间，以便能将其 "resolved" 状态报告给 AlertManager 。

			// 1.
			if a.State == StatePending {
				delete(r.active, fp)
				continue
			}

			// 2.
			if !a.ResolvedAt.IsZero() && ts.Sub(a.ResolvedAt) > resolvedRetention {
				delete(r.active, fp)
				continue
			}

			// 3. [!] 可见 r.active 中可以包含 Inactive 状态的告警，这些告警是此前触发的，会在 r.active 中保存至少 resolvedRetention 时间。
			if a.State != StateInactive {
				a.State = StateInactive		// [!] 设置告警为非活跃状态
				a.ResolvedAt = ts			// [!] 设置告警的解决时间
			}

			// 4.
			continue
		}


		// 至此，意味着告警 a 存在于 resultFPs 中，即告警 a 被重复触发：
		//
		// 1. 如果 a 的状态是 pending ，且 now() - a.ActiveAt 已经超过一定时间，意味着它持续触发了很多次，将其状态变更为 StateFiring，同时设置 FiredAt 为当前时间戳。
		if a.State == StatePending && ts.Sub(a.ActiveAt) >= r.holdDuration {
			a.State = StateFiring 	// [!] 设置告警为 Firing 状态
			a.FiredAt = ts 			// [!] 设置告警的 Firing 时间
		}


		// 2. 如果需要保存这些新触发的告警，则构造数据点，存到 vec 中，
		if r.restored {
			//
			vec = append(vec, r.sample(a, ts))
			//
			vec = append(vec, r.forStateSample(a, ts, float64(a.ActiveAt.Unix())))
		}
	}

	// We have already acquired the lock above hence using SetHealth and SetLastError will deadlock.

	r.health = HealthGood
	r.lastError = nil

	return vec, nil
}

// State returns the maximum state of alert instances for this rule.
// StateFiring > StatePending > StateInactive
func (r *AlertingRule) State() AlertState {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	maxState := StateInactive
	for _, a := range r.active {
		if a.State > maxState {
			maxState = a.State
		}
	}
	return maxState
}

// ActiveAlerts returns a slice of active alerts.
//
// 返回 r.active 中那些状态为 fire 和 pending 状态的告警。
func (r *AlertingRule) ActiveAlerts() []*Alert {
	var res []*Alert
	for _, a := range r.currentAlerts() {
		// 过滤掉 r.active 中那些 ResolvedAt 不为 0 的告警，这些告警是 StateInactive 状态的。
		if a.ResolvedAt.IsZero() {
			res = append(res, a)
		}
	}
	return res
}

// currentAlerts returns all instances of alerts for this rule.
// This may include inactive alerts that were previously firing.
//
// 返回 r.active 中的所有告警。
// 注意，r.active 中可以包含 Inactive 状态的告警，这些告警是此前触发的，会在 r.active 中保存至少 resolvedRetention 时间。
func (r *AlertingRule) currentAlerts() []*Alert {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	alerts := make([]*Alert, 0, len(r.active))
	for _, a := range r.active {
		anew := *a
		alerts = append(alerts, &anew)
	}
	return alerts
}

// ForEachActiveAlert runs the given function on each alert.
// This should be used when you want to use the actual alerts from the AlertingRule and not on its copy.
// If you want to run on a copy of alerts then don't use this, get the alerts from 'ActiveAlerts()'.
func (r *AlertingRule) ForEachActiveAlert(f func(*Alert)) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	// 遍历 r.active 中每个 alert 进行逐个处理
	for _, a := range r.active {
		f(a)
	}
}

func (r *AlertingRule) sendAlerts(ctx context.Context, ts time.Time, resendDelay time.Duration, interval time.Duration, notifyFunc NotifyFunc) {

	alerts := []*Alert{}

	// 遍历 r.active 中每个 alert 进行逐个处理
	r.ForEachActiveAlert(func(alert *Alert) {

		// 检查告警能否发送通知
		if alert.needsSending(ts, resendDelay) {

			// 设置发送通知的时间，可用来控制发送通知的时间间隔。
			alert.LastSentAt = ts

			// Allow for a couple Eval or Alertmanager send failures
			delta := resendDelay
			if interval > resendDelay {
				delta = interval
			}
			alert.ValidUntil = ts.Add(3 * delta)
			anew := *alert

			// 把需要发送通知的告警添加到 alerts 中保存
			alerts = append(alerts, &anew)
		}
	})

	// 发送通知
	notifyFunc(ctx, r.vector.String(), alerts...)
}

func (r *AlertingRule) String() string {

	ar := rulefmt.Rule{
		Alert:       r.name,
		Expr:        r.vector.String(),
		For:         model.Duration(r.holdDuration),
		Labels:      r.labels.Map(),
		Annotations: r.annotations.Map(),
	}

	byt, err := yaml.Marshal(ar)
	if err != nil {
		return fmt.Sprintf("error marshaling alerting rule: %s", err.Error())
	}

	return string(byt)
}

// HTMLSnippet returns an HTML snippet representing this alerting rule.
// The resulting snippet is expected to be presented in a <pre> element,
// so that line breaks and other returned whitespace is respected.
func (r *AlertingRule) HTMLSnippet(pathPrefix string) html_template.HTML {

	alertMetric := model.Metric{
		model.MetricNameLabel: alertMetricName,
		alertNameLabel:        model.LabelValue(r.name),
	}

	labelsMap := make(map[string]string, len(r.labels))
	for _, l := range r.labels {
		labelsMap[l.Name] = html_template.HTMLEscapeString(l.Value)
	}

	annotationsMap := make(map[string]string, len(r.annotations))
	for _, l := range r.annotations {
		annotationsMap[l.Name] = html_template.HTMLEscapeString(l.Value)
	}

	ar := rulefmt.Rule{
		Alert:       fmt.Sprintf("<a href=%q>%s</a>", pathPrefix+strutil.TableLinkForExpression(alertMetric.String()), r.name),
		Expr:        fmt.Sprintf("<a href=%q>%s</a>", pathPrefix+strutil.TableLinkForExpression(r.vector.String()), html_template.HTMLEscapeString(r.vector.String())),
		For:         model.Duration(r.holdDuration),
		Labels:      labelsMap,
		Annotations: annotationsMap,
	}

	byt, err := yaml.Marshal(ar)
	if err != nil {
		return html_template.HTML(fmt.Sprintf("error marshaling alerting rule: %q", html_template.HTMLEscapeString(err.Error())))
	}
	return html_template.HTML(byt)
}

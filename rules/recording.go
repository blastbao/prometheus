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
	"html/template"
	"net/url"
	"sync"
	"time"

	yaml "gopkg.in/yaml.v2"

	"github.com/blastbao/prometheus/pkg/labels"
	"github.com/blastbao/prometheus/pkg/rulefmt"
	"github.com/blastbao/prometheus/promql"
	"github.com/blastbao/prometheus/promql/parser"
	"github.com/blastbao/prometheus/util/strutil"
)

// A RecordingRule records its vector expression into new timeseries.
//
// 记录规则：
//
// 记录规则允许预先计算经常需要或计算上昂贵的表达式，并将其结果保存为一组新的时间序列。
// 因此，查询预先计算的结果通常比每次需要时执行原始表达式快得多。
// 这对于仪表板尤其有用，仪表板需要在每次刷新时重复查询相同的表达式。
//
// 记录和警报规则存在于规则组（Group）中，组内的规则以固定间隔顺序运行
// 记录和警报规则的名称必须是有效的度量标准名称。
//
type RecordingRule struct {

	// 告警名
	name   string
	// 告警表达式
	vector parser.Expr
	// 需要附加到结果集上的标签。
	labels labels.Labels

	// Protects the below.
	mtx sync.Mutex

	// The health of the recording rule.
	// 警报规则的运行状况。
	health RuleHealth

	// Timestamp of last evaluation of the recording rule.
	// 最近一次计算规则的时间戳。
	evaluationTimestamp time.Time

	// The last error seen by the recording rule.
	// 警报规则看到的最后一个错误。
	lastError error

	// Duration of how long it took to evaluate the recording rule.
	// 计算规则所用的时间（秒）。
	evaluationDuration time.Duration
}

// NewRecordingRule returns a new recording rule.
func NewRecordingRule(name string, vector parser.Expr, lset labels.Labels) *RecordingRule {
	return &RecordingRule{
		name:   name,
		vector: vector,
		health: HealthUnknown,
		labels: lset,
	}
}

// Name returns the rule name.
func (rule *RecordingRule) Name() string {
	return rule.name
}

// Query returns the rule query expression.
func (rule *RecordingRule) Query() parser.Expr {
	return rule.vector
}

// Labels returns the rule labels.
func (rule *RecordingRule) Labels() labels.Labels {
	return rule.labels
}

// Eval evaluates the rule and then overrides the metric names and labels accordingly.
func (rule *RecordingRule) Eval(ctx context.Context, ts time.Time, query QueryFunc, _ *url.URL) (promql.Vector, error) {

	// 执行告警查询，返回一组 []promql.Sample 采样点，每个采样点即为一个告警点。
	vector, err := query(ctx, rule.vector.String(), ts)

	// 如果查询出错，则设置健康状态为 "Bad"、保存错误信息，然后返回。
	if err != nil {
		rule.SetHealth(HealthBad)
		rule.SetLastError(err)
		return nil, err
	}

	// Override the metric name and labels.
	// 遍历各个样本点，将其转换为一个新的 metric 数据，主要是添加 "__name__" 标签和其它 rule.labels 。
	for i := range vector {

		sample := &vector[i]

		// 将 sample.Metric 转换成 lb
		lb := labels.NewBuilder(sample.Metric)

		// 添加（覆盖）"__name__" 标签，将 sample 构造成新的 metric 数据。
		lb.Set(labels.MetricName, rule.name)

		// 将 rule.labels 中标签添加到 sample 上。
		for _, l := range rule.labels {
			lb.Set(l.Name, l.Value)
		}

		// 将 lb 更新到 sample.Metric
		sample.Metric = lb.Labels()
	}

	// Check that the rule does not produce identical metrics after applying labels.
	//
	// 检查处理后的 vector 中是否包含 sample.Metric 相同的冲突样本点。
	if vector.ContainsSameLabelset() {
		err = fmt.Errorf("vector contains metrics with the same labelset after applying rule labels")
		rule.SetHealth(HealthBad)
		rule.SetLastError(err)
		return nil, err
	}

	rule.SetHealth(HealthGood)
	rule.SetLastError(err)

	return vector, nil
}

func (rule *RecordingRule) String() string {

	r := rulefmt.Rule{
		Record: rule.name,
		Expr:   rule.vector.String(),
		Labels: rule.labels.Map(),
	}

	byt, err := yaml.Marshal(r)
	if err != nil {
		return fmt.Sprintf("error marshaling recording rule: %q", err.Error())
	}

	return string(byt)
}

// SetEvaluationDuration updates evaluationDuration to the time in seconds it took to evaluate the rule on its last evaluation.
func (rule *RecordingRule) SetEvaluationDuration(dur time.Duration) {
	rule.mtx.Lock()
	defer rule.mtx.Unlock()
	rule.evaluationDuration = dur
}

// SetLastError sets the current error seen by the recording rule.
func (rule *RecordingRule) SetLastError(err error) {
	rule.mtx.Lock()
	defer rule.mtx.Unlock()
	rule.lastError = err
}

// LastError returns the last error seen by the recording rule.
func (rule *RecordingRule) LastError() error {
	rule.mtx.Lock()
	defer rule.mtx.Unlock()
	return rule.lastError
}

// SetHealth sets the current health of the recording rule.
func (rule *RecordingRule) SetHealth(health RuleHealth) {
	rule.mtx.Lock()
	defer rule.mtx.Unlock()
	rule.health = health
}

// Health returns the current health of the recording rule.
func (rule *RecordingRule) Health() RuleHealth {
	rule.mtx.Lock()
	defer rule.mtx.Unlock()
	return rule.health
}

// GetEvaluationDuration returns the time in seconds it took to evaluate the recording rule.
func (rule *RecordingRule) GetEvaluationDuration() time.Duration {
	rule.mtx.Lock()
	defer rule.mtx.Unlock()
	return rule.evaluationDuration
}

// SetEvaluationTimestamp updates evaluationTimestamp to the timestamp of when the rule was last evaluated.
func (rule *RecordingRule) SetEvaluationTimestamp(ts time.Time) {
	rule.mtx.Lock()
	defer rule.mtx.Unlock()
	rule.evaluationTimestamp = ts
}

// GetEvaluationTimestamp returns the time the evaluation took place.
func (rule *RecordingRule) GetEvaluationTimestamp() time.Time {
	rule.mtx.Lock()
	defer rule.mtx.Unlock()
	return rule.evaluationTimestamp
}

// HTMLSnippet returns an HTML snippet representing this rule.
func (rule *RecordingRule) HTMLSnippet(pathPrefix string) template.HTML {
	ruleExpr := rule.vector.String()
	labels := make(map[string]string, len(rule.labels))
	for _, l := range rule.labels {
		labels[l.Name] = template.HTMLEscapeString(l.Value)
	}

	r := rulefmt.Rule{
		Record: fmt.Sprintf(`<a href="%s">%s</a>`, pathPrefix+strutil.TableLinkForExpression(rule.name), rule.name),
		Expr:   fmt.Sprintf(`<a href="%s">%s</a>`, pathPrefix+strutil.TableLinkForExpression(ruleExpr), template.HTMLEscapeString(ruleExpr)),
		Labels: labels,
	}

	byt, err := yaml.Marshal(r)
	if err != nil {
		return template.HTML(fmt.Sprintf("error marshaling recording rule: %q", template.HTMLEscapeString(err.Error())))
	}

	return template.HTML(byt)
}

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

package stats

import (
	"context"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
)

// QueryTiming identifies the code area or functionality in which time is spent during a query.
//
// QueryTiming 标识了在查询过程中某个 '代码段' 或 '函数' 的执行时间。
type QueryTiming int

// Query timings.
const (
	EvalTotalTime        QueryTiming = iota // eval 总时间
	ResultSortTime                          // 结果排序时间
	QueryPreparationTime                    // 查询准备时间
	InnerEvalTime                           // 内部 eval 时间
	ExecQueueTime                           // Exec queue wait time
	ExecTotalTime                           // Exec total time
)

// Return a string representation of a QueryTiming identifier.
//
// 返回 QueryTiming 标识符的字符串表示。
//
func (s QueryTiming) String() string {
	switch s {
	case EvalTotalTime:
		return "Eval total time"
	case ResultSortTime:
		return "Result sorting time"
	case QueryPreparationTime:
		return "Query preparation time"
	case InnerEvalTime:
		return "Inner eval time"
	case ExecQueueTime:
		return "Exec queue wait time"
	case ExecTotalTime:
		return "Exec total time"
	default:
		return "Unknown query timing"
	}
}

// SpanOperation returns a string representation of a QueryTiming span operation.
//
// SpanOperation 返回 QueryTiming 的 span 操作的字符串表示。
//
func (s QueryTiming) SpanOperation() string {
	switch s {
	case EvalTotalTime:
		return "promqlEval"
	case ResultSortTime:
		return "promqlSort"
	case QueryPreparationTime:
		return "promqlPrepare"
	case InnerEvalTime:
		return "promqlInnerEval"
	case ExecQueueTime:
		return "promqlExecQueue"
	case ExecTotalTime:
		return "promqlExec"
	default:
		return "Unknown query timing"
	}
}

// queryTimings with all query timers mapped to durations.
type queryTimings struct {
	EvalTotalTime        float64 `json:"evalTotalTime"`
	ResultSortTime       float64 `json:"resultSortTime"`
	QueryPreparationTime float64 `json:"queryPreparationTime"`
	InnerEvalTime        float64 `json:"innerEvalTime"`
	ExecQueueTime        float64 `json:"execQueueTime"`
	ExecTotalTime        float64 `json:"execTotalTime"`
}

// QueryStats currently only holding query timings.
//
//
type QueryStats struct {
	Timings queryTimings `json:"timings,omitempty"`
}

// NewQueryStats makes a QueryStats struct with all QueryTimings found in the given TimerGroup.
func NewQueryStats(tg *QueryTimers) *QueryStats {

	var qt queryTimings

	// 遍历所有定时器，将数据汇总到 qt 中
	for s, timer := range tg.TimerGroup.timers {
		switch s {
		case EvalTotalTime:
			qt.EvalTotalTime = timer.Duration()
		case ResultSortTime:
			qt.ResultSortTime = timer.Duration()
		case QueryPreparationTime:
			qt.QueryPreparationTime = timer.Duration()
		case InnerEvalTime:
			qt.InnerEvalTime = timer.Duration()
		case ExecQueueTime:
			qt.ExecQueueTime = timer.Duration()
		case ExecTotalTime:
			qt.ExecTotalTime = timer.Duration()
		}
	}

	// 封装 qt 并返回
	qs := QueryStats{
		Timings: qt,
	}

	return &qs
}

// SpanTimer unifies tracing and timing, to reduce repetition.
type SpanTimer struct {
	timer     *Timer
	observers []prometheus.Observer

	span opentracing.Span
}

func NewSpanTimer(ctx context.Context, operation string, timer *Timer, observers ...prometheus.Observer) (*SpanTimer, context.Context) {
	// 创建&启动 span
	span, ctx := opentracing.StartSpanFromContext(ctx, operation)
	// 启动定时器
	timer.Start()
	return &SpanTimer{
		timer:     timer,		// 定时器
		observers: observers,	// 监控上报
		span:      span,		// span
	}, ctx
}

func (s *SpanTimer) Finish() {
	// 停止计时器
	s.timer.Stop()
	// 停止 span
	s.span.Finish()
	// 上报执行时间
	for _, obs := range s.observers {
		obs.Observe(s.timer.ElapsedTime().Seconds())
	}
}

type QueryTimers struct {
	*TimerGroup // map[string]*Timer
}

func NewQueryTimers() *QueryTimers {
	return &QueryTimers{
		NewTimerGroup(),
	}
}

func (qs *QueryTimers) GetSpanTimer(ctx context.Context, qt QueryTiming, observers ...prometheus.Observer) (*SpanTimer, context.Context) {
	return NewSpanTimer(ctx, qt.SpanOperation(), qs.TimerGroup.GetTimer(qt), observers...)
}

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

package promql

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/blastbao/prometheus/pkg/labels"
	"github.com/blastbao/prometheus/pkg/timestamp"
	"github.com/blastbao/prometheus/pkg/value"
	"github.com/blastbao/prometheus/promql/parser"
	"github.com/blastbao/prometheus/storage"
	"github.com/blastbao/prometheus/util/stats"
)

const (
	namespace            = "prometheus"
	subsystem            = "engine"
	queryTag             = "query"
	env                  = "query execution"
	defaultLookbackDelta = 5 * time.Minute

	// The largest SampleValue that can be converted to an int64 without overflow.
	maxInt64 = 9223372036854774784
	// The smallest SampleValue that can be converted to an int64 without underflow.
	minInt64 = -9223372036854775808
)

var (
	// DefaultEvaluationInterval is the default evaluation interval of a subquery in milliseconds.
	// DefaultEvaluationInterval 是子查询的默认计算间隔（毫秒）。
	DefaultEvaluationInterval int64
)

// SetDefaultEvaluationInterval sets DefaultEvaluationInterval.
func SetDefaultEvaluationInterval(ev time.Duration) {
	atomic.StoreInt64(&DefaultEvaluationInterval, durationToInt64Millis(ev))
}

// GetDefaultEvaluationInterval returns the DefaultEvaluationInterval as time.Duration.
func GetDefaultEvaluationInterval() int64 {
	return atomic.LoadInt64(&DefaultEvaluationInterval)
}


type engineMetrics struct {
	currentQueries       prometheus.Gauge
	maxConcurrentQueries prometheus.Gauge
	queryLogEnabled      prometheus.Gauge
	queryLogFailures     prometheus.Counter
	queryQueueTime       prometheus.Summary
	queryPrepareTime     prometheus.Summary
	queryInnerEval       prometheus.Summary
	queryResultSort      prometheus.Summary
}

// convertibleToInt64 returns true if v does not over-/underflow an int64.
func convertibleToInt64(v float64) bool {
	return v <= maxInt64 && v >= minInt64
}

type (
	// ErrQueryTimeout is returned if a query timed out during processing.
	ErrQueryTimeout string

	// ErrQueryCanceled is returned if a query was canceled during processing.
	ErrQueryCanceled string

	// ErrTooManySamples is returned if a query would load more than the maximum allowed samples into memory.
	ErrTooManySamples string

	// ErrStorage is returned if an error was encountered in the storage layer during query handling.
	ErrStorage struct{ Err error }
)

func (e ErrQueryTimeout) Error() string {
	return fmt.Sprintf("query timed out in %s", string(e))
}
func (e ErrQueryCanceled) Error() string {
	return fmt.Sprintf("query was canceled in %s", string(e))
}
func (e ErrTooManySamples) Error() string {
	return fmt.Sprintf("query processing would load too many samples into memory in %s", string(e))
}
func (e ErrStorage) Error() string {
	return e.Err.Error()
}

// QueryLogger is an interface that can be used to log all the queries logged
// by the engine.
type QueryLogger interface {
	Log(...interface{}) error
	Close() error
}




// PromQL 本质就是实现下面的 interface ，执行一个query，返回结果，支持取消、关闭和获取解析后表达式以及执行统计信息。


// A Query is derived from an a raw query string and can be run against an engine it is associated with.
type Query interface {

	// Exec processes the query. Can only be called once.
	// 执行
	Exec(ctx context.Context) *Result

	// Close recovers memory used by the query result.
	// 关闭
	Close()

	// Statement returns the parsed statement of the query.
	// 表达式
	Statement() parser.Statement

	// Stats returns statistics about the lifetime of the query.
	// 统计信息
	Stats() *stats.QueryTimers

	// Cancel signals that a running query execution should be aborted.
	// 取消
	Cancel()
}

// query implements the Query interface.
type query struct {

	// Underlying data provider.
	// 底层数据提供者
	queryable storage.Queryable

	// The original query string.
	// 原始查询语句
	q string

	// Statement of the parsed query.
	// 原始查询语句的表达式
	stmt parser.Statement

	// Timer stats for the query execution.
	// 执行时间的统计
	stats *stats.QueryTimers

	// Result matrix for reuse.
	// 结果矩阵
	matrix Matrix

	// Cancellation function for the query.
	// 取消函数
	cancel func()

	// The engine against which the query is executed.
	//
	ng *Engine
}

// Statement implements the Query interface.
func (q *query) Statement() parser.Statement {
	return q.stmt
}

// Stats implements the Query interface.
func (q *query) Stats() *stats.QueryTimers {
	return q.stats
}

// Cancel implements the Query interface.
func (q *query) Cancel() {
	if q.cancel != nil {
		q.cancel()
	}
}

// Close implements the Query interface.
func (q *query) Close() {
	for _, s := range q.matrix {
		putPointSlice(s.Points)
	}
}

// Exec implements the Query interface.
func (q *query) Exec(ctx context.Context) *Result {

	// 从 ctx 中取出 span，设置 tag
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span.SetTag(queryTag, q.stmt.String())
	}

	// 执行查询
	res, warnings, err := q.ng.exec(ctx, q)

	// 返回结果
	return &Result{
		Err: err,
		Value: res,
		Warnings: warnings,
	}
}


type queryOrigin struct{}

// contextDone returns an error if the context was canceled or timed out.
func contextDone(ctx context.Context, env string) error {
	if err := ctx.Err(); err != nil {
		return contextErr(err, env)
	}
	return nil
}

func contextErr(err error, env string) error {
	switch err {
	case context.Canceled:
		return ErrQueryCanceled(env)
	case context.DeadlineExceeded:
		return ErrQueryTimeout(env)
	default:
		return err
	}
}

// EngineOpts contains configuration options used when creating a new Engine.
type EngineOpts struct {

	Logger             log.Logger

	Reg                prometheus.Registerer

	MaxSamples         int
	Timeout            time.Duration
	ActiveQueryTracker *ActiveQueryTracker


	// LookbackDelta determines the time since the last sample after which a time series is considered stale.
	LookbackDelta time.Duration
}

// Engine handles the lifetime of queries from beginning to end.
// Engine 处理查询的整个生命周期。
//
// It is connected to a querier.
type Engine struct {
	logger             log.Logger
	metrics            *engineMetrics
	timeout            time.Duration
	maxSamplesPerQuery int
	activeQueryTracker *ActiveQueryTracker
	queryLogger        QueryLogger
	queryLoggerLock    sync.RWMutex
	lookbackDelta      time.Duration
}

// NewEngine returns a new engine.
func NewEngine(opts EngineOpts) *Engine {

	if opts.Logger == nil {
		opts.Logger = log.NewNopLogger()
	}

	metrics := &engineMetrics{
		currentQueries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queries",
			Help:      "The current number of queries being executed or waiting.",
		}),
		queryLogEnabled: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "query_log_enabled",
			Help:      "State of the query log.",
		}),
		queryLogFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "query_log_failures_total",
			Help:      "The number of query log failures.",
		}),
		maxConcurrentQueries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queries_concurrent_max",
			Help:      "The max number of concurrent queries.",
		}),
		queryQueueTime: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "query_duration_seconds",
			Help:        "Query timings",
			ConstLabels: prometheus.Labels{"slice": "queue_time"},
			Objectives:  map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}),
		queryPrepareTime: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "query_duration_seconds",
			Help:        "Query timings",
			ConstLabels: prometheus.Labels{"slice": "prepare_time"},
			Objectives:  map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}),
		queryInnerEval: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "query_duration_seconds",
			Help:        "Query timings",
			ConstLabels: prometheus.Labels{"slice": "inner_eval"},
			Objectives:  map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}),
		queryResultSort: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "query_duration_seconds",
			Help:        "Query timings",
			ConstLabels: prometheus.Labels{"slice": "result_sort"},
			Objectives:  map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}),
	}


	if t := opts.ActiveQueryTracker; t != nil {
		metrics.maxConcurrentQueries.Set(float64(t.GetMaxConcurrent()))
	} else {
		metrics.maxConcurrentQueries.Set(-1)
	}


	if opts.LookbackDelta == 0 {
		opts.LookbackDelta = defaultLookbackDelta
		if l := opts.Logger; l != nil {
			level.Debug(l).Log("msg", "Lookback delta is zero, setting to default value", "value", defaultLookbackDelta)
		}
	}


	if opts.Reg != nil {
		opts.Reg.MustRegister(
			metrics.currentQueries,
			metrics.maxConcurrentQueries,
			metrics.queryLogEnabled,
			metrics.queryLogFailures,
			metrics.queryQueueTime,
			metrics.queryPrepareTime,
			metrics.queryInnerEval,
			metrics.queryResultSort,
		)
	}

	return &Engine{
		timeout:            opts.Timeout,
		logger:             opts.Logger,
		metrics:            metrics,
		maxSamplesPerQuery: opts.MaxSamples,
		activeQueryTracker: opts.ActiveQueryTracker,
		lookbackDelta:      opts.LookbackDelta,
	}
}


// SetQueryLogger sets the query logger.
func (ng *Engine) SetQueryLogger(l QueryLogger) {
	ng.queryLoggerLock.Lock()
	defer ng.queryLoggerLock.Unlock()

	if ng.queryLogger != nil {
		// An error closing the old file descriptor should not make reload fail; only log a warning.
		err := ng.queryLogger.Close()
		if err != nil {
			level.Warn(ng.logger).Log("msg", "Error while closing the previous query log file", "err", err)
		}
	}

	ng.queryLogger = l
	if l != nil {
		ng.metrics.queryLogEnabled.Set(1)
	} else {
		ng.metrics.queryLogEnabled.Set(0)
	}
}



// NewInstantQuery returns an evaluation query for the given expression at the given time.
func (ng *Engine) NewInstantQuery(q storage.Queryable, qs string, ts time.Time) (Query, error) {

	// 表达式解析
	expr, err := parser.ParseExpr(qs)
	if err != nil {
		return nil, err
	}

	// 构造查询对象
	qry := ng.newQuery(q, expr, ts, ts, 0)
	qry.q = qs 	// 设置原始查询语句

	return qry, nil
}

// NewRangeQuery returns an evaluation query for the given time range and with the resolution set by the interval.
func (ng *Engine) NewRangeQuery(q storage.Queryable, qs string, start, end time.Time, interval time.Duration) (Query, error) {

	// 表达式解析
	expr, err := parser.ParseExpr(qs)
	if err != nil {
		return nil, err
	}

	// 表达式类型必须是 矢量 或 标量
	if expr.Type() != parser.ValueTypeVector && expr.Type() != parser.ValueTypeScalar {
		return nil, errors.Errorf("invalid expression type %q for range query, must be Scalar or instant Vector", parser.DocumentedType(expr.Type()))
	}

	// 构造查询对象
	qry := ng.newQuery(q, expr, start, end, interval)
	qry.q = qs	// 设置原始查询语句

	return qry, nil
}

func (ng *Engine) newQuery(q storage.Queryable, expr parser.Expr, start, end time.Time, interval time.Duration) *query {

	// EvalStmt 保存了表达式和求值范围信息。
	es := &parser.EvalStmt{
		Expr:     expr,		// 表达式
		Start:    start,	// 开始时间
		End:      end,		// 结束时间
		Interval: interval,	// 时间间隔
	}

	// 构造查询对象
	qry := &query{
		stmt:      es,						// 表达式及查询区间
		ng:        ng,						// engine 的引用
		stats:     stats.NewQueryTimers(),	// 查询统计信息
		queryable: q,						// storage 查询器
	}

	return qry
}

func (ng *Engine) newTestQuery(f func(context.Context) error) Query {
	qry := &query{
		q:     "test statement",
		stmt:  parser.TestStmt(f),
		ng:    ng,
		stats: stats.NewQueryTimers(),
	}
	return qry
}






// exec executes the query.
//
// At this point per query only one EvalStmt is evaluated.
// Alert and record statements are not handled by the Engine.
//
//
//
func (ng *Engine) exec(ctx context.Context, q *query) (v parser.Value, w storage.Warnings, err error) {

	// metrics
	ng.metrics.currentQueries.Inc()
	defer ng.metrics.currentQueries.Dec()

	// ctx
	ctx, cancel := context.WithTimeout(ctx, ng.timeout)
	q.cancel = cancel

	// logger
	defer func() {

		ng.queryLoggerLock.RLock()

		if l := ng.queryLogger; l != nil {

			params := make(map[string]interface{}, 4)
			params["query"] = q.q
			if eq, ok := q.Statement().(*parser.EvalStmt); ok {
				params["start"] = formatDate(eq.Start)
				params["end"] = formatDate(eq.End)
				params["step"] = int64(eq.Interval / (time.Second / time.Nanosecond)) // The step provided by the user is in seconds.
			}

			f := []interface{}{"params", params}
			if err != nil {
				f = append(f, "error", err)
			}

			f = append(f, "stats", stats.NewQueryStats(q.Stats()))
			if origin := ctx.Value(queryOrigin{}); origin != nil {
				for k, v := range origin.(map[string]interface{}) {
					f = append(f, k, v)
				}
			}

			if err := l.Log(f...); err != nil {
				ng.metrics.queryLogFailures.Inc()
				level.Error(ng.logger).Log("msg", "can't log query", "err", err)
			}
		}
		ng.queryLoggerLock.RUnlock()
	}()

	// statistics
	execSpanTimer, ctx := q.stats.GetSpanTimer(ctx, stats.ExecTotalTime)
	defer execSpanTimer.Finish()
	queueSpanTimer, _ := q.stats.GetSpanTimer(ctx, stats.ExecQueueTime, ng.metrics.queryQueueTime)

	// Log query in active log.
	// The active log guarantees that we don't run over MaxConcurrent queries.
	if ng.activeQueryTracker != nil {
		queryIndex, err := ng.activeQueryTracker.Insert(ctx, q.q)
		if err != nil {
			queueSpanTimer.Finish()
			return nil, nil, contextErr(err, "query queue")
		}
		defer ng.activeQueryTracker.Delete(queryIndex)
	}
	queueSpanTimer.Finish()

	// Cancel when execution is done or an error was raised.
	defer q.cancel()

	const env = "query execution"

	evalSpanTimer, ctx := q.stats.GetSpanTimer(ctx, stats.EvalTotalTime)
	defer evalSpanTimer.Finish()

	// The base context might already be canceled on the first iteration (e.g. during shutdown).
	if err := contextDone(ctx, env); err != nil {
		return nil, nil, err
	}

	switch s := q.Statement().(type) {
	case *parser.EvalStmt:
		return ng.execEvalStmt(ctx, q, s)
	case parser.TestStmt:
		return nil, nil, s(ctx)
	}

	panic(errors.Errorf("promql.Engine.exec: unhandled statement of type %T", q.Statement()))
}

// time.Time => millisecond
func timeMilliseconds(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond/time.Nanosecond)
}

// time.Duration => millisecond
func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}






// execEvalStmt evaluates the expression of an evaluation statement for the given time range.
//
// execEvalStmt 执行指定时间范围的表达式查询语句。
//
func (ng *Engine) execEvalStmt(ctx context.Context, query *query, s *parser.EvalStmt) (parser.Value, storage.Warnings, error) {

	prepareSpanTimer, ctxPrepare := query.stats.GetSpanTimer(ctx, stats.QueryPreparationTime, ng.metrics.queryPrepareTime)

	// ???
	mint := ng.findMinTime(s)

	querier, err := query.queryable.Querier(ctxPrepare, timestamp.FromTime(mint), timestamp.FromTime(s.End))
	if err != nil {
		prepareSpanTimer.Finish()
		return nil, nil, err
	}
	defer querier.Close()


	//
	warnings, err := ng.populateSeries(ctxPrepare, querier, s)


	prepareSpanTimer.Finish()


	if err != nil {
		return nil, warnings, err
	}



	evalSpanTimer, ctxInnerEval := query.stats.GetSpanTimer(ctx, stats.InnerEvalTime, ng.metrics.queryInnerEval)


	// Instant evaluation.
	//
	// This is executed as a range evaluation with one step.
	//
	//
	//
	if s.Start == s.End && s.Interval == 0 {


		start := timeMilliseconds(s.Start) // time.Time => millisecond

		evaluator := &evaluator{
			startTimestamp:      start,
			endTimestamp:        start,
			interval:            1,
			ctx:                 ctxInnerEval,
			maxSamples:          ng.maxSamplesPerQuery,
			defaultEvalInterval: GetDefaultEvaluationInterval(),
			logger:              ng.logger,
			lookbackDelta:       ng.lookbackDelta,
		}


		//
		val, err := evaluator.Eval(s.Expr)
		if err != nil {
			return nil, warnings, err
		}


		evalSpanTimer.Finish()

		var mat Matrix

		switch result := val.(type) {
		case Matrix:
			mat = result
		case String:
			return result, warnings, nil
		default:
			panic(errors.Errorf("promql.Engine.exec: invalid expression type %q", val.Type()))
		}


		query.matrix = mat
		switch s.Expr.Type() {
		case parser.ValueTypeVector:

			// Convert matrix with one value per series into vector.

			vector := make(Vector, len(mat))

			for i, s := range mat {
				// Point might have a different timestamp,
				// force it to the evaluation timestamp as that is when we ran the evaluation.
				vector[i] = Sample{Metric: s.Metric, Point: Point{V: s.Points[0].V, T: start}}
			}

			return vector, warnings, nil

		case parser.ValueTypeScalar:

			return Scalar{
				V: mat[0].Points[0].V,
				T: start,
			}, warnings, nil

		case parser.ValueTypeMatrix:
			return mat, warnings, nil

		default:
			panic(errors.Errorf("promql.Engine.exec: unexpected expression type %q", s.Expr.Type()))
		}
	}



	// Range evaluation.
	evaluator := &evaluator{
		startTimestamp:      timeMilliseconds(s.Start),
		endTimestamp:        timeMilliseconds(s.End),
		interval:            durationMilliseconds(s.Interval),
		ctx:                 ctxInnerEval,
		maxSamples:          ng.maxSamplesPerQuery,
		defaultEvalInterval: GetDefaultEvaluationInterval(),
		logger:              ng.logger,
		lookbackDelta:       ng.lookbackDelta,
	}



	val, err := evaluator.Eval(s.Expr)
	if err != nil {
		return nil, warnings, err
	}

	evalSpanTimer.Finish()

	mat, ok := val.(Matrix)
	if !ok {
		panic(errors.Errorf("promql.Engine.exec: invalid expression type %q", val.Type()))
	}


	query.matrix = mat

	if err := contextDone(ctx, "expression evaluation"); err != nil {
		return nil, warnings, err
	}



	// TODO(fabxc): where to ensure metric labels are a copy from the storage internals.
	sortSpanTimer, _ := query.stats.GetSpanTimer(ctx, stats.ResultSortTime, ng.metrics.queryResultSort)
	sort.Sort(mat)
	sortSpanTimer.Finish()

	return mat, warnings, nil
}

// cumulativeSubqueryOffset returns the sum of range and offset of all subqueries in the path.
//
// 确保最终的 timeRange 能够覆盖整个搜索路径的所有上层子查询。
func (ng *Engine) cumulativeSubqueryOffset(path []parser.Node) time.Duration {

	var subqOffset time.Duration

	// 遍历搜索路径上所以的上层子查询，每个子查询有独立的 timeRange 和 timeOffset ，
	// 这里累加所有子查询的  timeRange 和 timeOffset 。
	for _, node := range path {
		switch n := node.(type) {
		case *parser.SubqueryExpr:
			subqOffset += n.Range + n.Offset
		}
	}
	return subqOffset
}








func (ng *Engine) findMinTime(s *parser.EvalStmt) time.Time {


	var maxOffset time.Duration

	// parser.Inspect() 调用 parser.Walk() 深度优先遍历 AST ，在深搜过程中，会对每个 node 调用 f 进行处理。
	parser.Inspect(

		// 根节点
		s.Expr,

		// f(node, path) error
		func(node parser.Node, path []parser.Node) error {


			//
			subqOffset := ng.cumulativeSubqueryOffset(path)




			switch n := node.(type) {

			// foo{bar="baz"}
			case *parser.VectorSelector:

				if maxOffset < ng.lookbackDelta + subqOffset {
					maxOffset = ng.lookbackDelta + subqOffset
				}

				m := n.Offset + ng.lookbackDelta + subqOffset

				if m > maxOffset {
					maxOffset = m
				}

			// foo{bar="baz"}[2s]
			case *parser.MatrixSelector:

				if maxOffset < n.Range + subqOffset {
					maxOffset = n.Range + subqOffset
				}

				m := n.VectorSelector.(*parser.VectorSelector).Offset + n.Range + subqOffset

				if m > maxOffset {
					maxOffset = m
				}
			}

			return nil
		},
	)

	return s.Start.Add(-maxOffset)
}

func (ng *Engine) populateSeries(ctx context.Context, querier storage.Querier, s *parser.EvalStmt) (storage.Warnings, error) {

	var (

		// Whenever a MatrixSelector is evaluated, evalRange is set to the corresponding range.
		// The evaluation of the VectorSelector inside then evaluates the given range and unsets the variable.
		//
		// 当执行 MatrixSelector 时，evaluationRange 设置为相应的时间范围。
		// 当执行 MatrixSelector 内含的 VectorSelector 时，这个时间范围被忽略？

		evalRange time.Duration
		warnings  storage.Warnings
		err       error
	)


	// parser.Inspect() 调用 parser.Walk() 深度优先遍历 AST ，在深搜过程中，会对每个 node 调用 f() 进行处理。
	parser.Inspect(

		// 根 node
		s.Expr,

		// f()
		func(node parser.Node, path []parser.Node) error {

			var set storage.SeriesSet
			var wrn storage.Warnings

			// 构造查询条件
			hints := &storage.SelectHints{
				Start: timestamp.FromTime(s.Start),			// 开始时间戳
				End:   timestamp.FromTime(s.End),			// 结束时间戳
				Step:  durationToInt64Millis(s.Interval),	// 步长
			}


			// We need to make sure we select the timerange selected by the subquery.
			// 我们需要确保我们选择了子查询所选择的时间范围。
			//
			// TODO(gouthamve): cumulativeSubqueryOffset gives the sum of range and the offset
			// TODO(gouthamve): cumulativeSubqueryOffset 给出 range 和 offset 的总和
			//
			// we can optimise it by separating out the range and offsets, and subtracting the offsets from end also.
			// 我们可以通过分离出 range 和 offsets 来优化它，并将 offsets 从 end 减去。



			// 一条搜索路径上可能包含若干个子查询，每个子查询指定了独立的 timeRange ，但是，
			// 上层查询是基于下层查询的返回结果，所以，需要确保下层查询的 timeRange 一定能
			// 覆盖上层查询的 timeRange 。
			subqOffset := ng.cumulativeSubqueryOffset(path)

			// time.Duration => millisecond
			offsetMilliseconds := durationMilliseconds(subqOffset)

			// 放大查询范围，以覆盖 path 上所有子查询
			hints.Start = hints.Start - offsetMilliseconds


			switch n := node.(type) {

			// foo{bar="baz"}
			case *parser.VectorSelector:

				if evalRange == 0 {

					hints.Start = hints.Start - durationMilliseconds(ng.lookbackDelta)

				} else {

					hints.Range = durationMilliseconds(evalRange)


					// For all matrix queries we want to ensure that we have (end-start) + range selected ,
					// this way we have `range` data before the start time
					hints.Start = hints.Start - durationMilliseconds(evalRange)


					evalRange = 0

				}

				// 在 path 上回溯的查找第一个 函数/聚合 的实例。
				hints.Func = extractFuncFromPath(path)
				//
				hints.By, hints.Grouping = extractGroupsFromPath(path)

				//
				if n.Offset > 0 {
					offsetMilliseconds := durationMilliseconds(n.Offset)
					hints.Start = hints.Start - offsetMilliseconds
					hints.End = hints.End - offsetMilliseconds
				}

				// 根据指定的搜索条件(hints)查询时序数据，返回数据的样式即 promQL 页面返回的格式。
				set, wrn, err = querier.Select(false, hints, n.LabelMatchers...)

				// 保存警告
				warnings = append(warnings, wrn...)

				// 错误检查
				if err != nil {
					level.Error(ng.logger).Log("msg", "error selecting series set", "err", err)
					return err
				}

				// 保存时序数据
				n.UnexpandedSeriesSet = set

			// foo{bar="baz"}[2s]
			case *parser.MatrixSelector:
				evalRange = n.Range
			}

			return nil
		},
	)


	return warnings, err
}

// extractFuncFromPath walks up the path and searches for the first instance of a function or aggregation.
//
// extractFuncFromPath 会在路径上寻找第一个 函数/聚合 的实例。
//
func extractFuncFromPath(path []parser.Node) string {

	// 递归出口，返回空
	if len(path) == 0 {
		return ""
	}

	// 检查 path 最后的 node
	switch n := path[len(path)-1].(type) {

	// 聚合表达式，返回操作名
	case *parser.AggregateExpr:
		return n.Op.String()

	// 函数调用，返回函数名
	case *parser.Call:
		return n.Func.Name

	// 二元表达式，返回空
	case *parser.BinaryExpr:

		// If we hit a binary expression we terminate since we only care about
		// functions or aggregations over a single metric.
		//
		// 如果是二元表达式，就直接 return，因为只关心在单个 metric 上的函数或聚合操作。
		return ""
	}

	// 递归查询
	return extractFuncFromPath(path[:len(path)-1])
}

// extractGroupsFromPath parses vector outer function and extracts grouping information if by or without was used.
func extractGroupsFromPath(p []parser.Node) (bool, []string) {

	if len(p) == 0 {
		return false, nil
	}

	switch n := p[len(p)-1].(type) {
	case *parser.AggregateExpr:
		return !n.Without, n.Grouping
	}

	return false, nil
}

func checkForSeriesSetExpansion(ctx context.Context, expr parser.Expr) {
	switch e := expr.(type) {
	case *parser.MatrixSelector:
		checkForSeriesSetExpansion(ctx, e.VectorSelector)
	case *parser.VectorSelector:

		if e.Series == nil {

			// 通过迭代器取出查询的时间序列
			series, err := expandSeriesSet(ctx, e.UnexpandedSeriesSet)

			if err != nil {
				panic(err)
			} else {
				e.Series = series
			}

		}
	}
}

func expandSeriesSet(ctx context.Context, it storage.SeriesSet) (res []storage.Series, err error) {
	// 遍历迭代器，取出查询结果，保存到 res 中
	for it.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		res = append(res, it.At())
	}
	return res, it.Err()
}












// An evaluator evaluates given expressions over given fixed timestamps.
// It is attached to an engine through which it connects to a querier and reports errors.
// On timeout or cancellation of its context it terminates.
//
// evaluator 在固定的时间戳（毫秒）上计算表达式。
// 它附在 engine 上，通过 engine 连接到 querier 并报告错误。
// 在超时或取消时，它就终止了。
//
type evaluator struct {

	ctx context.Context

	startTimestamp int64 	// Start time in milliseconds.
	endTimestamp   int64 	// End time in milliseconds.
	interval       int64 	// Interval in milliseconds.

	maxSamples          int
	currentSamples      int
	defaultEvalInterval int64
	logger              log.Logger
	lookbackDelta       time.Duration
}


// errorf causes a panic with the input formatted into an error.
func (ev *evaluator) errorf(format string, args ...interface{}) {
	ev.error(errors.Errorf(format, args...))
}

// error causes a panic with the given error.
func (ev *evaluator) error(err error) {
	panic(err)
}

// recover is the handler that turns panics into returns from the top level of evaluation.
func (ev *evaluator) recover(errp *error) {
	e := recover()
	if e == nil {
		return
	}
	if err, ok := e.(runtime.Error); ok {
		// Print the stack trace but do not inhibit the running application.
		buf := make([]byte, 64<<10)
		buf = buf[:runtime.Stack(buf, false)]
		level.Error(ev.logger).Log("msg", "runtime panic in parser", "err", e, "stacktrace", string(buf))
		*errp = errors.Wrap(err, "unexpected error")
	} else {
		*errp = e.(error)
	}
}

func (ev *evaluator) Eval(expr parser.Expr) (v parser.Value, err error) {
	defer ev.recover(&err)
	return ev.eval(expr), nil
}


// EvalNodeHelper stores extra information and caches for evaluating a single node across steps.
//
//
type EvalNodeHelper struct {

	// Evaluation timestamp.
	ts int64

	// Vector that can be used for output.
	out Vector

	// Caches.
	// dropMetricName and label_*.
	dmn map[uint64]labels.Labels

	// signatureFunc.
	sigf map[uint64]uint64

	// funcHistogramQuantile.
	signatureToMetricWithBuckets map[uint64]*metricWithBuckets

	// label_replace.
	regex *regexp.Regexp

	// For binary vector matching.
	rightSigs    map[uint64]Sample
	matchedSigs  map[uint64]map[uint64]struct{}
	resultMetric map[uint64]labels.Labels
}

// dropMetricName is a cached version of dropMetricName.
func (enh *EvalNodeHelper) dropMetricName(l labels.Labels) labels.Labels {

	if enh.dmn == nil {
		enh.dmn = make(map[uint64]labels.Labels, len(enh.out))
	}

	h := l.Hash()
	ret, ok := enh.dmn[h]
	if ok {
		return ret
	}
	ret = dropMetricName(l)
	enh.dmn[h] = ret
	return ret
}


// signatureFunc is a cached version of signatureFunc.
//
//
func (enh *EvalNodeHelper) signatureFunc(on bool, names ...string) func(labels.Labels) uint64 {


	if enh.sigf == nil {
		enh.sigf = make(map[uint64]uint64, len(enh.out))
	}

	f := signatureFunc(on, names...)

	return func(l labels.Labels) uint64 {
		h := l.Hash()
		ret, ok := enh.sigf[h]
		if ok {
			return ret
		}
		ret = f(l)
		enh.sigf[h] = ret
		return ret
	}

}

// rangeEval evaluates the given expressions, and then for each step calls
// the given function with the values computed for each expression at that step.
//
// The return value is the combination into time series of all the function call results.
//
// rangeEval 评估给定的表达式，然后对每一步调用给定的函数，并在该步中计算出每个表达式的值。
// 返回值是所有函数调用结果的组合成时间序列。
//
//
func (ev *evaluator) rangeEval(f func([]parser.Value, *EvalNodeHelper) Vector, exprs ...parser.Expr) Matrix {

	// stepCnt := (endTs - startTs) / interval + 1
	numSteps := int( (ev.endTimestamp - ev.startTimestamp) / ev.interval ) + 1
	// 保存每个表达式的处理结果，这个变量内容后面可能会修改
	matrixes := make([]Matrix, len(exprs))
	// 保存每个表达式的处理结果，这个变量始终不变
	origMatrixes := make([]Matrix, len(exprs))
	// 保存 ev 在执行本次 rangeEval() 之前已经处理的样本数
	originalNumSamples := ev.currentSamples

	// 遍历表达式列表，逐个执行，执行结果保存在 matrixes 中。
	for i, expr := range exprs {
		// Functions will take string arguments from the expressions, not the values.
		if expr != nil && expr.Type() != parser.ValueTypeString {

			// ev.currentSamples will be updated to the correct value within the ev.eval call.
			// ev.currentSamples 将在 ev.eval 调用中被更新为正确的值。
			// 调用 ev.eval(expr) 执行表达式 expr ，并加结果存储到 matrixes[i] 上。
			matrixes[i] = ev.eval(expr).(Matrix)

			// Keep a copy of the original point slices so that they can be returned to the pool.
			// 把 matrixes[i] 保存到 origMatrixes[i] 上，因为其后面会被修改。
			origMatrixes[i] = make(Matrix, len(matrixes[i]))
			copy(origMatrixes[i], matrixes[i])
		}

		// 至此，则当前表达式满足 "expr == nil" 或 "expr.Type() == parser.ValueTypeString"：
		//	（1）如果 expr == nil，无需处理；
		//	（2）如果 expr.Type() == parser.ValueTypeString，则 expr 为参数。
	}

	vectors := make([]Vector, len(exprs))    // Input vectors for the function.
	args := make([]parser.Value, len(exprs)) // Argument to function.

	// Create an output vector that is as big as the input matrix with the most time series.
	biggestLen := 1
	for i := range exprs {
		vectors[i] = make(Vector, 0, len(matrixes[i]))
		if len(matrixes[i]) > biggestLen {
			biggestLen = len(matrixes[i])
		}
	}

	enh := &EvalNodeHelper{
		out: make(Vector, 0, biggestLen),
	}

	tempNumSamples := ev.currentSamples

	// 保存 hash(sample.labels) => series 的映射，所以 biggestLen 是标签集合的最大数目。
	seriess := make(map[uint64]Series, biggestLen) // Output series by series hash.

	// 逐个时间区间 [ ts, ts + interval ] 进行处理：
	//
	// (1) 检查 ctx 是否有效，因为是 for 循环，需要每轮处理前检查一下是否超时，避免阻塞太久。
	// (2) 之前每个表达式的返回结果保存在 matrixes 中，对于表达式 exprs[i]，从其返回结果 matrixes[i] 中
	//	   取出时间戳等于 ts 的样本点，汇总存储到 vectors[i] 中。
	// (3) 将 vectors[i] 赋值给 args[i] ，这样 args[...] 便保存了每个 exprs 的结果
	// (4) 得到 args 之后，调用 result = f(args, enh) 获取处理结果
	// (5) 如果是 instant query ，就将 result 立即返回，函数结束
	// (6) 如果是 range query ，就将 result 汇总到外层变量 seriess 中
	// (7) 将 seriess 进行格式转换后，返回，函数结束。

	for ts := ev.startTimestamp; ts <= ev.endTimestamp; ts += ev.interval {

		// 检查是 ctx 已经结束
		if err := contextDone(ev.ctx, "expression evaluation"); err != nil {
			ev.error(err)
		}

		// Reset number of samples in memory after each timestamp.
		ev.currentSamples = tempNumSamples


		// Gather input vectors for this timestamp.
		//
		// 处理表达式 i 的返回结果
		for i := range exprs {

			// 清空 vectors[i] ，用于存储 exprs[i] 的处理后结果
			vectors[i] = vectors[i][:0]

			// 遍历表达式 i 的查询结果 matrixes[i] ，它包含一组时序数据 seriess，
			// 这里取出每个时序 series 中符合条件的 Point , 然后转换为 sample 存储到 vector[i] 中。
			for j, series := range matrixes[i] {

				// 遍历当前时序数据中的 Points 样本点
				for _, point := range series.Points {

					// 如果当前样本点 point 的时间恰好等于当前时间区间的起点，转换为 Sample 后添加到 vectors[i] 中
					if point.T == ts {

						if ev.currentSamples < ev.maxSamples {
							vectors[i] = append(vectors[i], Sample{Metric: series.Metric, Point: point})
							// Move input vectors forward so we don't have to re-scan the same past points at the next step.
							matrixes[i][j].Points = series.Points[1:]
							ev.currentSamples++
						} else {
							ev.error(ErrTooManySamples(env))
						}

					}

					break

				}
			}

			//
			args[i] = vectors[i]
		}





		// Make the function call.
		//
		// 执行函数调用 f()
		enh.ts = ts
		result := f(args, enh)


		// 检查返回结果是否包含重复的 Metric
		if result.ContainsSameLabelset() {
			ev.errorf("vector cannot contain metrics with the same labelset")
		}

		//
		enh.out = result[:0] // Reuse result vector.
		ev.currentSamples += len(result)


		// When we reset currentSamples to tempNumSamples during the next iteration of the loop it also
		// needs to include the samples from the result here, as they're still in memory.
		//

		tempNumSamples += len(result)

		// 异常检查
		if ev.currentSamples > ev.maxSamples {
			ev.error(ErrTooManySamples(env))
		}

		// If this could be an instant query, shortcut so as not to change sort order.
		//
		// 如果 startTimestamp == endTimestamp，则为 instant query ，此时只执行一轮查询便返回，
		// 不需要逐个时间区间进行查询并汇总。
		// 对 result 进行转换后直接返回。
		if ev.endTimestamp == ev.startTimestamp {

			// 用于保存返回结果
			matrix := make(Matrix, len(result))

			// 遍历函数 f() 的返回的样本结果
			for i, sample := range result {
				//（1）重置时间戳为 ts
				sample.Point.T = ts
				//（2） 由 sample 构造单样本的 Series 保存到 matrix[i] 上
				matrix[i] = Series{
					Metric: sample.Metric,
					Points: []Point{ sample.Point },
				}
			}

			// 更新样本总数
			ev.currentSamples = originalNumSamples + matrix.TotalSamples()

			// 返回 instant query 的结果
			return matrix
		}



		// Add samples in output vector to output series.
		//
		// 遍历函数 f() 返回的样本，按标签集将 sample 存储到外层变量 seriess[ hash(sample.labels) ] 中聚合起来，
		// 这样，seriess 会按标签集来汇总各个时间区间 [ ts, ts + interval ] 的查询结果。
		for _, sample := range result {

			// 计算当前样本 sample 的标签集的 hash 值
			h := sample.Metric.Hash()

			// 检查该 hash 值是否已存在关联的 Series 对象，若不存在则创建一个
			ss, ok := seriess[h]
			if !ok {
				ss = Series{
					Metric: sample.Metric,
					Points: getPointSlice(numSteps),
				}
			}

			// 重置样本的时间戳为 ts
			sample.Point.T = ts
			// 将样本 sample.Point 添加到其 hash 值关联的 Series 对象上
			ss.Points = append(ss.Points, sample.Point)
			// 保存/覆盖 Series
			seriess[h] = ss
		}

	}



	// Reuse the original point slices.
	//
	// 回收存储
	for _, m := range origMatrixes {
		for _, s := range m {
			putPointSlice(s.Points)
		}
	}


	// Assemble the output matrix.
	// By the time we get here we know we don't have too many samples.
	//
	//
	//
	matrix := make(Matrix, 0, len(seriess))
	for _, ss := range seriess {
		matrix = append(matrix, ss)
	}

	//
	ev.currentSamples = originalNumSamples + matrix.TotalSamples()

	return matrix
}


// evalSubquery evaluates given SubqueryExpr and returns an equivalent evaluated MatrixSelector in its place.
// Note that the Name and LabelMatchers are not set.
//
// evalSubquery 评估给定的 SubqueryExpr 表达式，并返回一个等价的 MatrixSelector 来代替它。
// 注意，没有设置 VectorSelector 中的 Name 和 LabelMatchers 。
func (ev *evaluator) evalSubquery(subq *parser.SubqueryExpr) *parser.MatrixSelector {

	// 评估子表达式，得到返回值 matrix
	matrix := ev.eval(subq).(Matrix)

	// 构造 VectorSelector
	vs := &parser.VectorSelector{
		Offset: subq.Offset,
		Series: make([]storage.Series, 0, len(matrix)),
	}

	// 构造 MatrixSelector
	ms := &parser.MatrixSelector{
		Range:          subq.Range,
		VectorSelector: vs,
	}

	// 把查询结果 matrix 中的 series 从 promql.Series 转换为 storage.Series 对象，填充到 VectorSelector.Series[] 中。
	for _, series := range matrix {
		vs.Series = append(vs.Series, NewStorageSeries(series))
	}

	return ms
}

// eval evaluates the given expression as the given AST expression node requires.
func (ev *evaluator) eval(expr parser.Expr) parser.Value {

	// This is the top-level evaluation method.
	// Thus, we check for timeout/cancellation here.
	if err := contextDone(ev.ctx, "expression evaluation"); err != nil {
		ev.error(err)
	}

	//
	numSteps := int((ev.endTimestamp-ev.startTimestamp)/ev.interval) + 1






	switch e := expr.(type) {

	case *parser.AggregateExpr:

		// 聚合参数是由"()"括起来的，需要去除括号。
		unwrapParenExpr(&e.Param)

		// 如果 e.Param 是字符串类型，能直接取出该参数值 s.Val
		if s, ok := e.Param.(*parser.StringLiteral); ok {
			// 执行表达式 e.Expr，然后对结果进行聚合操作
			return ev.rangeEval(
				func(v []parser.Value, enh *EvalNodeHelper) Vector {
					return ev.aggregation(e.Op, e.Grouping, e.Without, s.Val, v[0].(Vector), enh)
				},
				e.Expr,	// v[0]
			)
		}

		// 如果 e.Param 不是字符串类型，需要通过参数列表传递给 ev.rangeEval() ，其内部会进行求值。
		return ev.rangeEval(
			func(v []parser.Value, enh *EvalNodeHelper) Vector {
				var param float64
				// 如果 e.Param != nil ，需要从 args[0] 中取出参数 param
				if e.Param != nil {
					param = v[0].(Vector)[0].V
				}
				return ev.aggregation(e.Op, e.Grouping, e.Without, param, v[1].(Vector), enh)
			},
			e.Param, 	// v[0]
			e.Expr,		// v[1]
		)


	case *parser.Call:


		// 根据函数名检出函数对象
		call := FunctionCalls[e.Func.Name]


		if e.Func.Name == "timestamp" {
			// Matrix evaluation always returns the evaluation time,
			// so this function needs special handling when given a vector selector.
			vs, ok := e.Args[0].(*parser.VectorSelector)
			if ok {
				return ev.rangeEval(func(v []parser.Value, enh *EvalNodeHelper) Vector {
					return call([]parser.Value{ev.vectorSelector(vs, enh.ts)}, e.Args, enh)
				})
			}
		}

		// Check if the function has a matrix argument.
		//
		// 检查函数 function 是否包含 MatrixSelector 类型参数。

		var matrixArgIndex int
		var matrixArg bool

		// 遍历函数参数列表
		for i := range e.Args {

			// 去除"()"
			unwrapParenExpr(&e.Args[i])

			// 取出当前参数
			a := e.Args[i]

			// 检查参数类型
			//（1）检查参数是否是 MatrixSelector 类型
			if _, ok := a.(*parser.MatrixSelector); ok {
				matrixArgIndex = i
				matrixArg = true
				break
			}
			// （2）检查参数是否是 SubqueryExpr 类型，若是，则将 parser.SubqueryExpr 替换为 parser.MatrixSelector
			if subq, ok := a.(*parser.SubqueryExpr); ok { 	// parser.SubqueryExpr can be used in place of parser.MatrixSelector.
				matrixArgIndex = i
				matrixArg = true
				e.Args[i] = ev.evalSubquery(subq) 			// Replacing parser.SubqueryExpr with parser.MatrixSelector.
				break
			}
			// （3）其它类型，不予处理
		}

		// 参数中不含有 MatrixSelector 查询，则直接调用 call
		if !matrixArg {
			// Does not have a matrix argument.
			return ev.rangeEval(
				func(v []parser.Value, enh *EvalNodeHelper) Vector {
					return call(v, e.Args, enh)
				},
				e.Args...,
			)
		}



		// 否则，参数中含有 MatrixSelector 查询（只有一个？假设只有一个...），其下标为 matrixArgIndex 。




		// 函数参数
		inArgs := make([]parser.Value, len(e.Args))

		// Evaluate any non-matrix arguments.
		otherArgs := make([]Matrix, len(e.Args))
		otherInArgs := make([]Vector, len(e.Args))


		// 遍历函数参数
		for i, e := range e.Args {
			// 如果参数 i 不是 MatrixSelector 查询，就直接调用 ev.eval(e) 来评估它，得到结果后存到 otherArgs[i] 上，
			if i != matrixArgIndex {
				otherArgs[i] = ev.eval(e).(Matrix)
				otherInArgs[i] = Vector{ Sample{} }
				inArgs[i] = otherInArgs[i]
			}
		}


		// 取出 e.Args[matrixArgIndex]
		matrixSel := e.Args[matrixArgIndex].(*parser.MatrixSelector)
		// 取出 e.Args[matrixArgIndex].VectorSelector
		vectorSel := matrixSel.VectorSelector.(*parser.VectorSelector)
		checkForSeriesSetExpansion(ev.ctx, matrixSel)

		// offset
		offset := durationMilliseconds(vectorSel.Offset)
		// range
		selRange := durationMilliseconds(matrixSel.Range)
		// step
		stepRange := selRange
		if stepRange > ev.interval {
			stepRange = ev.interval
		}

		// Reuse objects across steps to save memory allocations.
		//
		// 跨 steps 重用对象，减少内存分配。

		inMatrix := make(Matrix, 1)
		inArgs[matrixArgIndex] = inMatrix
		enh := &EvalNodeHelper{
			out: make(Vector, 0, 1),
		}




		// Process all the calls for one time series at a time.
		//


		it := storage.NewBuffer(selRange)

		// Output matrix.
		matrix := make(Matrix, 0, len(vectorSel.Series))
		points := getPointSlice(16)



		for _, series := range vectorSel.Series {

			// 重置样本缓存
			points = points[:0]

			// 重置迭代器，用于遍历当前 series 的样本数据
			it.Reset(series.Iterator())

			//
			ss := Series{
				// For all range vector functions, the only change to the output
				// labels is dropping the metric name so just do it once here.
				Metric: dropMetricName(series.Labels()),
				Points: getPointSlice(numSteps),
			}



			inMatrix[0].Metric = series.Labels()




			for ts, step := ev.startTimestamp, -1; ts <= ev.endTimestamp; ts += ev.interval {

				step++

				// Set the non-matrix arguments.
				// They are scalar, so it is safe to use the step number when looking up the argument, as there will be no gaps.
				//
				// 设置非矩阵参数。
				// 它们是标量的，所以在查询参数时使用步数是安全的，因为不会有空隙。


				// otherArgs[i] 上存储了 e.Args[i] 的评估结果，该结果是 Matrix 类型，也即包含一组 series 。
				//
				// otherArgs[j][0] 为 e.Args[i] 评估后得到的第一组 series ，也可能只有一组 series 返回。
				//
				// otherArgs[j][0].Points[step] 为取出 series 时序中的第 step 个样本。


				for j := range e.Args {
					// 对于非 MatrixSelector 类型参数，直接取出对应的样本值
					if j != matrixArgIndex {
						otherInArgs[j][0].V = otherArgs[j][0].Points[step].V
					}
				}


				// 查询时间区间 [mint, maxt]
				maxt := ts - offset
				mint := maxt - selRange

				// Evaluate the matrix selector for this series for this step.
				//
				//
				points = ev.matrixIterSlice(it, mint, maxt, points)
				if len(points) == 0 {
					continue
				}


				inMatrix[0].Points = points
				enh.ts = ts


				// Make the function call.
				//
				// 函数调用
				// 	inArgs: 输入参数
				// 	e.Args: 一般用不到
				// 	enh: 输出
				outVec := call(inArgs, e.Args, enh)
				enh.out = outVec[:0]


				if len(outVec) > 0 {
					ss.Points = append(ss.Points,
						Point{
							V: outVec[0].Point.V,
							T: ts,
						},
					)
				}

				// Only buffer stepRange milliseconds from the second step on.
				it.ReduceDelta(stepRange)
			}


			if len(ss.Points) > 0 {

				if ev.currentSamples < ev.maxSamples {
					matrix = append(matrix, ss)
					ev.currentSamples += len(ss.Points)
				} else {
					ev.error(ErrTooManySamples(env))
				}
			} else {
				putPointSlice(ss.Points)
			}


		}

		putPointSlice(points)


		// The absent_over_time function returns 0 or 1 series.
		//
		// So far, the matrix contains multiple series.
		//
		// The following code will create a new series with values of 1 for the timestamps where no series has value.
		//
		//
		//
		// absent_over_time 函数返回 0 或 1 个 series 。
		//
		// 到目前为止， matrix 中包含多个 series 。
		//
		// 下面的代码为
		//
		// 将为没有系列值的时间戳创建一个新的系列，其值为1。
		//
		if e.Func.Name == "absent_over_time" {


			steps := int(1 + (ev.endTimestamp-ev.startTimestamp)/ev.interval)


			// Iterate once to look for a complete series.
			//
			// 迭代一次，寻找一个完整的 series 。
			for _, series := range matrix {
				if len(series.Points) == steps {
					return Matrix{}
				}
			}

			found := map[int64]struct{}{}

			for i, series := range matrix {

				for _, point := range series.Points {
					found[point.T] = struct{}{}
				}

				if i > 0 && len(found) == steps {
					return Matrix{}
				}

			}

			points := make([]Point, 0, steps-len(found))
			for ts := ev.startTimestamp; ts <= ev.endTimestamp; ts += ev.interval {
				if _, ok := found[ts]; !ok {
					points = append(points,
						Point{
							T: ts,
							V: 1,
						},
					)
				}
			}

			return Matrix {
				Series{
					Metric: createLabelsForAbsentFunction(e.Args[0]),
					Points: points,
				},
			}
		}

		if matrix.ContainsSameLabelset() {
			ev.errorf("vector cannot contain metrics with the same labelset")
		}

		return matrix


	case *parser.ParenExpr:
		return ev.eval(e.Expr)

	case *parser.UnaryExpr:

		// 执行表达式，获取返回值
		matrix := ev.eval(e.Expr).(Matrix)

		// 对返回的每个 point 的值取相反数
		if e.Op == parser.SUB {
			for i := range matrix {
				matrix[i].Metric = dropMetricName(matrix[i].Metric)
				for j := range matrix[i].Points {
					matrix[i].Points[j].V = -matrix[i].Points[j].V
				}
			}
			if matrix.ContainsSameLabelset() {
				ev.errorf("vector cannot contain metrics with the same labelset")
			}
		}
		return matrix

	case *parser.BinaryExpr:

		switch lt, rt := e.LHS.Type(), e.RHS.Type(); {

		case lt == parser.ValueTypeScalar && rt == parser.ValueTypeScalar:

			return ev.rangeEval(
				func(v []parser.Value, enh *EvalNodeHelper) Vector {
					val := scalarBinop(e.Op, v[0].(Vector)[0].Point.V, v[1].(Vector)[0].Point.V)
					return append(enh.out, Sample{Point: Point{V: val}})
				},
				e.LHS,
				e.RHS,
			)

		case lt == parser.ValueTypeVector && rt == parser.ValueTypeVector:

			switch e.Op {

			case parser.LAND:
				return ev.rangeEval(
					func(v []parser.Value, enh *EvalNodeHelper) Vector {
						return ev.VectorAnd(v[0].(Vector), v[1].(Vector), e.VectorMatching, enh)
					},
					e.LHS,
					e.RHS,
				)
			case parser.LOR:

				return ev.rangeEval(
					func(v []parser.Value, enh *EvalNodeHelper) Vector {
						return ev.VectorOr(v[0].(Vector), v[1].(Vector), e.VectorMatching, enh)
					},
					e.LHS,
					e.RHS,
				)

			case parser.LUNLESS:
				return ev.rangeEval(
					func(v []parser.Value, enh *EvalNodeHelper) Vector {
						return ev.VectorUnless(v[0].(Vector), v[1].(Vector), e.VectorMatching, enh)
					},
					e.LHS,
					e.RHS,
				)
			default:
				return ev.rangeEval(
					func(v []parser.Value, enh *EvalNodeHelper) Vector {
						return ev.VectorBinop(e.Op, v[0].(Vector), v[1].(Vector), e.VectorMatching, e.ReturnBool, enh)
					},
					e.LHS,
					e.RHS,
				)
			}

		case lt == parser.ValueTypeVector && rt == parser.ValueTypeScalar:

			return ev.rangeEval(
				func(v []parser.Value, enh *EvalNodeHelper) Vector {
					return ev.VectorscalarBinop(e.Op, v[0].(Vector), Scalar{V: v[1].(Vector)[0].Point.V}, false, e.ReturnBool, enh)
				},
				e.LHS,
				e.RHS,
			)

		case lt == parser.ValueTypeScalar && rt == parser.ValueTypeVector:
			return ev.rangeEval(
				func(v []parser.Value, enh *EvalNodeHelper) Vector {
					return ev.VectorscalarBinop(e.Op, v[1].(Vector), Scalar{V: v[0].(Vector)[0].Point.V}, true, e.ReturnBool, enh)
				},
				e.LHS,
				e.RHS,
			)
		}


	case *parser.NumberLiteral:
		return ev.rangeEval(
			func(v []parser.Value, enh *EvalNodeHelper) Vector {
				return append(enh.out, Sample{Point: Point{V: e.Val}})
			},
		)

	case *parser.VectorSelector:


		//
		checkForSeriesSetExpansion(ev.ctx, e)

		mat := make(Matrix, 0, len(e.Series))


		it := storage.NewBuffer(durationMilliseconds(ev.lookbackDelta))



		for i, s := range e.Series {

			it.Reset(s.Iterator())

			ss := Series{
				Metric: e.Series[i].Labels(),
				Points: getPointSlice(numSteps),
			}

			for ts := ev.startTimestamp; ts <= ev.endTimestamp; ts += ev.interval {
				_, v, ok := ev.vectorSelectorSingle(it, e, ts)
				if ok {
					if ev.currentSamples < ev.maxSamples {
						ss.Points = append(ss.Points, Point{V: v, T: ts})
						ev.currentSamples++
					} else {
						ev.error(ErrTooManySamples(env))
					}
				}
			}

			if len(ss.Points) > 0 {
				mat = append(mat, ss)
			} else {
				putPointSlice(ss.Points)
			}

		}

		return mat

	case *parser.MatrixSelector:

		if ev.startTimestamp != ev.endTimestamp {
			panic(errors.New("cannot do range evaluation of matrix selector"))
		}
		return ev.matrixSelector(e)


	case *parser.SubqueryExpr:

		offsetMillis := durationToInt64Millis(e.Offset)
		rangeMillis := durationToInt64Millis(e.Range)

		newEv := &evaluator{
			endTimestamp:        ev.endTimestamp - offsetMillis,
			interval:            ev.defaultEvalInterval,
			ctx:                 ev.ctx,
			currentSamples:      ev.currentSamples,
			maxSamples:          ev.maxSamples,
			defaultEvalInterval: ev.defaultEvalInterval,
			logger:              ev.logger,
			lookbackDelta:       ev.lookbackDelta,
		}

		if e.Step != 0 {
			newEv.interval = durationToInt64Millis(e.Step)
		}

		// Start with the first timestamp after (ev.startTimestamp - offset - range)
		// that is aligned with the step (multiple of 'newEv.interval').
		newEv.startTimestamp = newEv.interval * ((ev.startTimestamp - offsetMillis - rangeMillis) / newEv.interval)
		if newEv.startTimestamp < (ev.startTimestamp - offsetMillis - rangeMillis) {
			newEv.startTimestamp += newEv.interval
		}

		res := newEv.eval(e.Expr)
		ev.currentSamples = newEv.currentSamples
		return res

	case *parser.StringLiteral:
		return String{
			V: e.Val,
			T: ev.startTimestamp,
		}
	}

	panic(errors.Errorf("unhandled expression of type: %T", expr))
}

func durationToInt64Millis(d time.Duration) int64 {
	return int64(d / time.Millisecond)
}



// vectorSelector evaluates a *parser.VectorSelector expression.
func (ev *evaluator) vectorSelector(node *parser.VectorSelector, ts int64) Vector {


	checkForSeriesSetExpansion(ev.ctx, node)

	var (
		vec = make(Vector, 0, len(node.Series))
	)


	it := storage.NewBuffer(durationMilliseconds(ev.lookbackDelta))


	for i, s := range node.Series {

		it.Reset(s.Iterator())

		t, v, ok := ev.vectorSelectorSingle(it, node, ts)
		if ok {

			vec = append(vec,
				Sample{
					Metric: node.Series[i].Labels(),
					Point:  Point{
								V: v,
								T: t,
							},
				},
			)

			ev.currentSamples++

		}

		if ev.currentSamples >= ev.maxSamples {
			ev.error(ErrTooManySamples(env))
		}

	}
	return vec
}

// vectorSelectorSingle evaluates a instant vector for the iterator of one time series.
func (ev *evaluator) vectorSelectorSingle(it *storage.BufferedSeriesIterator, node *parser.VectorSelector, ts int64) (int64, float64, bool) {

	refTime := ts - durationMilliseconds(node.Offset)

	var t int64
	var v float64

	ok := it.Seek(refTime)
	if !ok {
		if it.Err() != nil {
			ev.error(it.Err())
		}
	}

	if ok {
		t, v = it.Values()
	}

	if !ok || t > refTime {
		t, v, ok = it.PeekBack(1)
		if !ok || t < refTime-durationMilliseconds(ev.lookbackDelta) {
			return 0, 0, false
		}
	}

	if value.IsStaleNaN(v) {
		return 0, 0, false
	}

	return t, v, true
}

var pointPool = sync.Pool{}

func getPointSlice(sz int) []Point {
	p := pointPool.Get()
	if p != nil {
		return p.([]Point)
	}
	return make([]Point, 0, sz)
}

func putPointSlice(p []Point) {
	//lint:ignore SA6002 relax staticcheck verification.
	pointPool.Put(p[:0])
}

// matrixSelector evaluates a *parser.MatrixSelector expression.
func (ev *evaluator) matrixSelector(node *parser.MatrixSelector) Matrix {


	checkForSeriesSetExpansion(ev.ctx, node)


	vs := node.VectorSelector.(*parser.VectorSelector)

	var (
		offset = durationMilliseconds(vs.Offset)
		maxt   = ev.startTimestamp - offset
		mint   = maxt - durationMilliseconds(node.Range)
		matrix = make(Matrix, 0, len(vs.Series))
	)


	it := storage.NewBuffer(durationMilliseconds(node.Range))

	// VectorSelector 返回的时序数据
	series := vs.Series

	//
	for i, s := range series {

		//
		if err := contextDone(ev.ctx, "expression evaluation"); err != nil {
			ev.error(err)
		}

		it.Reset(s.Iterator())

		ss := Series{
			Metric: series[i].Labels(),
		}

		ss.Points = ev.matrixIterSlice(it, mint, maxt, getPointSlice(16))

		if len(ss.Points) > 0 {
			matrix = append(matrix, ss)
		} else {
			putPointSlice(ss.Points)
		}

	}

	return matrix
}

// matrixIterSlice populates a matrix vector covering the requested range for a
// single time series, with points retrieved from an iterator.
//
// As an optimization, the matrix vector may already contain points of the same
// time series from the evaluation of an earlier step (with lower mint and maxt values).
//
// Any such points falling before mint are discarded;
// points that fall into the [mint, maxt] range are retained; only points with later timestamps
// are populated from the iterator.
//
//
func (ev *evaluator) matrixIterSlice(it *storage.BufferedSeriesIterator, mint, maxt int64, out []Point) []Point {

	// 如果 out 中有 Point 位于 [mint, maxt] 区间，需要移除 out 中那些 < mint 的 Points 。
	if len(out) > 0 && out[len(out)-1].T >= mint {
		// There is an overlap between previous and current ranges, retain common points.
		//
		// In most such cases:
		//   (a) the overlap is significantly larger than the eval step; and/or
		//   (b) the number of samples is relatively small.
		//
		// so a linear search will be as fast as a binary search.

		//（1）确定 out 中 < mint 的样本数
		var drop int
		for drop = 0; out[drop].T < mint; drop++ {
		}

		//（2）从 out 中移除这些样本，使 out 中只包含 >= mint 的样本
		copy(out, out[drop:])
		out = out[:len(out)-drop]


		// Only append points with timestamps after the last timestamp we have.
		//
		//（3）更新 mint 的值
		mint = out[len(out)-1].T + 1

	} else {
		out = out[:0]
	}

	// 查找 >= maxt 的样本点
	ok := it.Seek(maxt)
	if !ok {
		if it.Err() != nil {
			ev.error(it.Err())
		}
	}

	// buffer 中保存 < maxt 的样本点
	buf := it.Buffer()

	// 遍历这些 < maxt 的样本点，如果其 <= mint ，则添加到 out 中
	for buf.Next() {

		t, v := buf.At()

		if value.IsStaleNaN(v) {
			continue
		}

		// Values in the buffer are guaranteed to be smaller than maxt.
		if t >= mint {

			if ev.currentSamples >= ev.maxSamples {
				ev.error(ErrTooManySamples(env))
			}

			out = append(out,
				Point{
					T: t,
					V: v,
				},
			)
			ev.currentSamples++
		}

	}


	// The seeked sample might also be in the range.

	if ok {

		t, v := it.Values()

		if t == maxt && !value.IsStaleNaN(v) {


			if ev.currentSamples >= ev.maxSamples {
				ev.error(ErrTooManySamples(env))
			}


			out = append(out,
				Point{
					T: t,
					V: v,
				},
			)
			ev.currentSamples++

		}
	}

	return out
}






func (ev *evaluator) VectorAnd(lhs, rhs Vector, matching *parser.VectorMatching, enh *EvalNodeHelper) Vector {

	if matching.Card != parser.CardManyToMany {
		panic("set operations must only use many-to-many matching")
	}

	sigf := enh.signatureFunc(matching.On, matching.MatchingLabels...)


	// The set of signatures for the right-hand side Vector.
	rightSigs := map[uint64]struct{}{}

	// Add all rhs samples to a map so we can easily find matches later.
	for _, rs := range rhs {
		rightSigs[sigf(rs.Metric)] = struct{}{}
	}

	for _, ls := range lhs {
		// If there's a matching entry in the right-hand side Vector, add the sample.
		if _, ok := rightSigs[sigf(ls.Metric)]; ok {
			enh.out = append(enh.out, ls)
		}
	}
	return enh.out
}

func (ev *evaluator) VectorOr(lhs, rhs Vector, matching *parser.VectorMatching, enh *EvalNodeHelper) Vector {
	if matching.Card != parser.CardManyToMany {
		panic("set operations must only use many-to-many matching")
	}
	sigf := enh.signatureFunc(matching.On, matching.MatchingLabels...)

	leftSigs := map[uint64]struct{}{}
	// Add everything from the left-hand-side Vector.
	for _, ls := range lhs {
		leftSigs[sigf(ls.Metric)] = struct{}{}
		enh.out = append(enh.out, ls)
	}
	// Add all right-hand side elements which have not been added from the left-hand side.
	for _, rs := range rhs {
		if _, ok := leftSigs[sigf(rs.Metric)]; !ok {
			enh.out = append(enh.out, rs)
		}
	}
	return enh.out
}

func (ev *evaluator) VectorUnless(lhs, rhs Vector, matching *parser.VectorMatching, enh *EvalNodeHelper) Vector {

	if matching.Card != parser.CardManyToMany {
		panic("set operations must only use many-to-many matching")
	}


	sigf := enh.signatureFunc(matching.On, matching.MatchingLabels...)

	rightSigs := map[uint64]struct{}{}
	for _, rs := range rhs {
		rightSigs[sigf(rs.Metric)] = struct{}{}
	}

	for _, ls := range lhs {
		if _, ok := rightSigs[sigf(ls.Metric)]; !ok {
			enh.out = append(enh.out, ls)
		}
	}
	return enh.out
}

// VectorBinop evaluates a binary operation between two Vectors, excluding set operators.
func (ev *evaluator) VectorBinop(op parser.ItemType, lhs, rhs Vector, matching *parser.VectorMatching, returnBool bool, enh *EvalNodeHelper) Vector {

	if matching.Card == parser.CardManyToMany {
		panic("many-to-many only allowed for set operators")
	}

	sigf := enh.signatureFunc(matching.On, matching.MatchingLabels...)


	// The control flow below handles one-to-one or many-to-one matching.
	// For one-to-many, swap sidedness and account for the swap when calculating values.
	if matching.Card == parser.CardOneToMany {
		lhs, rhs = rhs, lhs
	}

	// All samples from the rhs hashed by the matching label/values.
	if enh.rightSigs == nil {
		enh.rightSigs = make(map[uint64]Sample, len(enh.out))
	} else {
		for k := range enh.rightSigs {
			delete(enh.rightSigs, k)
		}
	}
	rightSigs := enh.rightSigs





	// Add all rhs samples to a map so we can easily find matches later.
	for _, rs := range rhs {

		sig := sigf(rs.Metric)

		// The rhs is guaranteed to be the 'one' side.
		//
		// Having multiple samples with the same signature means that the matching is many-to-many.
		if duplSample, found := rightSigs[sig]; found {
			// oneSide represents which side of the vector represents the 'one' in the many-to-one relationship.
			oneSide := "right"
			if matching.Card == parser.CardOneToMany {
				oneSide = "left"
			}
			matchedLabels := rs.Metric.MatchLabels(matching.On, matching.MatchingLabels...)
			// Many-to-many matching not allowed.
			ev.errorf("found duplicate series for the match group %s on the %s hand-side of the operation: [%s, %s]"+
				";many-to-many matching not allowed: matching labels must be unique on one side", matchedLabels.String(), oneSide, rs.Metric.String(), duplSample.Metric.String())
		}
		rightSigs[sig] = rs
	}




	// Tracks the match-signature.
	//
	// For one-to-one operations the value is nil.
	// For many-to-one the value is a set of signatures to detect duplicated result elements.
	if enh.matchedSigs == nil {
		enh.matchedSigs = make(map[uint64]map[uint64]struct{}, len(rightSigs))
	} else {
		for k := range enh.matchedSigs {
			delete(enh.matchedSigs, k)
		}
	}
	matchedSigs := enh.matchedSigs

	// For all lhs samples find a respective rhs sample and perform the binary operation.
	for _, ls := range lhs {
		sig := sigf(ls.Metric)


		rs, found := rightSigs[sig] // Look for a match in the rhs Vector.
		if !found {
			continue
		}

		// Account for potentially swapped sidedness.
		vl, vr := ls.V, rs.V
		if matching.Card == parser.CardOneToMany {
			vl, vr = vr, vl
		}
		value, keep := vectorElemBinop(op, vl, vr)
		if returnBool {
			if keep {
				value = 1.0
			} else {
				value = 0.0
			}
		} else if !keep {
			continue
		}
		metric := resultMetric(ls.Metric, rs.Metric, op, matching, enh)

		insertedSigs, exists := matchedSigs[sig]
		if matching.Card == parser.CardOneToOne {


			if exists {
				ev.errorf("multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)")
			}
			matchedSigs[sig] = nil // Set existence to true.


		} else {

			// In many-to-one matching the grouping labels have to ensure a unique metric for the result Vector.
			//
			// Check whether those labels have already been added for the same matching labels.

			insertSig := metric.Hash()

			if !exists {

				insertedSigs = map[uint64]struct{}{}
				matchedSigs[sig] = insertedSigs

			} else if _, duplicate := insertedSigs[insertSig]; duplicate {

				ev.errorf("multiple matches for labels: grouping labels must ensure unique matches")

			}

			insertedSigs[insertSig] = struct{}{}

		}

		enh.out = append(enh.out, Sample{
			Metric: metric,
			Point:  Point{V: value},
		})
	}
	return enh.out
}





// signatureFunc returns a function that calculates the signature for a metric ignoring the provided labels.
// If on, then the given labels are only used instead.
func signatureFunc(on bool, names ...string) func(labels.Labels) uint64 {

	sort.Strings(names)

	if on {
		return func(lset labels.Labels) uint64 {
			h, _ := lset.HashForLabels(make([]byte, 0, 1024), names...)
			return h
		}
	}

	return func(lset labels.Labels) uint64 {
		h, _ := lset.HashWithoutLabels(make([]byte, 0, 1024), names...)
		return h
	}
}





// resultMetric returns the metric for the given sample(s) based on the Vector
// binary operation and the matching options.
func resultMetric(lhs, rhs labels.Labels, op parser.ItemType, matching *parser.VectorMatching, enh *EvalNodeHelper) labels.Labels {

	if enh.resultMetric == nil {
		enh.resultMetric = make(map[uint64]labels.Labels, len(enh.out))
	}


	// op and matching are always the same for a given node, so there's no need to include them in the hash key.
	// If the lhs and rhs are the same then the xor would be 0, so add in one side to protect against that.
	lh := lhs.Hash()
	h := (lh ^ rhs.Hash()) + lh
	if ret, ok := enh.resultMetric[h]; ok {
		return ret
	}

	lb := labels.NewBuilder(lhs)

	if shouldDropMetricName(op) {
		lb.Del(labels.MetricName)
	}

	if matching.Card == parser.CardOneToOne {
		if matching.On {
		Outer:
			for _, l := range lhs {
				for _, n := range matching.MatchingLabels {
					if l.Name == n {
						continue Outer
					}
				}
				lb.Del(l.Name)
			}
		} else {
			lb.Del(matching.MatchingLabels...)
		}
	}
	for _, ln := range matching.Include {
		// Included labels from the `group_x` modifier are taken from the "one"-side.
		if v := rhs.Get(ln); v != "" {
			lb.Set(ln, v)
		} else {
			lb.Del(ln)
		}
	}

	ret := lb.Labels()
	enh.resultMetric[h] = ret
	return ret
}

// VectorscalarBinop evaluates a binary operation between a Vector and a Scalar.
func (ev *evaluator) VectorscalarBinop(op parser.ItemType, lhs Vector, rhs Scalar, swap, returnBool bool, enh *EvalNodeHelper) Vector {



	for _, lhsSample := range lhs {



		lv, rv := lhsSample.V, rhs.V


		// lhs always contains the Vector. If the original position was different swap for calculating the value.
		if swap {
			lv, rv = rv, lv
		}
		value, keep := vectorElemBinop(op, lv, rv)


		// Catch cases where the scalar is the LHS in a scalar-vector comparison operation.
		// We want to always keep the vector element value as the output value, even if it's on the RHS.
		if op.IsComparisonOperator() && swap {
			value = rv
		}
		if returnBool {
			if keep {
				value = 1.0
			} else {
				value = 0.0
			}
			keep = true
		}
		if keep {
			lhsSample.V = value
			if shouldDropMetricName(op) || returnBool {
				lhsSample.Metric = enh.dropMetricName(lhsSample.Metric)
			}
			enh.out = append(enh.out, lhsSample)
		}
	}
	return enh.out
}

func dropMetricName(l labels.Labels) labels.Labels {
	return labels.NewBuilder(l).Del(labels.MetricName).Labels()
}

// scalarBinop evaluates a binary operation between two Scalars.
func scalarBinop(op parser.ItemType, lhs, rhs float64) float64 {
	switch op {
	case parser.ADD:
		return lhs + rhs
	case parser.SUB:
		return lhs - rhs
	case parser.MUL:
		return lhs * rhs
	case parser.DIV:
		return lhs / rhs
	case parser.POW:
		return math.Pow(lhs, rhs)
	case parser.MOD:
		return math.Mod(lhs, rhs)
	case parser.EQL:
		return btos(lhs == rhs)
	case parser.NEQ:
		return btos(lhs != rhs)
	case parser.GTR:
		return btos(lhs > rhs)
	case parser.LSS:
		return btos(lhs < rhs)
	case parser.GTE:
		return btos(lhs >= rhs)
	case parser.LTE:
		return btos(lhs <= rhs)
	}
	panic(errors.Errorf("operator %q not allowed for Scalar operations", op))
}

// vectorElemBinop evaluates a binary operation between two Vector elements.
func vectorElemBinop(op parser.ItemType, lhs, rhs float64) (float64, bool) {
	switch op {
	case parser.ADD:
		return lhs + rhs, true
	case parser.SUB:
		return lhs - rhs, true
	case parser.MUL:
		return lhs * rhs, true
	case parser.DIV:
		return lhs / rhs, true
	case parser.POW:
		return math.Pow(lhs, rhs), true
	case parser.MOD:
		return math.Mod(lhs, rhs), true
	case parser.EQL:
		return lhs, lhs == rhs
	case parser.NEQ:
		return lhs, lhs != rhs
	case parser.GTR:
		return lhs, lhs > rhs
	case parser.LSS:
		return lhs, lhs < rhs
	case parser.GTE:
		return lhs, lhs >= rhs
	case parser.LTE:
		return lhs, lhs <= rhs
	}
	panic(errors.Errorf("operator %q not allowed for operations between Vectors", op))
}



type groupedAggregation struct {
	labels      labels.Labels
	value       float64
	mean        float64
	groupCount  int
	heap        vectorByValueHeap
	reverseHeap vectorByReverseValueHeap
}




// aggregation evaluates an aggregation operation on a Vector.
//
//
// 聚合操作语法:
// 	<aggr-op>([parameter,] <vector expression>) [without|by (<label list>)]
//
// 参数对应关系:
// 	<aggr-op> : op
//  parameter : param
//  <vector expression> : vec
//  without|by : without
//  <label list> : grouping
//
func (ev *evaluator) aggregation(op parser.ItemType, grouping []string, without bool, param interface{}, vec Vector, enh *EvalNodeHelper) Vector {

	result := map[uint64]*groupedAggregation{}

	// 参数解析：只有 count_values，quantile，topk，bottomk 支持参数。
	var k int64
	if op == parser.TOPK || op == parser.BOTTOMK {
		f := param.(float64)
		if !convertibleToInt64(f) {
			ev.errorf("Scalar value %v overflows int64", f)
		}
		k = int64(f)
		if k < 1 {
			return Vector{}
		}
	}

	var q float64
	if op == parser.QUANTILE {
		q = param.(float64)
	}

	var valueLabel string
	if op == parser.COUNT_VALUES {
		// count_values() 操作增加的附加标签名由参数指定
		valueLabel = param.(string)
		// 检查是否合法标签名
		if !model.LabelName(valueLabel).IsValid() {
			ev.errorf("invalid label name %q", valueLabel)
		}
		// by ?
		if !without {
			grouping = append(grouping, valueLabel)
		}
	}


	// 对 grouping 进行排序，以便后续调用 metric.HashWithoutLabels() 和 metric.HashForLabels() 函数
	sort.Strings(grouping)

	lb := labels.NewBuilder(nil)
	buf := make([]byte, 0, 1024)


	// 遍历矢量数据 samples vector
	for _, s := range vec {

		// 取出当前 sample 的标签集
		metric := s.Metric

		// 如果是 count_values() 操作，则添加附加标签到当前 sample 上
		if op == parser.COUNT_VALUES {
			lb.Reset(metric)
			// count_values() 操作增加的附加标签的值为样本值
			lb.Set(valueLabel, strconv.FormatFloat(s.V, 'f', -1, 64))
			// 重置标签集合
			metric = lb.Labels()
		}

		var (
			groupingKey uint64
		)

		// 计算聚合 key
		//
		// 具有相同 labels 集的 samples 的会聚合到同一个 key 下面进行聚合运算。
		// 这里 hash 值是这组 labels 的代表，方便代码处理。


		// without 操作，会移除 grouping 标签和 "__name__" 标签，计算 hash 值也要忽略它们:
		if without {
			// HashWithoutLabels 返回 ls 中与 names 不匹配的标签集合的 hash 值，注意，也会忽略 "__name__" 标签。
			groupingKey, buf = metric.HashWithoutLabels(buf, grouping...)

		// by 操作，只保留 grouping 标签，计算 hash 值仅需要参考它们:
		} else {
			// HashForLabels 返回 ls 中与 names 匹配的标签集合的 hash 值。
			groupingKey, buf = metric.HashForLabels(buf, grouping...)
		}

		// 聚合结果保存在 result 中
		group, ok := result[groupingKey]

		// Add a new group if it doesn't exist.
		if !ok {

			// 需保留的标签集
			var m labels.Labels

			// 如果是 without 操作，需要移除 grouping 标签和 "__name__" 标签。
			if without {
				lb.Reset(metric)
				lb.Del(grouping...)
				lb.Del(labels.MetricName)
				m = lb.Labels()
			// 如果是 by 操作，需要移除 grouping 之外的标签，只保留 grouping 标签。
			} else {
				m = make(labels.Labels, 0, len(grouping))
				for _, l := range metric {
					for _, n := range grouping {
						if l.Name == n {
							m = append(m, l)
							break
						}
					}
				}
				// 确保标签有序
				sort.Sort(m)
			}


			// 设置聚合结果的初始值
			result[groupingKey] = &groupedAggregation{
				labels:     m,
				value:      s.V,
				mean:       s.V,
				groupCount: 1,
			}


			inputVecLen := int64(len(vec))
			resultSize := k
			if k > inputVecLen {
				resultSize = inputVecLen
			}

			if op == parser.STDVAR || op == parser.STDDEV {
				result[groupingKey].value = 0.0
			} else if op == parser.TOPK || op == parser.QUANTILE {
				result[groupingKey].heap = make(vectorByValueHeap, 0, resultSize)
				heap.Push(&result[groupingKey].heap, &Sample{
					Point:  Point{V: s.V},
					Metric: s.Metric,
				})
			} else if op == parser.BOTTOMK {
				result[groupingKey].reverseHeap = make(vectorByReverseValueHeap, 0, resultSize)
				heap.Push(&result[groupingKey].reverseHeap, &Sample{
					Point:  Point{V: s.V},
					Metric: s.Metric,
				})
			}

			continue
		}


		// 开始执行聚合操作，将当前样本点 sample 聚合到 result[groupingKey] 也即变量 group 上。

		switch op {
		case parser.SUM: 	// 累加
			group.value += s.V

		case parser.AVG: 	// 均值
			group.groupCount++
			group.mean += (s.V - group.mean) / float64(group.groupCount)

		case parser.MAX:	// 最大值
			if group.value < s.V || math.IsNaN(group.value) {
				group.value = s.V
			}

		case parser.MIN:	// 最小值
			if group.value > s.V || math.IsNaN(group.value) {
				group.value = s.V
			}

		case parser.COUNT, parser.COUNT_VALUES:	// 样本数
			group.groupCount++

		case parser.STDVAR, parser.STDDEV:		// 标准差
			group.groupCount++
			delta := s.V - group.mean
			group.mean += delta / float64(group.groupCount)
			group.value += delta * (s.V - group.mean)

		case parser.TOPK:
			// 先判断要不要入堆，若需要则 pop + push
			if int64(len(group.heap)) < k || group.heap[0].V < s.V || math.IsNaN(group.heap[0].V) {
				if int64(len(group.heap)) == k {
					heap.Pop(&group.heap)
				}
				heap.Push(&group.heap, &Sample{
					Point:  Point{V: s.V},
					Metric: s.Metric,
				})
			}

		case parser.BOTTOMK:
			// 先判断要不要入堆，若需要则 pop + push
			if int64(len(group.reverseHeap)) < k || group.reverseHeap[0].V > s.V || math.IsNaN(group.reverseHeap[0].V) {
				if int64(len(group.reverseHeap)) == k {
					heap.Pop(&group.reverseHeap)
				}
				heap.Push(&group.reverseHeap, &Sample{
					Point:  Point{V: s.V},
					Metric: s.Metric,
				})
			}

		case parser.QUANTILE:	// 直接入堆
			group.heap = append(group.heap, s)

		default:
			panic(errors.Errorf("expected aggregation operator but got %q", op))
		}
	}


	// Construct the result Vector from the aggregated groups.
	for _, aggr := range result {

		switch op {
		case parser.AVG:
			aggr.value = aggr.mean

		case parser.COUNT, parser.COUNT_VALUES:
			aggr.value = float64(aggr.groupCount)

		case parser.STDVAR:
			aggr.value = aggr.value / float64(aggr.groupCount)

		case parser.STDDEV:
			aggr.value = math.Sqrt(aggr.value / float64(aggr.groupCount))

		case parser.TOPK:

			// The heap keeps the lowest value on top, so reverse it.
			sort.Sort(sort.Reverse(aggr.heap))

			//
			for _, v := range aggr.heap {
				enh.out = append(enh.out, Sample{
					Metric: v.Metric,
					Point:  Point{V: v.V},
				})
			}
			continue // Bypass default append.

		case parser.BOTTOMK:
			// The heap keeps the lowest value on top, so reverse it.
			sort.Sort(sort.Reverse(aggr.reverseHeap))
			for _, v := range aggr.reverseHeap {
				enh.out = append(enh.out, Sample{
					Metric: v.Metric,
					Point:  Point{V: v.V},
				})
			}
			continue // Bypass default append.

		case parser.QUANTILE:
			aggr.value = quantile(q, aggr.heap)

		default:
			// For other aggregations, we already have the right value.
		}

		enh.out = append(enh.out, Sample{
			Metric: aggr.labels,
			Point:  Point{V: aggr.value},
		})
	}


	return enh.out
}

// btos returns 1 if b is true, 0 otherwise.
func btos(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// shouldDropMetricName returns whether the metric name should be dropped in the result of the op operation.
//
//
func shouldDropMetricName(op parser.ItemType) bool {
	switch op {
	case parser.ADD, parser.SUB, parser.DIV, parser.MUL, parser.POW, parser.MOD:
		return true
	default:
		return false
	}
}

// NewOriginContext returns a new context with data about the origin attached.
func NewOriginContext(ctx context.Context, data map[string]interface{}) context.Context {
	return context.WithValue(ctx, queryOrigin{}, data)
}

func formatDate(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

// unwrapParenExpr does the AST equivalent of removing parentheses around a expression.
func unwrapParenExpr(e *parser.Expr) {
	for {
		if p, ok := (*e).(*parser.ParenExpr); ok {
			*e = p.Expr
		} else {
			break
		}
	}
}

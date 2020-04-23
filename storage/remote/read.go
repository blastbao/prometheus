// Copyright 2017 The Prometheus Authors
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

package remote

import (
	"context"
	"fmt"

	"github.com/blastbao/prometheus/pkg/labels"
	"github.com/blastbao/prometheus/storage"
	"github.com/prometheus/client_golang/prometheus"
)

var remoteReadQueries = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "remote_read_queries",
		Help:      "The number of in-flight remote read queries.",
	},
	[]string{remoteName, endpoint},
)

func init() {
	prometheus.MustRegister(remoteReadQueries)
}



// QueryableClient returns a storage.Queryable which queries the given Client to select series sets.
//
//
func QueryableClient(c *Client) storage.Queryable {


	remoteReadQueries.WithLabelValues(c.remoteName, c.url.String())

	return storage.QueryableFunc(
		func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
			return &querier{
				ctx:    ctx,
				mint:   mint,
				maxt:   maxt,
				client: c,
			}, nil
		},
	)

}

// querier is an adapter to make a Client usable as a storage.Querier.
//
//
type querier struct {
	ctx        context.Context
	mint, maxt int64
	client     *Client
}



// Select implements storage.Querier and uses the given matchers to read series sets from the Client.
//
//
func (q *querier) Select(
	sortSeries bool,				// 是否排序
	hints *storage.SelectHints,		// 查询条件
	matchers ...*labels.Matcher,	// 标签匹配条件
) (
	storage.SeriesSet,				// 时序数据（迭代器）
	storage.Warnings,				// 警告
	error,							// 错误
) {


	// 构造 prompb.Query 对象
	query, err := ToQuery(q.mint, q.maxt, matchers, hints)
	if err != nil {
		return nil, nil, err
	}

	// 上报
	remoteReadGauge := remoteReadQueries.WithLabelValues(q.client.remoteName, q.client.url.String())
	remoteReadGauge.Inc()
	defer remoteReadGauge.Dec()


	// 发送 prompb.Query 查询请求到 remote storage 得到 prompb.QueryResult 查询结果
	res, err := q.client.Read(q.ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("remote_read: %v", err)
	}

	// 把 res 从 prompb.QueryResult 结果转换成 storage.SeriesSet 迭代器
	return FromQueryResult(sortSeries, res), nil, nil
}


// LabelValues implements storage.Querier and is a noop.
func (q *querier) LabelValues(string) ([]string, storage.Warnings, error) {
	// TODO implement?
	return nil, nil, nil
}

// LabelNames implements storage.Querier and is a noop.
func (q *querier) LabelNames() ([]string, storage.Warnings, error) {
	// TODO implement?
	return nil, nil, nil
}

// Close implements storage.Querier and is a noop.
func (q *querier) Close() error {
	return nil
}


// ExternalLabelsHandler returns a storage.Queryable which creates a externalLabelsQuerier.
func ExternalLabelsHandler(next storage.Queryable, externalLabels labels.Labels) storage.Queryable {

	return storage.QueryableFunc(

		func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {


			q, err := next.Querier(ctx, mint, maxt)

			if err != nil {
				return nil, err
			}

			return &externalLabelsQuerier{
				Querier: q,
				externalLabels: externalLabels,
			}, nil

		},

	)
}



// externalLabelsQuerier is a querier which ensures that Select() results match the configured external labels.
type externalLabelsQuerier struct {



	storage.Querier



	externalLabels labels.Labels


}





// Select adds equality matchers for all external labels to the list of matchers
// before calling the wrapped storage.Queryable.
//
// The added external labels are removed from the returned series sets.
func (q externalLabelsQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {

	// 根据 q.externalLabels 构造新的 matcher(s) 添加到参数 matchers 中，返回添加后的完整 matchers(m) 和被新添加的 ExternalLabels(added)
	m, added := q.addExternalLabels(matchers)

	// 用新的 matchers(m) 去查询
	s, warnings, err := q.Querier.Select(sortSeries, hints, m...)
	if err != nil {
		return nil, warnings, err
	}

	// The added external labels are removed from the returned series sets.
	return newSeriesSetFilter(s, added), warnings, nil
}



// PreferLocalStorageFilter returns a QueryableFunc which creates a NoopQuerier
// if requested timeframe can be answered completely by the local TSDB,
// and reduces maxt if the timeframe can be partially answered by TSDB.
func PreferLocalStorageFilter(next storage.Queryable, cb startTimeCallback) storage.Queryable {

	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {

		localStartTime, err := cb()
		if err != nil {
			return nil, err
		}
		cmaxt := maxt
		// Avoid queries whose timerange is later than the first timestamp in local DB.
		if mint > localStartTime {
			return storage.NoopQuerier(), nil
		}
		// Query only samples older than the first timestamp in local DB.
		if maxt > localStartTime {
			cmaxt = localStartTime
		}
		return next.Querier(ctx, mint, cmaxt)
	})
}

// RequiredMatchersFilter returns a storage.Queryable which creates a requiredMatchersQuerier.
func RequiredMatchersFilter(next storage.Queryable, required []*labels.Matcher) storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		q, err := next.Querier(ctx, mint, maxt)
		if err != nil {
			return nil, err
		}
		return &requiredMatchersQuerier{Querier: q, requiredMatchers: required}, nil
	})
}



// requiredMatchersQuerier wraps a storage.Querier and requires Select() calls to match the given labelSet.
type requiredMatchersQuerier struct {
	storage.Querier
	requiredMatchers []*labels.Matcher
}



// Select returns a NoopSeriesSet if the given matchers don't match the label set of the requiredMatchersQuerier.
//
// Otherwise it'll call the wrapped querier.
func (q requiredMatchersQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {

	// ms 中存储了 required matcher
	ms := q.requiredMatchers

	// 若 ms 中的某个 watcher 存在于 matchers 中，就从 ms 中移除该 matcher
	for _, m := range matchers {
		for i, r := range ms {
			// 如果 m 和 r 的匹配条件完全相同，就从 ms 中移除 r
			if m.Type == labels.MatchEqual && m.Name == r.Name && m.Value == r.Value {
				ms = append(ms[:i], ms[i+1:]...) // 从 ms 中移除 ms[i]，注意，此时 i 已经指向新元素
				break
			}
		}
		if len(ms) == 0 {
			break
		}
	}

	// 如果 ms 中有剩余的 watcher ，意味着 matchers 中不存在这些 watcher ，返回 noop～
	if len(ms) > 0 {
		return storage.NoopSeriesSet(), nil, nil
	}

	// 否则，matchers 中完整包含了 ms ，就用 matchers 来查询
	return q.Querier.Select(sortSeries, hints, matchers...)
}

// addExternalLabels adds matchers for each external label.
//
// External labels that already have a corresponding user-supplied matcher are skipped,
// as we assume that the user explicitly wants to select a different value for them.
//
// We return the new set of matchers, along with a map of labels for which matchers were added,
// so that these can later be removed from the result time series again.


func (q externalLabelsQuerier) addExternalLabels(matchers []*labels.Matcher) ([]*labels.Matcher, labels.Labels) {

	el := make(labels.Labels, len(q.externalLabels))
	copy(el, q.externalLabels)

	// ms won't be sorted, so have to O(n^2) the search.
	//
	// 移除 el 中的已经出现在 matchers 的标签，因为 matchers 未排序，所以需要二重循环，时间复杂度 O(n*n)。
	for _, matcher := range matchers {
		// 移除 el 中与当前 matcher 同名的标签
		for i := 0; i < len(el); {
			if matcher.Name == el[i].Name {
				el = el[:i+copy(el[i:], el[i+1:])] 	// 从 el 中移除 el[i]，注意，此时 i 已经指向新元素
				continue
			}
			i++
		}
	}

	// 至此，el 中剩下的都是需要构造新 matcher 的标签，下面便逐个构造
	for _, l := range el {
		// 创建新 matcher
		m, err := labels.NewMatcher(labels.MatchEqual, l.Name, l.Value)
		if err != nil {
			panic(err)
		}
		// 添加到 matchers
		matchers = append(matchers, m)
	}

	return matchers, el
}



// The toFilter labels are removed from series sets ss .

func newSeriesSetFilter(ss storage.SeriesSet, toFilter labels.Labels) storage.SeriesSet {
	return &seriesSetFilter{
		SeriesSet: ss,
		toFilter:  toFilter,
	}
}

type seriesSetFilter struct {
	storage.SeriesSet
	toFilter labels.Labels
	querier  storage.Querier
}

func (ssf *seriesSetFilter) GetQuerier() storage.Querier {
	return ssf.querier
}

func (ssf *seriesSetFilter) SetQuerier(querier storage.Querier) {
	ssf.querier = querier
}

func (ssf seriesSetFilter) At() storage.Series {
	return seriesFilter{
		Series:   ssf.SeriesSet.At(),
		toFilter: ssf.toFilter,
	}
}

type seriesFilter struct {
	storage.Series
	toFilter labels.Labels
}

func (sf seriesFilter) Labels() labels.Labels {

	labels := sf.Series.Labels()

	// 若 labels 中的某个 label 存在于 sf.toFilter 中，就从 labels 中移除该 label 。
	//
	// 注: 因为 labels 和 sf.toFilter 都是按照 label name 排序的，下面只用一层循环即可搞定，o(n+m) 的复杂度。
	for i, j := 0, 0; i < len(labels) && j < len(sf.toFilter); {

		if labels[i].Name < sf.toFilter[j].Name {
			i++
		} else if labels[i].Name > sf.toFilter[j].Name {
			j++
		} else {
			labels = labels[:i+copy(labels[i:], labels[i+1:])]
			j++
		}
	}
	return labels
}

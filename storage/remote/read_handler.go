// Copyright The Prometheus Authors
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
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/gate"
)

type readHandler struct {
	logger                    *slog.Logger
	queryable                 storage.SampleAndChunkQueryable
	config                    func() config.Config
	remoteReadSampleLimit     int
	remoteReadMaxBytesInFrame int
	remoteReadGate            *gate.Gate
	queries                   prometheus.Gauge
	gateWaitDuration          prometheus.Histogram
	externalLabelConflicts    prometheus.Counter
	marshalPool               *sync.Pool
}

// NewReadHandler creates a http.Handler that accepts remote read requests and
// writes them to the provided queryable.
// The handler panics with http.ErrAbortHandler if a streaming response fails after it has started.
func NewReadHandler(logger *slog.Logger, r prometheus.Registerer, queryable storage.SampleAndChunkQueryable, config func() config.Config, remoteReadSampleLimit, remoteReadConcurrencyLimit, remoteReadMaxBytesInFrame int) http.Handler {
	h := &readHandler{
		logger:                    logger,
		queryable:                 queryable,
		config:                    config,
		remoteReadSampleLimit:     remoteReadSampleLimit,
		remoteReadGate:            gate.New(remoteReadConcurrencyLimit),
		remoteReadMaxBytesInFrame: remoteReadMaxBytesInFrame,
		marshalPool:               &sync.Pool{},

		queries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "remote_read_handler",
			Name:      "queries",
			Help:      "The current number of remote read queries that are either in execution or queued on the handler.",
		}),
		gateWaitDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:                       namespace,
			Subsystem:                       "remote_read_handler",
			Name:                            "gate_wait_duration_seconds",
			Help:                            "How long a remote read request spent waiting for the concurrency gate, including requests canceled while waiting.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
		}),
		externalLabelConflicts: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "remote_read_handler",
			Name:      "external_label_conflicts_total",
			Help:      "The number of remote read queries that selected series with and without a label named like an external label.",
		}),
	}
	if r != nil {
		r.MustRegister(h.queries, h.gateWaitDuration, h.externalLabelConflicts)
	}
	return h
}

func (h *readHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	gateStart := time.Now()
	err := h.remoteReadGate.Start(ctx)
	// Observe before checking the error so requests that are canceled while waiting on the gate
	// are recorded too.
	h.gateWaitDuration.Observe(time.Since(gateStart).Seconds())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.queries.Inc()

	defer h.remoteReadGate.Done()
	defer h.queries.Dec()

	req, err := DecodeReadRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	externalLabels := h.config().GlobalConfig.ExternalLabels.Map()

	sortedExternalLabels := make([]prompb.Label, 0, len(externalLabels))
	for name, value := range externalLabels {
		sortedExternalLabels = append(sortedExternalLabels, prompb.Label{
			Name:  name,
			Value: value,
		})
	}
	slices.SortFunc(sortedExternalLabels, func(a, b prompb.Label) int {
		return strings.Compare(a.Name, b.Name)
	})

	responseType, err := NegotiateResponseType(req.AcceptedResponseTypes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch responseType {
	case prompb.ReadRequest_STREAMED_XOR_CHUNKS:
		h.remoteReadStreamedXORChunks(ctx, w, req, externalLabels, sortedExternalLabels)
	default:
		// On empty or unknown types in req.AcceptedResponseTypes we default to non streamed, raw samples response.
		h.remoteReadSamples(ctx, w, req, externalLabels, sortedExternalLabels)
	}
}

func (h *readHandler) remoteReadSamples(
	ctx context.Context,
	w http.ResponseWriter,
	req *prompb.ReadRequest,
	externalLabels map[string]string,
	sortedExternalLabels []prompb.Label,
) {
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	resp := prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, len(req.Queries)),
	}
	for i, query := range req.Queries {
		if err := func() error {
			filteredMatchers, err := filterExtLabelsFromMatchers(query.Matchers, externalLabels)
			if err != nil {
				return err
			}

			querier, err := h.queryable.Querier(query.StartTimestampMs, query.EndTimestampMs)
			if err != nil {
				return err
			}
			defer func() {
				if err := querier.Close(); err != nil {
					h.logger.Warn("Error on querier close", "err", err.Error())
				}
			}()

			var hints *storage.SelectHints
			if query.Hints != nil {
				hints = &storage.SelectHints{
					Start:    query.Hints.StartMs,
					End:      query.Hints.EndMs,
					Step:     query.Hints.StepMs,
					Func:     query.Hints.Func,
					Grouping: query.Hints.Grouping,
					Range:    query.Hints.RangeMs,
					By:       query.Hints.By,
				}
			}

			var (
				ss        storage.SeriesSet
				extLabels = sortedExternalLabels
			)
			if keeps := h.externalLabelPartitions(ctx, querier, filteredMatchers, sortedExternalLabels); keeps != nil {
				// Select every part before iterating any, as the fanout storage requires.
				sets := make([]storage.SeriesSet, 0, len(keeps))
				for _, keep := range keeps {
					sets = append(sets, newExternalLabelsSeriesSet(querier.Select(ctx, true, hints, filteredMatchers...), keep, sortedExternalLabels, withSeriesLabels))
				}
				ss = storage.NewMergeSeriesSet(sets, 0, storage.ChainedSeriesMerge)
				extLabels = nil
			} else {
				ss = querier.Select(ctx, false, hints, filteredMatchers...)
			}

			var ws annotations.Annotations
			resp.Results[i], ws, err = ToQueryResult(ss, h.remoteReadSampleLimit)
			if err != nil {
				return err
			}
			for _, w := range ws {
				h.logger.Warn("Warnings on remote read query", "err", w.Error())
			}
			if len(extLabels) > 0 {
				for _, ts := range resp.Results[i].Timeseries {
					ts.Labels = MergeLabels(ts.Labels, extLabels)
				}
			}
			return nil
		}(); err != nil {
			if httpErr, ok := errors.AsType[HTTPError](err); ok {
				http.Error(w, httpErr.Error(), httpErr.Status())
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if err := EncodeReadResponse(&resp, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *readHandler) remoteReadStreamedXORChunks(ctx context.Context, w http.ResponseWriter, req *prompb.ReadRequest, externalLabels map[string]string, sortedExternalLabels []prompb.Label) {
	w.Header().Set("Content-Type", "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse")

	f, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "internal http.ResponseWriter does not implement http.Flusher interface", http.StatusInternalServerError)
		return
	}

	sw := &streamedReadWriter{Writer: w}
	cw := NewChunkedWriter(sw, f)

	for i, query := range req.Queries {
		if err := func() error {
			filteredMatchers, err := filterExtLabelsFromMatchers(query.Matchers, externalLabels)
			if err != nil {
				return err
			}

			querier, err := h.queryable.ChunkQuerier(query.StartTimestampMs, query.EndTimestampMs)
			if err != nil {
				return err
			}
			defer func() {
				if err := querier.Close(); err != nil {
					h.logger.Warn("Error on chunk querier close", "err", err.Error())
				}
			}()

			var hints *storage.SelectHints
			if query.Hints != nil {
				hints = &storage.SelectHints{
					Start:    query.Hints.StartMs,
					End:      query.Hints.EndMs,
					Step:     query.Hints.StepMs,
					Func:     query.Hints.Func,
					Grouping: query.Hints.Grouping,
					Range:    query.Hints.RangeMs,
					By:       query.Hints.By,
				}
			}

			var (
				ss        storage.ChunkSeriesSet
				extLabels = sortedExternalLabels
			)
			if keeps := h.externalLabelPartitions(ctx, querier, filteredMatchers, sortedExternalLabels); keeps != nil {
				// Select every part before iterating any, as the fanout storage requires.
				sets := make([]storage.ChunkSeriesSet, 0, len(keeps))
				for _, keep := range keeps {
					sets = append(sets, newExternalLabelsSeriesSet(querier.Select(ctx, true, hints, filteredMatchers...), keep, sortedExternalLabels, withChunkSeriesLabels))
				}
				ss = storage.NewMergeChunkSeriesSet(sets, 0, storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge))
				extLabels = nil
			} else {
				ss = querier.Select(ctx, true, hints, filteredMatchers...)
			}

			ws, err := StreamChunkedReadResponses(
				cw,
				int64(i),
				// The streaming API has to provide the series sorted.
				ss,
				extLabels,
				h.remoteReadMaxBytesInFrame,
				h.marshalPool,
			)
			if err != nil {
				return err
			}

			for _, w := range ws {
				h.logger.Warn("Warnings on chunked remote read query", "warnings", w.Error())
			}
			return nil
		}(); err != nil {
			if sw.started {
				if h.logger != nil {
					h.logger.Error("Error streaming remote read response", "err", err, "query_index", i)
				}
				// Returning normally would make the incomplete stream appear successful.
				panic(http.ErrAbortHandler)
			}
			if httpErr, ok := errors.AsType[HTTPError](err); ok {
				http.Error(w, httpErr.Error(), httpErr.Status())
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	cw.Close()
}

type streamedReadWriter struct {
	io.Writer
	started bool
}

func (w *streamedReadWriter) Write(p []byte) (int, error) {
	// A write can commit headers even when it returns no bytes and an error.
	w.started = true
	return w.Writer.Write(p)
}

// filterExtLabelsFromMatchers change equality matchers which match external labels
// to a matcher that looks for an empty label,
// as that label should not be present in the storage.
func filterExtLabelsFromMatchers(pbMatchers []*prompb.LabelMatcher, externalLabels map[string]string) ([]*labels.Matcher, error) {
	matchers, err := FromLabelMatchers(pbMatchers)
	if err != nil {
		return nil, err
	}

	filteredMatchers := make([]*labels.Matcher, 0, len(matchers))
	for _, m := range matchers {
		value := externalLabels[m.Name]
		if m.Type == labels.MatchEqual && value == m.Value {
			matcher, err := labels.NewMatcher(labels.MatchEqual, m.Name, "")
			if err != nil {
				return nil, err
			}
			filteredMatchers = append(filteredMatchers, matcher)
		} else {
			filteredMatchers = append(filteredMatchers, m)
		}
	}

	return filteredMatchers, nil
}

// externalLabelPartitions returns filters that split the series selected by
// matchers into parts that get the same external labels added, or nil when all
// of them do. A series that already has a label named like an external label
// keeps its own value (see honor_labels), so the series that have it and the
// series that do not get different labels added and do not stay in order. Each
// part is selected and ordered on its own, and merging the parts also returns
// series that end up with the same labels as one series.
func (h *readHandler) externalLabelPartitions(ctx context.Context, querier storage.LabelQuerier, matchers []*labels.Matcher, sortedExternalLabels []prompb.Label) []func(labels.Labels) bool {
	if len(sortedExternalLabels) == 0 {
		return nil
	}
	// The label queries below drop warnings: a remote read source behind the
	// fanout storage does not implement them and warns about it every time. An
	// error falls back to selecting all series together.
	fail := func(err error) []func(labels.Labels) bool {
		if h.logger != nil {
			h.logger.Debug("Selecting series together after failing to query external label names", "err", err)
		}
		return nil
	}

	// The names of the external labels that some of the selected series have.
	var names []string
	for _, l := range sortedExternalLabels {
		if slices.ContainsFunc(matchers, func(m *labels.Matcher) bool {
			// The matcher selects only series with the label, or only series without it.
			return m.Name == l.Name && (!m.Matches("") || m.Type == labels.MatchEqual && m.Value == "")
		}) {
			continue
		}
		values, _, err := querier.LabelValues(ctx, l.Name, &storage.LabelHints{Limit: 1}, matchers...)
		if err != nil {
			return fail(err)
		}
		if len(values) > 0 {
			names = append(names, l.Name)
		}
	}
	if len(names) == 0 {
		return nil
	}

	// One part for each combination of having and not having those labels
	// that some of the selected series have. The parts are selected with the
	// same matchers and filtered afterwards, so that a remote read source
	// behind the fanout storage still gets its external labels as matchers.
	var keeps []func(labels.Labels) bool
	// Bit i of has tells whether the series of a part have the label names[i].
	for has := range 1 << len(names) {
		ms := make([]*labels.Matcher, 0, len(matchers)+len(names))
		ms = append(ms, matchers...)
		for i, name := range names {
			typ := labels.MatchEqual
			if has&(1<<i) != 0 {
				typ = labels.MatchNotEqual
			}
			ms = append(ms, labels.MustNewMatcher(typ, name, ""))
		}
		found, _, err := querier.LabelNames(ctx, &storage.LabelHints{Limit: 1}, ms...)
		if err != nil {
			return fail(err)
		}
		if len(found) == 0 {
			continue
		}
		keeps = append(keeps, func(lset labels.Labels) bool {
			for i, name := range names {
				if lset.Has(name) != (has&(1<<i) != 0) {
					return false
				}
			}
			return true
		})
	}
	if len(keeps) < 2 {
		return nil
	}
	h.externalLabelConflicts.Inc()
	return keeps
}

// seriesSet is a storage.SeriesSet or a storage.ChunkSeriesSet.
type seriesSet[T storage.Labels] interface {
	Next() bool
	At() T
	Err() error
	Warnings() annotations.Annotations
}

// externalLabelsSeriesSet adds external labels to the series of a set sorted
// by labels, keeping the labels a series already has, and returns the series
// sorted by the resulting labels. It skips the series that keep returns false
// for, and all the other series must have the same external label names.
type externalLabelsSeriesSet[T storage.Labels] struct {
	set                  seriesSet[T]
	keep                 func(labels.Labels) bool
	sortedExternalLabels []prompb.Label
	withLabels           func(T, labels.Labels) T
	builder              labels.Builder
	order                externalLabelsOrder[T]
	cur                  T
	done                 bool
}

// newExternalLabelsSeriesSet returns an externalLabelsSeriesSet. withLabels
// returns a series with other labels, and sortedExternalLabels must not be
// empty.
func newExternalLabelsSeriesSet[T storage.Labels](set seriesSet[T], keep func(labels.Labels) bool, sortedExternalLabels []prompb.Label, withLabels func(T, labels.Labels) T) *externalLabelsSeriesSet[T] {
	return &externalLabelsSeriesSet[T]{
		set:                  set,
		keep:                 keep,
		sortedExternalLabels: sortedExternalLabels,
		withLabels:           withLabels,
		order: externalLabelsOrder[T]{
			compare:  func(a, b T) int { return labels.Compare(a.Labels(), b.Labels()) },
			lastName: sortedExternalLabels[len(sortedExternalLabels)-1].Name,
		},
	}
}

func (s *externalLabelsSeriesSet[T]) Next() bool {
	for {
		if series, ok := s.order.next(); ok {
			s.cur = series
			return true
		}
		if s.done {
			return false
		}
		if !s.set.Next() {
			s.done = true
			s.order.flush()
			continue
		}
		series := s.set.At()
		lset := series.Labels()
		if !s.keep(lset) {
			continue
		}
		var lastName string
		lset.Range(func(l labels.Label) { lastName = l.Name })
		s.builder.Reset(lset)
		for _, l := range s.sortedExternalLabels {
			if !lset.Has(l.Name) {
				s.builder.Set(l.Name, l.Value)
			}
		}
		s.order.add(s.withLabels(series, s.builder.Labels()), lastName)
	}
}

func (s *externalLabelsSeriesSet[T]) At() T { return s.cur }

func (s *externalLabelsSeriesSet[T]) Err() error { return s.set.Err() }

func (s *externalLabelsSeriesSet[T]) Warnings() annotations.Annotations { return s.set.Warnings() }

// seriesWithLabels is a series with other labels.
type seriesWithLabels struct {
	storage.Series
	lset labels.Labels
}

func (s seriesWithLabels) Labels() labels.Labels { return s.lset }

// withSeriesLabels returns s with the labels lset.
func withSeriesLabels(s storage.Series, lset labels.Labels) storage.Series {
	return seriesWithLabels{Series: s, lset: lset}
}

// chunkSeriesWithLabels is a chunk series with other labels.
type chunkSeriesWithLabels struct {
	storage.ChunkSeries
	lset labels.Labels
}

func (s chunkSeriesWithLabels) Labels() labels.Labels { return s.lset }

// withChunkSeriesLabels returns s with the labels lset.
func withChunkSeriesLabels(s storage.ChunkSeries, lset labels.Labels) storage.ChunkSeries {
	return chunkSeriesWithLabels{ChunkSeries: s, lset: lset}
}

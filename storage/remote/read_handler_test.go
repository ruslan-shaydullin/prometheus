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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/prometheus/prometheus/util/testutil"
)

func TestSampledReadEndpoint(t *testing.T) {
	executeSampledRead := func(
		t *testing.T,
		queryable storage.SampleAndChunkQueryable,
		getConfig func() config.Config,
		req *prompb.ReadRequest,
	) (int, *prompb.ReadResponse) {
		t.Helper()

		h := NewReadHandler(nil, nil, queryable, getConfig, 1e6, 1, 0)

		data, err := proto.Marshal(req)
		require.NoError(t, err)

		compressed := snappy.Encode(nil, data)
		request, err := http.NewRequest(http.MethodPost, "", bytes.NewBuffer(compressed))
		require.NoError(t, err)

		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, request)

		require.Equal(t, 2, recorder.Code/100)
		result := recorder.Result()
		require.Equal(t, "application/x-protobuf", result.Header.Get("Content-Type"))
		require.Equal(t, "snappy", result.Header.Get("Content-Encoding"))

		compressed, err = io.ReadAll(result.Body)
		require.NoError(t, err)

		uncompressed, err := snappy.Decode(nil, compressed)
		require.NoError(t, err)

		var resp prompb.ReadResponse
		require.NoError(t, proto.Unmarshal(uncompressed, &resp))
		return recorder.Code, &resp
	}

	t.Run("with external labels", func(t *testing.T) {
		store := promqltest.LoadedStorage(t, `
		load 1m
			test_metric1{foo="bar",baz="qux"} 1
	`)
		defer store.Close()

		addNativeHistogramsToTestSuite(t, store, 1)

		matcher1, err := labels.NewMatcher(labels.MatchEqual, "__name__", "test_metric1")
		require.NoError(t, err)

		matcher2, err := labels.NewMatcher(labels.MatchEqual, "d", "e")
		require.NoError(t, err)

		matcher3, err := labels.NewMatcher(labels.MatchEqual, "__name__", "test_histogram_metric1")
		require.NoError(t, err)

		matcher4, err := labels.NewMatcher(labels.MatchEqual, "__name__", "test_nhcb_metric1")
		require.NoError(t, err)

		query1, err := ToQuery(0, 1, []*labels.Matcher{matcher1, matcher2}, &storage.SelectHints{Step: 0, Func: "avg"})
		require.NoError(t, err)

		query2, err := ToQuery(0, 1, []*labels.Matcher{matcher3, matcher2}, &storage.SelectHints{Step: 0, Func: "avg"})
		require.NoError(t, err)

		query3, err := ToQuery(0, 1, []*labels.Matcher{matcher4, matcher2}, &storage.SelectHints{Step: 0, Func: "avg"})
		require.NoError(t, err)

		_, resp := executeSampledRead(t, store, func() config.Config {
			return config.Config{
				GlobalConfig: config.GlobalConfig{
					// We expect external labels to be added, with the source labels honored.
					ExternalLabels: labels.FromStrings("b", "c", "baz", "a", "d", "e"),
				},
			}
		}, &prompb.ReadRequest{Queries: []*prompb.Query{query1, query2, query3}})

		require.Len(t, resp.Results, 3, "Expected 3 results.")

		require.Equal(t, &prompb.QueryResult{
			Timeseries: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "test_metric1"},
						{Name: "b", Value: "c"},
						{Name: "baz", Value: "qux"},
						{Name: "d", Value: "e"},
						{Name: "foo", Value: "bar"},
					},
					Samples: []prompb.Sample{{Value: 1, Timestamp: 0}},
				},
			},
		}, resp.Results[0])

		require.Equal(t, &prompb.QueryResult{
			Timeseries: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "test_histogram_metric1"},
						{Name: "b", Value: "c"},
						{Name: "baz", Value: "qux"},
						{Name: "d", Value: "e"},
					},
					Histograms: []prompb.Histogram{
						prompb.FromFloatHistogram(0, tsdbutil.GenerateTestFloatHistogram(0)),
					},
				},
			},
		}, resp.Results[1])

		require.Equal(t, &prompb.QueryResult{
			Timeseries: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "test_nhcb_metric1"},
						{Name: "b", Value: "c"},
						{Name: "baz", Value: "qux"},
						{Name: "d", Value: "e"},
					},
					Histograms: []prompb.Histogram{{
						// We cannot use prompb.FromFloatHistogram as that's one
						// of the things we are testing here.
						Schema:    histogram.CustomBucketsSchema,
						Count:     &prompb.Histogram_CountFloat{CountFloat: 5},
						Sum:       18.4,
						ZeroCount: &prompb.Histogram_ZeroCountFloat{},
						PositiveSpans: []prompb.BucketSpan{
							{Offset: 0, Length: 2},
							{Offset: 1, Length: 2},
						},
						PositiveCounts: []float64{1, 2, 1, 1},
						CustomValues:   []float64{0, 1, 2, 3, 4},
					}},
				},
			},
		}, resp.Results[2])
	})

	t.Run("without external labels", func(t *testing.T) {
		store := promqltest.LoadedStorage(t, `
		load 1m
			test_metric{foo="bar"} 1
		`)
		defer store.Close()

		matcher, err := labels.NewMatcher(labels.MatchEqual, "__name__", "test_metric")
		require.NoError(t, err)
		query, err := ToQuery(0, 1, []*labels.Matcher{matcher}, nil)
		require.NoError(t, err)

		statusCode, resp := executeSampledRead(t, store, func() config.Config {
			return config.Config{}
		}, &prompb.ReadRequest{Queries: []*prompb.Query{query}})

		require.Equal(t, http.StatusOK, statusCode)
		require.Equal(t, &prompb.ReadResponse{
			Results: []*prompb.QueryResult{{
				Timeseries: []*prompb.TimeSeries{{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "test_metric"},
						{Name: "foo", Value: "bar"},
					},
					Samples: []prompb.Sample{{Value: 1}},
				}},
			}},
		}, resp)
	})
}

func BenchmarkSampledReadEndpointWithoutExternalLabels(b *testing.B) {
	for _, seriesCount := range []int{1, 10, 100} {
		for _, labelCount := range []int{3, 12, 24} {
			b.Run(fmt.Sprintf("series_%d_labels_%d", seriesCount, labelCount), func(b *testing.B) {
				store := teststorage.New(b)
				app := store.AppenderV2(b.Context())
				for i := range seriesCount {
					labelStrings := make([]string, 0, 2*labelCount)
					labelStrings = append(labelStrings, "__name__", "test_metric", "series", strconv.Itoa(i))
					for j := 2; j < labelCount; j++ {
						labelStrings = append(labelStrings, fmt.Sprintf("label_%d", j), "value")
					}
					_, err := app.Append(0, labels.FromStrings(labelStrings...), 0, 0, 1, nil, nil, storage.AOptions{})
					require.NoError(b, err)
				}
				require.NoError(b, app.Commit())

				api := NewReadHandler(nil, nil, store, func() config.Config {
					return config.Config{}
				}, 0, 1, 0)
				matcher, err := labels.NewMatcher(labels.MatchEqual, "__name__", "test_metric")
				require.NoError(b, err)
				query, err := ToQuery(0, 0, []*labels.Matcher{matcher}, nil)
				require.NoError(b, err)
				data, err := proto.Marshal(&prompb.ReadRequest{Queries: []*prompb.Query{query}})
				require.NoError(b, err)
				compressed := snappy.Encode(nil, data)
				request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(compressed))

				serve := func() *httptest.ResponseRecorder {
					request.Body = io.NopCloser(bytes.NewReader(compressed))
					recorder := httptest.NewRecorder()
					api.ServeHTTP(recorder, request)
					return recorder
				}

				recorder := serve()
				require.Equal(b, http.StatusOK, recorder.Code)
				uncompressed, err := snappy.Decode(nil, recorder.Body.Bytes())
				require.NoError(b, err)
				var resp prompb.ReadResponse
				require.NoError(b, proto.Unmarshal(uncompressed, &resp))
				require.Len(b, resp.Results, 1)
				require.Len(b, resp.Results[0].Timeseries, seriesCount)
				for _, ts := range resp.Results[0].Timeseries {
					require.Len(b, ts.Labels, labelCount)
				}

				b.ReportAllocs()
				for b.Loop() {
					recorder := serve()
					if recorder.Code != http.StatusOK {
						require.Equal(b, http.StatusOK, recorder.Code)
					}
				}
			})
		}
	}
}

func BenchmarkStreamReadEndpoint(b *testing.B) {
	bench := func(b *testing.B, store storage.SampleAndChunkQueryable, externalLabels labels.Labels, metric string, maxBytesInFrame, expectedFrames int) {
		api := NewReadHandler(nil, nil, store, func() config.Config {
			return config.Config{GlobalConfig: config.GlobalConfig{ExternalLabels: externalLabels}}
		},
			0, 1, maxBytesInFrame,
		)

		matcher, err := labels.NewMatcher(labels.MatchEqual, "__name__", metric)
		require.NoError(b, err)

		query, err := ToQuery(0, 14400001, []*labels.Matcher{matcher}, &storage.SelectHints{
			Step:  1,
			Func:  "sum",
			Start: 0,
			End:   14400001,
		})
		require.NoError(b, err)

		req := &prompb.ReadRequest{
			Queries:               []*prompb.Query{query},
			AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS},
		}
		data, err := proto.Marshal(req)
		require.NoError(b, err)

		b.ReportAllocs()

		for b.Loop() {
			compressed := snappy.Encode(nil, data)
			request, err := http.NewRequest(http.MethodPost, "", bytes.NewBuffer(compressed))
			require.NoError(b, err)

			recorder := httptest.NewRecorder()
			api.ServeHTTP(recorder, request)

			require.Equal(b, 2, recorder.Code/100)

			var results []*prompb.ChunkedReadResponse
			stream := NewChunkedReader(recorder.Result().Body, config.DefaultChunkedReadLimit, nil)

			for {
				res := &prompb.ChunkedReadResponse{}
				err := stream.NextProto(res)
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(b, err)
				results = append(results, res)
			}

			require.Len(b, results, expectedFrames)
		}
	}

	b.Run("test_data", func(b *testing.B) {
		store := promqltest.LoadedStorage(b, `
		load 1m
			test_metric1{foo="bar1",baz="qux"} 0+100x119
			test_metric1{foo="bar2",baz="qux"} 0+100x120
			test_metric1{foo="bar3",baz="qux"} 0+100x240
		`)
		b.Cleanup(func() { store.Close() })

		bench(b, store, labels.EmptyLabels(), "test_metric1", 0, 6)
	})

	for _, seriesCount := range []int{100, 1000} {
		for _, tc := range []struct {
			name           string
			externalLabels labels.Labels
			// Whether the series have a label that sorts after the external labels.
			lastLabelAfter bool
			// Whether series of another metric have the cluster label.
			otherMetricCluster bool
			// Whether every second series has its own cluster label.
			ownCluster bool
			// Whether to read from a block instead of the head.
			block bool
		}{
			{name: "external_labels_none"},
			{name: "external_labels_after_last_label", externalLabels: labels.FromStrings("cluster", "c1", "replica", "r1")},
			{name: "external_labels_before_last_label", externalLabels: labels.FromStrings("cluster", "c1", "replica", "r1"), lastLabelAfter: true},
			{name: "external_label_on_other_metric", externalLabels: labels.FromStrings("cluster", "c1", "replica", "r1"), otherMetricCluster: true},
			{name: "external_label_on_other_metric_block", externalLabels: labels.FromStrings("cluster", "c1", "replica", "r1"), otherMetricCluster: true, block: true},
			{name: "external_label_on_half", externalLabels: labels.FromStrings("cluster", "c1", "replica", "r1"), ownCluster: true},
			{name: "external_label_on_half_block", externalLabels: labels.FromStrings("cluster", "c1", "replica", "r1"), ownCluster: true, block: true},
		} {
			b.Run(fmt.Sprintf("series_%d/%s", seriesCount, tc.name), func(b *testing.B) {
				store := teststorage.New(b)
				app := store.AppenderV2(b.Context())
				appendSeries := func(lset labels.Labels) {
					for ts := range 240 {
						_, err := app.Append(0, lset, 0, int64(ts)*60000, float64(ts), nil, nil, storage.AOptions{})
						require.NoError(b, err)
					}
				}
				for i := range seriesCount {
					labelStrings := []string{"__name__", "test_metric", "instance", strconv.Itoa(i), "job", "test"}
					for j := range 8 {
						labelStrings = append(labelStrings, fmt.Sprintf("label_%d", j), "value")
					}
					if tc.lastLabelAfter {
						labelStrings = append(labelStrings, "zone", "z1")
					}
					if tc.ownCluster && i%2 == 0 {
						labelStrings = append(labelStrings, "cluster", "c2")
					}
					appendSeries(labels.FromStrings(labelStrings...))
					if tc.otherMetricCluster {
						appendSeries(labels.FromStrings("__name__", "other_metric", "cluster", "c2", "instance", strconv.Itoa(i)))
					}
				}
				require.NoError(b, app.Commit())
				if tc.block {
					head := store.Head()
					require.NoError(b, store.CompactHead(tsdb.NewRangeHead(head, head.MinTime(), head.MaxTime())))
					require.Len(b, store.Blocks(), 1)
				}

				bench(b, store, tc.externalLabels, "test_metric", 1<<20, seriesCount)
			})
		}
	}
}

func TestStreamReadEndpoint(t *testing.T) {
	t.Run("successful response", func(t *testing.T) {
		// First with 120 float samples. We expect 1 frame with 1 chunk.
		// Second with 121 float samples, We expect 1 frame with 2 chunks.
		// Third with 241 float samples. We expect 1 frame with 2 chunks, and 1 frame with 1 chunk for the same series due to bytes limit.
		// Fourth with 25 histogram samples. We expect 1 frame with 1 chunk.
		store := promqltest.LoadedStorage(t, `
		load 1m
			test_metric1{foo="bar1",baz="qux"} 0+100x119
			test_metric1{foo="bar2",baz="qux"} 0+100x120
			test_metric1{foo="bar3",baz="qux"} 0+100x240
	`)
		defer store.Close()

		addNativeHistogramsToTestSuite(t, store, 25)

		api := NewReadHandler(nil, nil, store, func() config.Config {
			return config.Config{
				GlobalConfig: config.GlobalConfig{
					// We expect external labels to be added, with the source labels honored.
					ExternalLabels: labels.FromStrings("baz", "a", "b", "c", "d", "e"),
				},
			}
		},
			1e6, 1,
			// Labelset has 57 bytes. Full chunk in test data has roughly 240 bytes. This allows us to have at max 2 chunks in this test.
			57+480,
		)

		// Encode the request.
		matcher1, err := labels.NewMatcher(labels.MatchEqual, "__name__", "test_metric1")
		require.NoError(t, err)

		matcher2, err := labels.NewMatcher(labels.MatchEqual, "d", "e")
		require.NoError(t, err)

		matcher3, err := labels.NewMatcher(labels.MatchEqual, "foo", "bar1")
		require.NoError(t, err)

		matcher4, err := labels.NewMatcher(labels.MatchEqual, "__name__", "test_histogram_metric1")
		require.NoError(t, err)

		query1, err := ToQuery(0, 14400001, []*labels.Matcher{matcher1, matcher2}, &storage.SelectHints{
			Step:  1,
			Func:  "avg",
			Start: 0,
			End:   14400001,
		})
		require.NoError(t, err)

		query2, err := ToQuery(0, 14400001, []*labels.Matcher{matcher1, matcher3}, &storage.SelectHints{
			Step:  1,
			Func:  "avg",
			Start: 0,
			End:   14400001,
		})
		require.NoError(t, err)

		query3, err := ToQuery(0, 14400001, []*labels.Matcher{matcher4}, &storage.SelectHints{
			Step:  1,
			Func:  "avg",
			Start: 0,
			End:   14400001,
		})
		require.NoError(t, err)

		req := &prompb.ReadRequest{
			Queries:               []*prompb.Query{query1, query2, query3},
			AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS},
		}
		data, err := proto.Marshal(req)
		require.NoError(t, err)

		compressed := snappy.Encode(nil, data)
		request, err := http.NewRequest(http.MethodPost, "", bytes.NewBuffer(compressed))
		require.NoError(t, err)

		recorder := httptest.NewRecorder()
		api.ServeHTTP(recorder, request)

		require.Equal(t, 2, recorder.Code/100)

		require.Equal(t, "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse", recorder.Result().Header.Get("Content-Type"))
		require.Empty(t, recorder.Result().Header.Get("Content-Encoding"))

		var results []*prompb.ChunkedReadResponse
		stream := NewChunkedReader(recorder.Result().Body, config.DefaultChunkedReadLimit, nil)
		for {
			res := &prompb.ChunkedReadResponse{}
			err := stream.NextProto(res)
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			results = append(results, res)
		}

		require.Len(t, results, 6, "Expected 6 results.")

		require.Equal(t, []*prompb.ChunkedReadResponse{
			{
				ChunkedSeries: []*prompb.ChunkedSeries{
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "test_metric1"},
							{Name: "b", Value: "c"},
							{Name: "baz", Value: "qux"},
							{Name: "d", Value: "e"},
							{Name: "foo", Value: "bar1"},
						},
						Chunks: []prompb.Chunk{
							{
								Type:      prompb.Chunk_XOR,
								MaxTimeMs: 7140000,
								Data:      []byte("\000x\000\000\000\000\000\000\000\000\000\340\324\003\302|\005\224\000\301\254}\351z2\320O\355\264n[\007\316\224\243md\371\320\375\032Pm\nS\235\016Q\255\006P\275\250\277\312\201Z\003(3\240R\207\332\005(\017\240\322\201\332=(\023\2402\203Z\007(w\2402\201Z\017(\023\265\227\364P\033@\245\007\364\nP\033C\245\002t\036P+@e\036\364\016Pk@e\002t:P;A\245\001\364\nS\373@\245\006t\006P+C\345\002\364\006Pk@\345\036t\nP\033A\245\003\364:P\033@\245\006t\016ZJ\377\\\205\313\210\327\270\017\345+F[\310\347E)\355\024\241\366\342}(v\215(N\203)\326\207(\336\203(V\332W\362\202t4\240m\005(\377AJ\006\320\322\202t\374\240\255\003(oA\312:\3202"),
							},
						},
					},
				},
			},
			{
				ChunkedSeries: []*prompb.ChunkedSeries{
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "test_metric1"},
							{Name: "b", Value: "c"},
							{Name: "baz", Value: "qux"},
							{Name: "d", Value: "e"},
							{Name: "foo", Value: "bar2"},
						},
						Chunks: []prompb.Chunk{
							{
								Type:      prompb.Chunk_XOR,
								MaxTimeMs: 7140000,
								Data:      []byte("\000x\000\000\000\000\000\000\000\000\000\340\324\003\302|\005\224\000\301\254}\351z2\320O\355\264n[\007\316\224\243md\371\320\375\032Pm\nS\235\016Q\255\006P\275\250\277\312\201Z\003(3\240R\207\332\005(\017\240\322\201\332=(\023\2402\203Z\007(w\2402\201Z\017(\023\265\227\364P\033@\245\007\364\nP\033C\245\002t\036P+@e\036\364\016Pk@e\002t:P;A\245\001\364\nS\373@\245\006t\006P+C\345\002\364\006Pk@\345\036t\nP\033A\245\003\364:P\033@\245\006t\016ZJ\377\\\205\313\210\327\270\017\345+F[\310\347E)\355\024\241\366\342}(v\215(N\203)\326\207(\336\203(V\332W\362\202t4\240m\005(\377AJ\006\320\322\202t\374\240\255\003(oA\312:\3202"),
							},
							{
								Type:      prompb.Chunk_XOR,
								MinTimeMs: 7200000,
								MaxTimeMs: 7200000,
								Data:      []byte("\000\001\200\364\356\006@\307p\000\000\000\000\000"),
							},
						},
					},
				},
			},
			{
				ChunkedSeries: []*prompb.ChunkedSeries{
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "test_metric1"},
							{Name: "b", Value: "c"},
							{Name: "baz", Value: "qux"},
							{Name: "d", Value: "e"},
							{Name: "foo", Value: "bar3"},
						},
						Chunks: []prompb.Chunk{
							{
								Type:      prompb.Chunk_XOR,
								MaxTimeMs: 7140000,
								Data:      []byte("\000x\000\000\000\000\000\000\000\000\000\340\324\003\302|\005\224\000\301\254}\351z2\320O\355\264n[\007\316\224\243md\371\320\375\032Pm\nS\235\016Q\255\006P\275\250\277\312\201Z\003(3\240R\207\332\005(\017\240\322\201\332=(\023\2402\203Z\007(w\2402\201Z\017(\023\265\227\364P\033@\245\007\364\nP\033C\245\002t\036P+@e\036\364\016Pk@e\002t:P;A\245\001\364\nS\373@\245\006t\006P+C\345\002\364\006Pk@\345\036t\nP\033A\245\003\364:P\033@\245\006t\016ZJ\377\\\205\313\210\327\270\017\345+F[\310\347E)\355\024\241\366\342}(v\215(N\203)\326\207(\336\203(V\332W\362\202t4\240m\005(\377AJ\006\320\322\202t\374\240\255\003(oA\312:\3202"),
							},
							{
								Type:      prompb.Chunk_XOR,
								MinTimeMs: 7200000,
								MaxTimeMs: 14340000,
								Data:      []byte("\000x\200\364\356\006@\307p\000\000\000\000\000\340\324\003\340>\224\355\260\277\322\200\372\005(=\240R\207:\003(\025\240\362\201z\003(\365\240r\203:\005(\r\241\322\201\372\r(\r\240R\237:\007(5\2402\201z\037(\025\2402\203:\005(\375\240R\200\372\r(\035\241\322\201:\003(5\240r\326g\364\271\213\227!\253q\037\312N\340GJ\033E)\375\024\241\266\362}(N\217(V\203)\336\207(\326\203(N\334W\322\203\2644\240}\005(\373AJ\031\3202\202\264\374\240\275\003(kA\3129\320R\201\2644\240\375\264\277\322\200\332\005(3\240r\207Z\003(\027\240\362\201Z\003(\363\240R\203\332\005(\017\241\322\201\332\r(\023\2402\237Z\007(7\2402\201Z\037(\023\240\322\200\332\005(\377\240R\200\332\r "),
							},
						},
					},
				},
			},
			{
				ChunkedSeries: []*prompb.ChunkedSeries{
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "test_metric1"},
							{Name: "b", Value: "c"},
							{Name: "baz", Value: "qux"},
							{Name: "d", Value: "e"},
							{Name: "foo", Value: "bar3"},
						},
						Chunks: []prompb.Chunk{
							{
								Type:      prompb.Chunk_XOR,
								MinTimeMs: 14400000,
								MaxTimeMs: 14400000,
								Data:      []byte("\000\001\200\350\335\r@\327p\000\000\000\000\000"),
							},
						},
					},
				},
			},
			{
				ChunkedSeries: []*prompb.ChunkedSeries{
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "test_metric1"},
							{Name: "b", Value: "c"},
							{Name: "baz", Value: "qux"},
							{Name: "d", Value: "e"},
							{Name: "foo", Value: "bar1"},
						},
						Chunks: []prompb.Chunk{
							{
								Type:      prompb.Chunk_XOR,
								MaxTimeMs: 7140000,
								Data:      []byte("\000x\000\000\000\000\000\000\000\000\000\340\324\003\302|\005\224\000\301\254}\351z2\320O\355\264n[\007\316\224\243md\371\320\375\032Pm\nS\235\016Q\255\006P\275\250\277\312\201Z\003(3\240R\207\332\005(\017\240\322\201\332=(\023\2402\203Z\007(w\2402\201Z\017(\023\265\227\364P\033@\245\007\364\nP\033C\245\002t\036P+@e\036\364\016Pk@e\002t:P;A\245\001\364\nS\373@\245\006t\006P+C\345\002\364\006Pk@\345\036t\nP\033A\245\003\364:P\033@\245\006t\016ZJ\377\\\205\313\210\327\270\017\345+F[\310\347E)\355\024\241\366\342}(v\215(N\203)\326\207(\336\203(V\332W\362\202t4\240m\005(\377AJ\006\320\322\202t\374\240\255\003(oA\312:\3202"),
							},
						},
					},
				},
				QueryIndex: 1,
			},
			{
				ChunkedSeries: []*prompb.ChunkedSeries{
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "test_histogram_metric1"},
							{Name: "b", Value: "c"},
							{Name: "baz", Value: "qux"},
							{Name: "d", Value: "e"},
						},
						Chunks: []prompb.Chunk{
							{
								Type:      prompb.Chunk_FLOAT_HISTOGRAM,
								MaxTimeMs: 1440000,
								Data:      []byte("\x00\x19\x00\xff?PbM\xd2\xf1\xa9\xfc\x8c\xa4\x94e$\xa2@(\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@2ffffff?\xf0\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00?\xf0\x00\x00\x00\x00\x00\x00?\xf0\x00\x00\x00\x00\x00\x00?\xf0\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00?\xf0\x00\x00\x00\x00\x00\x00?\xf0\x00\x00\x00\x00\x00\x00\xf8\xea`\xd6/v\x03\xd2\x1f\xc2_\xff\xd8\x0f\t\x7f\xff\t\x7f\xff\t\x7f\xff`<%\xff\xfc%\xff\xf4\xbda{4\x9f\xff\xff\xff\xff\xff\xfd\x80\xf5\x85\xec\a\xb0\x1e\xc0z\xc2\xf6\x03\xd8\r\xa4\x8f\xbd\xa0\xf5\xeb\x9f\xff\xff\xff\xff\xff\xfda{A\xeb\v\xd6\x17\xac/h=az\xc2о\xc0\xb8\xac\xcc\xcc\xcc\xcc\xcc\xdbA\xec\v\xda\x0fh=\xa0\xf6\x05\xed\a\xb4\x1a\t\x99\x9333333;\x02\xe7`^\xc0\xbd\x81s\xb0/`Z8\xd4'\xea\x1f\xbc\xea\x13\xe6gP\x9f2\xdc$\xee\a\xbb)\xff\xff\xff\xff\xff\xffP\x9f\xb8\x1e\xa1?P\x9f\xa8O\xdc\x0fP\x9f\xa8Om\x17\xfbB\xf6\xe7\xb5UUUUUw\x03\xda\x17\xb8\x1e\xe0{\x81\xed\v\xdc\x0fp4\x99\x9d\x99\x99\x99\x99\x99\x9eй\xda\x17\xb4/h\\\xed\v\xda\x16\xd87{\x03\xfb2\xe4\xcc\xcc\xcc\xcc\xcc\xe7`~fv\a\xe6Q1ݕ\xaa\xaa\xaa\xaa\xaa\xab\xb0?\x1d\x81\xfd\x81\xfd\x81\xf8\xec\x0f\xec\x0f\xa5\xe7\xb7<\xff\xff\xff\xff\xff\xff\x19\xc61\x9cb\xd4O\xc6:\xf5\xef\xff\xff\xff\xff\xff\xfc\xe39\xce3\x9a\x05\xeb\x13\xe0\xac\xcc\xcc\xcc\xcc\xcc\xc7X\x9f\x18\xc7X\x9f\x18\xa0\xce\xf0pə\x99\x99\x99\x99\xb5\x89\xfb\xc1\xeb\x13\xf5\x89\xfa\xc4\xfd\xe0\xf5\x89\xfa\xc4\xf4\x0f\xdc\x17\a\xaa\xaa\xaa\xaa\xaa\xabx=\xc1{\xc1\xef\a\xbc\x1e\xe0\xbd\xe0\xf7\x83C\x99\x8e\x7f\xff\xff\xff\xff\xff\xb8.w\x05\xee\v\xdc\x17;\x82\xf7\x05\xa0^\xd0\xfc\x16\xaa\xaa\xaa\xaa\xaa\xa9\xda\x1f\x99\x9d\xa1\xf9\x94\x19\x8c-\x99\x99\x99\x99\x99\x9d\xa1\xf8\xed\x0f\xed\x0f\xed\x0f\xc7h\x7fh}\x1d\xe7<\x99\x99\x99\x99\x99\x9a3\x8cc8\xc5\x02c\x05\xaa\xaa\xaa\xaa\xaa\xaaq\x9c\xe7\x19\xcd\x06\xf6\t\xf0\xcf\xff\xff\xff\xff\xff\xe3\xb0O\x8cc\xb0O\x8cP&\x18=UUUUU[\x04\xf8v\t\xfb\x04\xfd\x82|;\x04\xfd\x82z\x1f\xc7\x1c\x99\x99\x99\x99\x99\x9a\x18\xe1\x86\x18\xe1\x84"),
							},
						},
					},
				},
				QueryIndex: 2,
			},
		}, results)
	})

	t.Run("sorted with external labels", func(t *testing.T) {
		// Adding the same external labels to every series keeps their order, except
		// when the labels of a series are a prefix of the labels of a later series.
		store := promqltest.LoadedStorage(t, `
		load 1m
			prefix{job="a"} 1
			prefix{job="a", path="/x"} 2
			prefix{job="a", path="/y"} 3
			prefix{job="b"} 4
			nested{job="a"} 1
			nested{job="a", path="/x"} 2
			nested{job="a", path="/x", method="GET"} 3
			nested{job="a", path="/x", zz="1"} 4
			nested{job="a", zz="1"} 5
			unaffected{job="a"} 1
			unaffected{job="a", zone="z1"} 2
			unaffected{job="b"} 3
	`)
		t.Cleanup(func() { require.NoError(t, store.Close()) })

		type series struct {
			Labels labels.Labels
			Value  float64
		}
		for _, tc := range []struct {
			name           string
			metric         string
			externalLabels labels.Labels
			expected       []series
		}{
			{
				name:           "prefix",
				metric:         "prefix",
				externalLabels: labels.FromStrings("region", "foo"),
				expected: []series{
					{labels.FromStrings("__name__", "prefix", "job", "a", "path", "/x", "region", "foo"), 2},
					{labels.FromStrings("__name__", "prefix", "job", "a", "path", "/y", "region", "foo"), 3},
					{labels.FromStrings("__name__", "prefix", "job", "a", "region", "foo"), 1},
					{labels.FromStrings("__name__", "prefix", "job", "b", "region", "foo"), 4},
				},
			},
			{
				name:           "nested prefixes",
				metric:         "nested",
				externalLabels: labels.FromStrings("cluster", "c1", "replica", "r1"),
				expected: []series{
					{labels.FromStrings("__name__", "nested", "cluster", "c1", "job", "a", "method", "GET", "path", "/x", "replica", "r1"), 3},
					{labels.FromStrings("__name__", "nested", "cluster", "c1", "job", "a", "path", "/x", "replica", "r1"), 2},
					{labels.FromStrings("__name__", "nested", "cluster", "c1", "job", "a", "path", "/x", "replica", "r1", "zz", "1"), 4},
					{labels.FromStrings("__name__", "nested", "cluster", "c1", "job", "a", "replica", "r1"), 1},
					{labels.FromStrings("__name__", "nested", "cluster", "c1", "job", "a", "replica", "r1", "zz", "1"), 5},
				},
			},
			{
				name:           "prefix with a label after the external labels",
				metric:         "unaffected",
				externalLabels: labels.FromStrings("region", "foo"),
				expected: []series{
					{labels.FromStrings("__name__", "unaffected", "job", "a", "region", "foo"), 1},
					{labels.FromStrings("__name__", "unaffected", "job", "a", "region", "foo", "zone", "z1"), 2},
					{labels.FromStrings("__name__", "unaffected", "job", "b", "region", "foo"), 3},
				},
			},
			{
				name:   "no external labels",
				metric: "prefix",
				expected: []series{
					{labels.FromStrings("__name__", "prefix", "job", "a"), 1},
					{labels.FromStrings("__name__", "prefix", "job", "a", "path", "/x"), 2},
					{labels.FromStrings("__name__", "prefix", "job", "a", "path", "/y"), 3},
					{labels.FromStrings("__name__", "prefix", "job", "b"), 4},
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				handler := NewReadHandler(nil, nil, store, func() config.Config {
					return config.Config{GlobalConfig: config.GlobalConfig{ExternalLabels: tc.externalLabels}}
				}, 1e6, 1, 1<<20)
				data, err := proto.Marshal(&prompb.ReadRequest{
					Queries: []*prompb.Query{{
						StartTimestampMs: 0, EndTimestampMs: 1,
						Matchers: []*prompb.LabelMatcher{{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: tc.metric}},
					}},
					AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS},
				})
				require.NoError(t, err)
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/read", bytes.NewReader(snappy.Encode(nil, data))))
				require.Equal(t, http.StatusOK, recorder.Code)

				body := recorder.Result().Body
				ss := NewChunkedSeriesSet(NewChunkedReader(body, config.DefaultChunkedReadLimit, nil), body, 0, 1, func(error) {})
				var got []series
				for ss.Next() {
					iter := ss.At().Iterator(nil)
					require.Equal(t, chunkenc.ValFloat, iter.Next())
					_, value := iter.At()
					got = append(got, series{ss.At().Labels(), value})
				}
				require.NoError(t, ss.Err())
				for i := 1; i < len(got); i++ {
					require.Negative(t, labels.Compare(got[i-1].Labels, got[i].Labels), "series %d is not sorted after series %d", i, i-1)
				}
				testutil.RequireEqual(t, tc.expected, got)
			})
		}
	})

	t.Run("storage errors", func(t *testing.T) {
		store := promqltest.LoadedStorage(t, "load 1m\n test_metric{series=\"a\"} 1\n test_metric{series=\"b\"} 2\n")
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		storageErr := errors.New("injected storage error")

		for _, protocol := range []int{1, 2} {
			for _, instrumented := range []bool{false, true} {
				for _, tc := range []struct {
					name       string
					failQuery  int64
					failAfter  int
					err        error
					flush      bool
					empty      bool
					wantStatus int
					wantSeries int
				}{
					{name: "success", wantStatus: http.StatusOK, wantSeries: 2},
					{name: "empty", empty: true, wantStatus: http.StatusOK},
					{name: "error before response", failQuery: 1, err: storageErr, wantStatus: http.StatusInternalServerError},
					{name: "HTTP error before response", failQuery: 1, err: HTTPError{msg: "injected HTTP error", status: http.StatusBadRequest}, wantStatus: http.StatusBadRequest},
					{name: "buffered error", failQuery: 1, failAfter: 1, err: storageErr, wantStatus: http.StatusOK, wantSeries: 1},
					{name: "flushed error", failQuery: 1, failAfter: 1, err: storageErr, flush: true, wantStatus: http.StatusOK, wantSeries: 1},
					{name: "later query error", failQuery: 2, err: HTTPError{msg: "injected HTTP error", status: http.StatusBadRequest}, flush: true, wantStatus: http.StatusOK, wantSeries: 2},
				} {
					t.Run(fmt.Sprintf("HTTP%d/otel=%t/%s", protocol, instrumented, tc.name), func(t *testing.T) {
						ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
						defer cancel()
						allowError := make(chan struct{})
						releaseError := sync.OnceFunc(func() { close(allowError) })
						queryable := &failingChunkQueryable{
							SampleAndChunkQueryable: store,
							failQuery:               tc.failQuery,
							failAfter:               tc.failAfter,
							err:                     tc.err,
						}
						if tc.flush {
							queryable.beforeError = func(ctx context.Context) {
								select {
								case <-allowError:
								case <-ctx.Done():
								}
							}
						}

						var handlerLog, serverLog bytes.Buffer
						registry := prometheus.NewRegistry()
						handler := NewReadHandler(slog.New(slog.NewTextHandler(&handlerLog, nil)), registry, queryable, func() config.Config { return config.Config{} }, 1e6, 1, 1<<20)
						if instrumented {
							handler = otelhttp.NewHandler(handler, "remote_read")
						}
						// A real server exposes duplicate headers and incomplete HTTP responses.
						done := make(chan int, 2)
						server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							defer func() { done <- r.ProtoMajor }()
							if tc.flush {
								w = flushingReadResponseWriter{ResponseWriter: w}
							}
							handler.ServeHTTP(w, r)
						}))
						server.Config.ErrorLog = log.New(&serverLog, "", 0)
						server.EnableHTTP2 = protocol == 2
						server.StartTLS()
						t.Cleanup(func() {
							releaseError()
							cancel()
							server.Close()
						})
						client := server.Client()

						metric := "test_metric"
						if tc.empty {
							metric = "missing_metric"
						}
						query := &prompb.Query{
							StartTimestampMs: 0, EndTimestampMs: 1,
							Matchers: []*prompb.LabelMatcher{{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: metric}},
						}
						queries := []*prompb.Query{query}
						if tc.failQuery == 2 {
							queries = append(queries, query)
						}
						data, err := proto.Marshal(&prompb.ReadRequest{
							Queries:               queries,
							AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS},
						})
						require.NoError(t, err)
						newRequest := func() *http.Request {
							request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/api/v1/read", bytes.NewReader(snappy.Encode(nil, data)))
							require.NoError(t, err)
							request.Header.Set("Content-Type", "application/x-protobuf")
							request.Header.Set("Content-Encoding", "snappy")
							return request
						}
						checkCleanup := func() {
							t.Helper()
							select {
							case gotProtocol := <-done:
								require.Equal(t, protocol, gotProtocol)
							case <-ctx.Done():
								t.Fatal("handler did not finish:", ctx.Err())
							}
							require.Equal(t, queryable.opened.Load(), queryable.closed.Load())
							require.NoError(t, prom_testutil.GatherAndCompare(registry, strings.NewReader(
								"# HELP prometheus_remote_read_handler_queries The current number of remote read queries that are either in execution or queued on the handler.\n"+
									"# TYPE prometheus_remote_read_handler_queries gauge\n"+
									"prometheus_remote_read_handler_queries 0\n",
							), "prometheus_remote_read_handler_queries"))
						}

						response, err := client.Do(newRequest())
						aborted := tc.err != nil && tc.wantStatus == http.StatusOK
						if err != nil {
							// A buffered abort may close the connection before even the headers arrive.
							require.True(t, aborted && !tc.flush, "unexpected request failure: %v", err)
							require.NotErrorIs(t, err, context.DeadlineExceeded)
							require.NotErrorIs(t, err, context.Canceled)
						} else {
							defer response.Body.Close()
							require.Equal(t, tc.wantStatus, response.StatusCode)
							require.Equal(t, protocol, response.ProtoMajor)
							if tc.wantStatus != http.StatusOK {
								body, err := io.ReadAll(response.Body)
								require.NoError(t, err)
								require.Equal(t, "text/plain; charset=utf-8", response.Header.Get("Content-Type"))
								require.Equal(t, tc.err.Error()+"\n", string(body))
							} else {
								require.Equal(t, "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse", response.Header.Get("Content-Type"))
								body := &recordingReadBody{ReadCloser: response.Body}
								series := NewChunkedSeriesSet(NewChunkedReader(body, config.DefaultChunkedReadLimit, nil), body, 0, 1, func(error) {})
								count := 0
								for series.Next() {
									require.Less(t, count, tc.wantSeries)
									require.Equal(t, labels.FromStrings("__name__", "test_metric", "series", string(rune('a'+count))), series.At().Labels())
									iter := series.At().Iterator(nil)
									require.Equal(t, chunkenc.ValFloat, iter.Next())
									ts, value := iter.At()
									require.Equal(t, int64(0), ts)
									require.Equal(t, float64(count+1), value)
									require.Equal(t, chunkenc.ValNone, iter.Next())
									require.NoError(t, iter.Err())
									count++
									if tc.flush && count == tc.wantSeries {
										// Let storage fail only once the client has consumed the preceding frames.
										releaseError()
									}
								}
								if aborted {
									require.Error(t, series.Err())
									// A decoding error also occurs with the old plaintext suffix.
									// Require the HTTP body itself to be interrupted.
									require.Error(t, body.err)
									require.NotErrorIs(t, body.err, io.EOF)
									require.NotErrorIs(t, body.err, context.DeadlineExceeded)
									require.NotErrorIs(t, body.err, context.Canceled)
									require.NotContains(t, body.bytes.String(), tc.err.Error())
								} else {
									require.NoError(t, series.Err())
									require.ErrorIs(t, body.err, io.EOF)
								}
								if !aborted || tc.flush {
									require.Equal(t, tc.wantSeries, count)
								}
							}
							require.NoError(t, response.Body.Close())
						}
						require.NoError(t, ctx.Err())
						checkCleanup()

						// With a concurrency limit of one, another successful request proves the gate was released.
						response, err = client.Do(newRequest())
						require.NoError(t, err)
						defer response.Body.Close()
						require.Equal(t, http.StatusOK, response.StatusCode)
						_, err = io.Copy(io.Discard, response.Body)
						require.NoError(t, err)
						require.NoError(t, response.Body.Close())
						checkCleanup()
						server.Close()
						require.Empty(t, serverLog.String())
						if aborted {
							require.Equal(t, 1, strings.Count(handlerLog.String(), "level=ERROR"))
							require.Contains(t, handlerLog.String(), tc.err.Error())
						} else {
							require.Empty(t, handlerLog.String())
						}
					})
				}
			}
		}
	})

	t.Run("write errors", func(t *testing.T) {
		store := promqltest.LoadedStorage(t, "load 1m\n test_metric 1\n")
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		data, err := proto.Marshal(&prompb.ReadRequest{
			Queries: []*prompb.Query{{
				StartTimestampMs: 0, EndTimestampMs: 1,
				Matchers: []*prompb.LabelMatcher{{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "test_metric"}},
			}},
			AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS},
		})
		require.NoError(t, err)
		for _, tc := range []struct {
			name      string
			failWrite int
			partial   bool
		}{
			{name: "zero bytes", failWrite: 1},
			{name: "partial payload", failWrite: 3, partial: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				handler := NewReadHandler(nil, nil, store, func() config.Config { return config.Config{} }, 1e6, 1, 1<<20)
				writer := &failingReadResponseWriter{
					header: make(http.Header), failWrite: tc.failWrite, partial: tc.partial,
				}
				request := httptest.NewRequest(http.MethodPost, "/api/v1/read", bytes.NewReader(snappy.Encode(nil, data)))
				require.PanicsWithValue(t, http.ErrAbortHandler, func() { handler.ServeHTTP(writer, request) })
				require.Equal(t, http.StatusOK, writer.status)
				require.Equal(t, 1, writer.headers)
				require.Equal(t, tc.failWrite, writer.writes)
				require.Zero(t, writer.flushes)
			})
		}
	})
}

// Test for https://github.com/prometheus/prometheus/issues/12605: series that
// already have a label named like an external label keep their own value, series
// that end up with the same labels are returned as one, and a streamed response
// stays sorted.
func TestRemoteReadSortedResult(t *testing.T) {
	issueData := `
		load 30s
			aaa{instance="bbb", job="etcd", service="etcd", toposphere="test1"} 1 _ 1
			aaa{instance="bbb", job="etcd", region="bar", service="etcd", toposphere="test1"} 2
			aaa{instance="bbb", job="etcd", region="bar", service="etcd", toposphere="test2"} 3
			aaa{instance="bbb", job="etcd", region="foo", service="etcd", toposphere="test1"} _ 4
			aaa{instance="bbb", job="etcd", region="foo", service="etcd", toposphere="test2"} 5
	`
	type series struct {
		Labels  labels.Labels
		Samples []prompb.Sample
	}
	for _, tc := range []struct {
		name string
		load string
		// Series of a remote read source behind the fanout storage, if any.
		remoteLoad     string
		externalLabels labels.Labels
		matchers       []*labels.Matcher
		expected       []series
		// Whether some of the selected series have a label named like an
		// external label and others do not.
		expectedConflict bool
		// The matchers the remote read source has to be queried with.
		expectedRemoteMatchers []*prompb.LabelMatcher
	}{
		{
			name: "stored value differs",
			load: `
			load 30s
				test_metric1{a="1", b="2", c="3"} 1
				test_metric1{a="1", b="2", c="3", d="4"} 2
				test_metric1{a="1", c="5"} 3
				test_metric1{a="1", c="6"} 4
			`,
			externalLabels: labels.FromStrings("b", "1"),
			matchers:       []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "a", "1")},
			expected: []series{
				{labels.FromStrings("__name__", "test_metric1", "a", "1", "b", "1", "c", "5"), []prompb.Sample{{Value: 3}}},
				{labels.FromStrings("__name__", "test_metric1", "a", "1", "b", "1", "c", "6"), []prompb.Sample{{Value: 4}}},
				{labels.FromStrings("__name__", "test_metric1", "a", "1", "b", "2", "c", "3"), []prompb.Sample{{Value: 1}}},
				{labels.FromStrings("__name__", "test_metric1", "a", "1", "b", "2", "c", "3", "d", "4"), []prompb.Sample{{Value: 2}}},
			},
			expectedConflict: true,
		},
		{
			name:           "stored value differs or equals",
			load:           issueData,
			externalLabels: labels.FromStrings("region", "foo"),
			matchers:       []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "aaa")},
			expected: []series{
				{labels.FromStrings("__name__", "aaa", "instance", "bbb", "job", "etcd", "region", "bar", "service", "etcd", "toposphere", "test1"), []prompb.Sample{{Value: 2}}},
				{labels.FromStrings("__name__", "aaa", "instance", "bbb", "job", "etcd", "region", "bar", "service", "etcd", "toposphere", "test2"), []prompb.Sample{{Value: 3}}},
				{labels.FromStrings("__name__", "aaa", "instance", "bbb", "job", "etcd", "region", "foo", "service", "etcd", "toposphere", "test1"), []prompb.Sample{{Value: 1}, {Value: 4, Timestamp: 30000}, {Value: 1, Timestamp: 60000}}},
				{labels.FromStrings("__name__", "aaa", "instance", "bbb", "job", "etcd", "region", "foo", "service", "etcd", "toposphere", "test2"), []prompb.Sample{{Value: 5}}},
			},
			expectedConflict: true,
		},
		{
			name: "stored value equals",
			load: `
			load 30s
				aaa{job="etcd", toposphere="test0"} 1
				aaa{job="etcd", toposphere="test1"} 2
				aaa{job="etcd", region="foo", toposphere="test1"} 2
				aaa{job="etcd", region="foo", toposphere="test2"} 3
			`,
			externalLabels: labels.FromStrings("region", "foo"),
			matchers:       []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "aaa")},
			expected: []series{
				{labels.FromStrings("__name__", "aaa", "job", "etcd", "region", "foo", "toposphere", "test0"), []prompb.Sample{{Value: 1}}},
				{labels.FromStrings("__name__", "aaa", "job", "etcd", "region", "foo", "toposphere", "test1"), []prompb.Sample{{Value: 2}}},
				{labels.FromStrings("__name__", "aaa", "job", "etcd", "region", "foo", "toposphere", "test2"), []prompb.Sample{{Value: 3}}},
			},
			expectedConflict: true,
		},
		{
			name: "two external labels",
			load: `
			load 30s
				ddd{job="a"} 1
				ddd{cluster="c1", job="a", replica="r1"} _ 2
				ddd{cluster="c2", job="a", replica="r1"} 3
				ddd{job="b", replica="r2"} 4
			`,
			externalLabels: labels.FromStrings("cluster", "c1", "replica", "r1"),
			matchers:       []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "ddd")},
			expected: []series{
				{labels.FromStrings("__name__", "ddd", "cluster", "c1", "job", "a", "replica", "r1"), []prompb.Sample{{Value: 1}, {Value: 2, Timestamp: 30000}}},
				{labels.FromStrings("__name__", "ddd", "cluster", "c1", "job", "b", "replica", "r2"), []prompb.Sample{{Value: 4}}},
				{labels.FromStrings("__name__", "ddd", "cluster", "c2", "job", "a", "replica", "r1"), []prompb.Sample{{Value: 3}}},
			},
			expectedConflict: true,
		},
		{
			name:           "every selected series has the label",
			load:           issueData,
			externalLabels: labels.FromStrings("region", "foo"),
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "aaa"),
				labels.MustNewMatcher(labels.MatchEqual, "region", "bar"),
			},
			expected: []series{
				{labels.FromStrings("__name__", "aaa", "instance", "bbb", "job", "etcd", "region", "bar", "service", "etcd", "toposphere", "test1"), []prompb.Sample{{Value: 2}}},
				{labels.FromStrings("__name__", "aaa", "instance", "bbb", "job", "etcd", "region", "bar", "service", "etcd", "toposphere", "test2"), []prompb.Sample{{Value: 3}}},
			},
		},
		{
			name: "every selected series has the label without a matcher for it",
			load: `
			load 30s
				ccc{job="a", region="bar"} 1
				ccc{job="b", region="foo"} 2
			`,
			externalLabels: labels.FromStrings("region", "foo"),
			matchers:       []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "ccc")},
			expected: []series{
				{labels.FromStrings("__name__", "ccc", "job", "a", "region", "bar"), []prompb.Sample{{Value: 1}}},
				{labels.FromStrings("__name__", "ccc", "job", "b", "region", "foo"), []prompb.Sample{{Value: 2}}},
			},
		},
		{
			name: "remote read source",
			load: `
			load 30s
				x{job="a"} 1
				x{job="a", region="bar"} 2
			`,
			remoteLoad: `
			load 30s
				x{job="r1", region="foo"} 3
				x{job="r2", region="other"} 4
				x{job="r3"} 5
			`,
			externalLabels: labels.FromStrings("region", "foo"),
			matchers:       []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "x")},
			expected: []series{
				{labels.FromStrings("__name__", "x", "job", "a", "region", "bar"), []prompb.Sample{{Value: 2}}},
				{labels.FromStrings("__name__", "x", "job", "a", "region", "foo"), []prompb.Sample{{Value: 1}}},
				{labels.FromStrings("__name__", "x", "job", "r1", "region", "foo"), []prompb.Sample{{Value: 3}}},
			},
			expectedConflict: true,
			expectedRemoteMatchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "x"},
				{Type: prompb.LabelMatcher_EQ, Name: "region", Value: "foo"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := promqltest.LoadedStorage(t, tc.load)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			var (
				queryable storage.SampleAndChunkQueryable = store
				client    *recordingReadClient
			)
			if tc.remoteLoad != "" {
				remoteStore := promqltest.LoadedStorage(t, tc.remoteLoad)
				t.Cleanup(func() { require.NoError(t, remoteStore.Close()) })
				client = &recordingReadClient{queryable: remoteStore}
				queryable = fanoutQueryable{
					local: store,
					// Like remote_read with read_recent: true and the default
					// filter_external_labels: true.
					remote: NewSampleAndChunkQueryableClient(client, tc.externalLabels, nil, true, func() (int64, error) { return 0, nil }),
				}
			}
			registry := prometheus.NewRegistry()
			handler := NewReadHandler(nil, registry, queryable, func() config.Config {
				return config.Config{GlobalConfig: config.GlobalConfig{ExternalLabels: tc.externalLabels}}
			}, 1e6, 1, 1<<20)
			query, err := ToQuery(0, 60000, tc.matchers, nil)
			require.NoError(t, err)

			for _, responseType := range []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS, prompb.ReadRequest_SAMPLES} {
				data, err := proto.Marshal(&prompb.ReadRequest{
					Queries:               []*prompb.Query{query},
					AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{responseType},
				})
				require.NoError(t, err)
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/read", bytes.NewReader(snappy.Encode(nil, data))))
				require.Equal(t, http.StatusOK, recorder.Code)

				var got []series
				if responseType == prompb.ReadRequest_STREAMED_XOR_CHUNKS {
					body := recorder.Result().Body
					ss := NewChunkedSeriesSet(NewChunkedReader(body, config.DefaultChunkedReadLimit, nil), body, 0, 60000, func(error) {})
					for ss.Next() {
						var samples []prompb.Sample
						iter := ss.At().Iterator(nil)
						for iter.Next() == chunkenc.ValFloat {
							ts, value := iter.At()
							samples = append(samples, prompb.Sample{Value: value, Timestamp: ts})
						}
						require.NoError(t, iter.Err())
						got = append(got, series{ss.At().Labels(), samples})
					}
					require.NoError(t, ss.Err())
					for i := 1; i < len(got); i++ {
						require.Negative(t, labels.Compare(got[i-1].Labels, got[i].Labels), "series %d is not sorted after series %d", i, i-1)
					}
				} else {
					uncompressed, err := snappy.Decode(nil, recorder.Body.Bytes())
					require.NoError(t, err)
					var resp prompb.ReadResponse
					require.NoError(t, proto.Unmarshal(uncompressed, &resp))
					require.Len(t, resp.Results, 1)
					builder := labels.NewScratchBuilder(0)
					for _, ts := range resp.Results[0].Timeseries {
						got = append(got, series{ts.ToLabels(&builder, nil), ts.Samples})
					}
					// A samples response is not sorted.
					slices.SortFunc(got, func(a, b series) int { return labels.Compare(a.Labels, b.Labels) })
				}
				testutil.RequireEqual(t, tc.expected, got, "response type %s", responseType)
			}
			conflicts := 0
			if tc.expectedConflict {
				conflicts = 2
			}
			require.NoError(t, prom_testutil.GatherAndCompare(registry, strings.NewReader(fmt.Sprintf(
				"# HELP prometheus_remote_read_handler_external_label_conflicts_total The number of remote read queries that selected series with and without a label named like an external label.\n"+
					"# TYPE prometheus_remote_read_handler_external_label_conflicts_total counter\n"+
					"prometheus_remote_read_handler_external_label_conflicts_total %d\n", conflicts,
			)), "prometheus_remote_read_handler_external_label_conflicts_total"))
			if client != nil {
				require.NotEmpty(t, client.queries)
				for _, q := range client.queries {
					require.Equal(t, tc.expectedRemoteMatchers, q.Matchers)
				}
			}
		})
	}
}

// recordingReadClient is a ReadClient that answers from a queryable and records
// the queries it gets.
type recordingReadClient struct {
	queryable storage.Queryable
	queries   []*prompb.Query
}

func (c *recordingReadClient) Read(ctx context.Context, query *prompb.Query, sortSeries bool) (storage.SeriesSet, error) {
	c.queries = append(c.queries, query)
	matchers, err := FromLabelMatchers(query.Matchers)
	if err != nil {
		return nil, err
	}
	querier, err := c.queryable.Querier(query.StartTimestampMs, query.EndTimestampMs)
	if err != nil {
		return nil, err
	}
	defer querier.Close()
	res, _, err := ToQueryResult(querier.Select(ctx, sortSeries, nil, matchers...), 0)
	if err != nil {
		return nil, err
	}
	return FromQueryResult(sortSeries, res), nil
}

func (*recordingReadClient) ReadMultiple(context.Context, []*prompb.Query, bool) (storage.SeriesSet, error) {
	return nil, errors.New("not implemented")
}

// fanoutQueryable queries local storage and a remote read source like the fanout
// storage does.
type fanoutQueryable struct {
	local, remote storage.SampleAndChunkQueryable
}

func (f fanoutQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	local, err := f.local.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	remote, err := f.remote.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return storage.NewMergeQuerier([]storage.Querier{local}, []storage.Querier{remote}, storage.ChainedSeriesMerge), nil
}

func (f fanoutQueryable) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	local, err := f.local.ChunkQuerier(mint, maxt)
	if err != nil {
		return nil, err
	}
	remote, err := f.remote.ChunkQuerier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return storage.NewMergeChunkQuerier([]storage.ChunkQuerier{local}, []storage.ChunkQuerier{remote}, storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge)), nil
}

type failingChunkQueryable struct {
	storage.SampleAndChunkQueryable
	failQuery   int64
	failAfter   int
	err         error
	beforeError func(context.Context)
	opened      atomic.Int64
	closed      atomic.Int64
}

func (q *failingChunkQueryable) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	querier, err := q.SampleAndChunkQueryable.ChunkQuerier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &failingChunkQuerier{ChunkQuerier: querier, owner: q, fail: q.opened.Add(1) == q.failQuery}, nil
}

type failingChunkQuerier struct {
	storage.ChunkQuerier
	owner *failingChunkQueryable
	fail  bool
}

func (q *failingChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	set := q.ChunkQuerier.Select(ctx, sortSeries, hints, matchers...)
	if !q.fail {
		return set
	}
	return &failingChunkSeriesSet{
		ChunkSeriesSet: set, ctx: ctx, remaining: q.owner.failAfter,
		err: q.owner.err, beforeError: q.owner.beforeError,
	}
}

func (q *failingChunkQuerier) Close() error {
	err := q.ChunkQuerier.Close()
	q.owner.closed.Add(1)
	return err
}

type failingChunkSeriesSet struct {
	storage.ChunkSeriesSet
	ctx         context.Context
	remaining   int
	err         error
	beforeError func(context.Context)
}

func (s *failingChunkSeriesSet) Next() bool {
	if s.remaining == 0 {
		if s.beforeError != nil {
			s.beforeError(s.ctx)
		}
		return false
	}
	if !s.ChunkSeriesSet.Next() {
		return false
	}
	s.remaining--
	return true
}

func (s *failingChunkSeriesSet) Err() error {
	if s.remaining == 0 {
		return s.err
	}
	return s.ChunkSeriesSet.Err()
}

type flushingReadResponseWriter struct {
	http.ResponseWriter
}

func (w flushingReadResponseWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.Flush()
	return n, err
}

func (w flushingReadResponseWriter) Flush() {
	w.ResponseWriter.(http.Flusher).Flush()
}

type recordingReadBody struct {
	io.ReadCloser
	bytes bytes.Buffer
	err   error
}

func (r *recordingReadBody) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.bytes.Write(p[:n])
	if err != nil {
		r.err = err
	}
	return n, err
}

type failingReadResponseWriter struct {
	header    http.Header
	status    int
	headers   int
	writes    int
	flushes   int
	failWrite int
	partial   bool
}

func (w *failingReadResponseWriter) Header() http.Header { return w.header }

func (w *failingReadResponseWriter) WriteHeader(status int) {
	w.headers++
	if w.status == 0 {
		w.status = status
	}
}

func (w *failingReadResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	w.writes++
	if w.writes == w.failWrite {
		n := 0
		if w.partial {
			n = len(p) / 2
		}
		return n, errors.New("injected write error")
	}
	return len(p), nil
}

func (w *failingReadResponseWriter) Flush() {
	w.flushes++
}

func addNativeHistogramsToTestSuite(t *testing.T, s *teststorage.TestStorage, n int) {
	lbls := labels.FromStrings("__name__", "test_histogram_metric1", "baz", "qux")

	app := s.AppenderV2(t.Context())
	for i, fh := range tsdbutil.GenerateTestFloatHistograms(n) {
		_, err := app.Append(0, lbls, 0, int64(i)*int64(60*time.Second/time.Millisecond), 0, nil, fh, storage.AOptions{})
		require.NoError(t, err)
	}

	lbls = labels.FromStrings("__name__", "test_nhcb_metric1", "baz", "qux")
	for i, fh := range tsdbutil.GenerateTestCustomBucketsFloatHistograms(n) {
		_, err := app.Append(0, lbls, 0, int64(i)*int64(60*time.Second/time.Millisecond), 0, nil, fh, storage.AOptions{})
		require.NoError(t, err)
	}

	require.NoError(t, app.Commit())
}

func TestRemoteReadGateWaitDurationObserved(t *testing.T) {
	const (
		waitDuration  = 200 * time.Millisecond
		queryDuration = 100 * time.Millisecond
	)

	newHandlerAndRequest := func(t *testing.T, concurrencyLimit int, beforeQuery func()) (*readHandler, *prometheus.Registry, *http.Request) {
		t.Helper()
		queryable := gateWaitQueryable{Queryable: storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
			beforeQuery()
			return &storage.MockQuerier{
				SelectMockFunction: func(bool, *storage.SelectHints, ...*labels.Matcher) storage.SeriesSet {
					return storage.EmptySeriesSet()
				},
			}, nil
		})}

		reg := prometheus.NewRegistry()
		h, ok := NewReadHandler(nil, reg, queryable, func() config.Config {
			return config.Config{}
		}, 1e6, concurrencyLimit, 0).(*readHandler)
		require.True(t, ok)

		matcher, err := labels.NewMatcher(labels.MatchEqual, "__name__", "test_metric1")
		require.NoError(t, err)
		query, err := ToQuery(0, 1, []*labels.Matcher{matcher}, &storage.SelectHints{Step: 0, Func: "avg"})
		require.NoError(t, err)
		data, err := proto.Marshal(&prompb.ReadRequest{Queries: []*prompb.Query{query}})
		require.NoError(t, err)
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "", bytes.NewBuffer(snappy.Encode(nil, data)))
		require.NoError(t, err)
		return h, reg, request
	}

	isClosed := func(ch <-chan struct{}) bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}

	for _, tc := range []struct {
		name             string
		concurrencyLimit int
		contended        bool
		canceled         bool
		wantStatus       int
	}{
		{name: "successful acquisition", concurrencyLimit: 1, wantStatus: http.StatusOK},
		{name: "canceled acquisition", concurrencyLimit: 0, canceled: true, wantStatus: http.StatusInternalServerError},
		{name: "contended acquisition", concurrencyLimit: 1, contended: true, wantStatus: http.StatusOK},
		{name: "contended canceled acquisition", concurrencyLimit: 1, contended: true, canceled: true, wantStatus: http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queryStarted := make(chan struct{})
				queryContinue := make(chan struct{})
				releaseQuery := sync.OnceFunc(func() { close(queryContinue) })
				h, reg, request := newHandlerAndRequest(t, tc.concurrencyLimit, func() {
					close(queryStarted)
					<-queryContinue
				})
				ctx, cancel := context.WithCancel(t.Context())
				request = request.WithContext(ctx)
				recorder := httptest.NewRecorder()
				done := make(chan struct{})

				releaseGate := func() {}
				if tc.contended {
					require.NoError(t, h.remoteReadGate.Start(t.Context()))
					releaseGate = sync.OnceFunc(h.remoteReadGate.Done)
				}
				if !tc.contended || tc.canceled {
					releaseQuery()
				}
				if tc.canceled && !tc.contended {
					// With zero capacity, only the canceled context is ready in Gate.Start.
					cancel()
				}
				t.Cleanup(func() {
					cancel()
					releaseQuery()
					releaseGate()
					synctest.Wait()
					if !isClosed(done) {
						t.Error("remote read handler did not finish during cleanup")
					}
				})
				go func() {
					defer close(done)
					h.ServeHTTP(recorder, request)
				}()

				synctest.Wait()
				var wantWait time.Duration
				if tc.contended {
					require.False(t, isClosed(done), "request should be waiting on the gate")
					require.False(t, isClosed(queryStarted), "storage should not be queried before acquisition")
					hist := gatherHistogram(t, reg, "prometheus_remote_read_handler_gate_wait_duration_seconds")
					require.Zero(t, hist.GetSampleCount())
					require.Zero(t, hist.GetSampleSum())

					wantWait = waitDuration
					time.Sleep(waitDuration)
					if tc.canceled {
						// Keep the slot occupied until cancellation has completed.
						cancel()
					} else {
						releaseGate()
					}
					synctest.Wait()

					if !tc.canceled {
						require.True(t, isClosed(queryStarted), "request should have entered query processing")
						require.False(t, isClosed(done), "query processing should still be blocked")
						hist = gatherHistogram(t, reg, "prometheus_remote_read_handler_gate_wait_duration_seconds")
						require.Equal(t, uint64(1), hist.GetSampleCount())
						require.Equal(t, wantWait.Seconds(), hist.GetSampleSum())

						// Query processing must not add to the already-observed gate wait.
						time.Sleep(queryDuration)
						releaseQuery()
						synctest.Wait()
					}
				}

				require.True(t, isClosed(done), "remote read handler should have finished")
				require.Equal(t, tc.wantStatus, recorder.Code)
				require.Equal(t, !tc.canceled, isClosed(queryStarted), "canceled acquisition must not query storage")
				hist := gatherHistogram(t, reg, "prometheus_remote_read_handler_gate_wait_duration_seconds")
				require.Equal(t, uint64(1), hist.GetSampleCount())
				require.Equal(t, wantWait.Seconds(), hist.GetSampleSum())
				require.Zero(t, gatherGauge(t, reg, "prometheus_remote_read_handler_queries"))

				if tc.concurrencyLimit > 0 {
					releaseGate()
					gateCtx, gateCancel := context.WithTimeout(t.Context(), 2*time.Second)
					defer gateCancel()
					require.NoError(t, h.remoteReadGate.Start(gateCtx))
					h.remoteReadGate.Done()
				}
			})
		})
	}
}

type gateWaitQueryable struct {
	storage.Queryable
}

func (gateWaitQueryable) ChunkQuerier(int64, int64) (storage.ChunkQuerier, error) {
	return nil, errors.New("unexpected chunk query in gate wait test")
}

func gatherHistogram(t *testing.T, reg *prometheus.Registry, name string) *dto.Histogram {
	t.Helper()
	metric := gatherMetric(t, reg, name)
	require.NotNil(t, metric.GetHistogram())
	return metric.GetHistogram()
}

func gatherGauge(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	metric := gatherMetric(t, reg, name)
	require.NotNil(t, metric.GetGauge())
	return metric.GetGauge().GetValue()
}

func gatherMetric(t *testing.T, reg *prometheus.Registry, name string) *dto.Metric {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == name {
			require.Len(t, mf.GetMetric(), 1)
			return mf.GetMetric()[0]
		}
	}
	t.Fatalf("metric %q not found", name)
	return nil
}

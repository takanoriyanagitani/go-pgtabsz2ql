package graph

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	dl "github.com/graph-gophers/dataloader/v7"
	gm "github.com/takanoriyanagitani/go-pgtabsz2ql/graph/model"
)

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.

var ErrSizeLoaderNotDefined error = errors.New("size loader not defined")

type IO[T any] func(context.Context) (T, error)

func Of[T any](t T) IO[T] {
	return func(_ context.Context) (T, error) {
		return t, nil
	}
}

type SizeDataLoader = *dl.Loader[*gm.TableInfo, *gm.TableSizeInfo]

type SizeLoader struct {
	Loader SizeDataLoader
}

type SizeLoaderOpts struct {
	dl.Cache[*gm.TableInfo, *gm.TableSizeInfo]

	InputCapacity int
	Wait          time.Duration
}

var SizeLoaderOptsDefault SizeLoaderOpts = SizeLoaderOpts{
	Cache:         &dl.NoCache[*gm.TableInfo, *gm.TableSizeInfo]{},
	InputCapacity: 1024,
	Wait:          16 * time.Millisecond,
}

type BatchFnSizeInfo = dl.BatchFunc[*gm.TableInfo, *gm.TableSizeInfo]

func (o SizeLoaderOpts) ToLoader(
	bf BatchFnSizeInfo,
) SizeDataLoader {
	return dl.NewBatchedLoader[*gm.TableInfo, *gm.TableSizeInfo](
		bf,
		dl.WithCache[*gm.TableInfo, *gm.TableSizeInfo](o.Cache),
		dl.WithInputCapacity[*gm.TableInfo, *gm.TableSizeInfo](o.InputCapacity),
		dl.WithWait[*gm.TableInfo, *gm.TableSizeInfo](o.Wait),
	)
}

var sizeLoaderKey struct{}

func (l SizeLoader) GetSize(
	ctx context.Context,
	ti *gm.TableInfo,
) (*gm.TableSizeInfo, error) {
	var th dl.Thunk[*gm.TableSizeInfo] = l.Loader.Load(ctx, ti)
	return th()
}

func (l SizeLoader) ToContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, sizeLoaderKey, l)
}

func (l SizeLoader) WrapHandler(original http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var old context.Context = r.Context()
		var neo context.Context = l.ToContext(old)
		original.ServeHTTP(w, r.WithContext(neo))
	})
}

type RawTableInfo struct {
	TableSchema string
	TableName   string
}

func (t RawTableInfo) ToPartialInfo() *gm.TableInfo {
	return &gm.TableInfo{
		TableSchema: t.TableSchema,
		TableName:   t.TableName,
		TableSize:   nil,
	}
}

type RawSizeInfo struct {
	Oid         string
	RowEstimate float64
	TotalBytes  int64
	IndexBytes  int64
	ToastBytes  *int64
	TableSchema string
	TableName   string
}

func (s RawSizeInfo) ToSizeInfo() *gm.TableSizeInfo {
	return &gm.TableSizeInfo{
		Oid:         s.Oid,
		RowEstimate: s.RowEstimate,
		TotalBytes:  s.TotalBytes,
		IndexBytes:  s.IndexBytes,
		ToastBytes:  s.ToastBytes,
		TableSchema: s.TableSchema,
		TableName:   s.TableName,
	}
}

type RawTableSource func(gm.TablesInput) IO[[]RawTableInfo]

type RawSizeSource func(gm.TableSizeInput) IO[[]RawSizeInfo]

type Resolver struct {
	RawTableSource
	RawSizeSource
}

func (r *Resolver) ToPartialTables(
	ctx context.Context,
	i gm.TablesInput,
) ([]*gm.TableInfo, error) {
	raws, e := r.RawTableSource(i)(ctx)
	if nil != e {
		return nil, e
	}

	ret := make([]*gm.TableInfo, 0, len(raws))
	for _, raw := range raws {
		ret = append(ret, raw.ToPartialInfo())
	}
	return ret, nil
}

type TablesInfo []*gm.TableInfo

func (t TablesInfo) ToNames() []string {
	ret := make([]string, 0, len(t))
	for _, i := range t {
		ret = append(ret, i.TableName)
	}
	return ret
}

type RawSizeInfoArr []RawSizeInfo

func (a RawSizeInfoArr) ToMap(schema string) map[string]*gm.TableSizeInfo {
	tmap := map[string]*gm.TableSizeInfo{}
	for _, raw := range a {
		var sch string = raw.TableSchema
		if sch != schema {
			continue
		}

		var mapd *gm.TableSizeInfo = raw.ToSizeInfo()
		var tab string = raw.TableName
		tmap[tab] = mapd
	}
	return tmap
}

type SizeMap map[string]*gm.TableSizeInfo

func (m SizeMap) ToResults(tabs []string) []*dl.Result[*gm.TableSizeInfo] {
	ret := make([]*dl.Result[*gm.TableSizeInfo], 0, len(tabs))
	for _, tab := range tabs {
		res := &dl.Result[*gm.TableSizeInfo]{
			Data:  nil,
			Error: nil,
		}

		tsi, found := m[tab]
		if found {
			res.Data = tsi
		} else {
			res.Data = &gm.TableSizeInfo{}
		}

		ret = append(ret, res)
	}
	return ret
}

// Creates BatchFnSizeInfo.
// Note: The table info must have the same table schema.
func (r *Resolver) ToSizeBatchFn() BatchFnSizeInfo {
	return func(
		ctx context.Context,
		keys []*gm.TableInfo,
	) []*dl.Result[*gm.TableSizeInfo] {
		if len(keys) < 1 {
			return nil
		}

		var row1st *gm.TableInfo = keys[0]
		var tableSchema string = row1st.TableSchema

		var tableNames []string = TablesInfo(keys).ToNames()

		input := gm.TableSizeInput{
			SchemaPattern: tableSchema,
			TableNames:    tableNames,
		}

		var iraws IO[[]RawSizeInfo] = r.RawSizeSource(input)

		ret := make([]*dl.Result[*gm.TableSizeInfo], 0, len(keys))

		raws, e := iraws(ctx)

		if nil != e {
			log.Printf("err: %v\n", e)
			for range keys {
				ret = append(ret, &dl.Result[*gm.TableSizeInfo]{
					Data:  nil,
					Error: e,
				})
			}
			return ret
		}

		var tmap map[string]*gm.TableSizeInfo = RawSizeInfoArr(raws).
			ToMap(tableSchema)

		return SizeMap(tmap).ToResults(tableNames)
	}
}

func (r *Resolver) ToSizeLoader(
	opts SizeLoaderOpts,
) SizeDataLoader {
	return opts.ToLoader(r.ToSizeBatchFn())
}

func (r *Resolver) ToSizeLoaderDefault() SizeDataLoader {
	return r.ToSizeLoader(SizeLoaderOptsDefault)
}

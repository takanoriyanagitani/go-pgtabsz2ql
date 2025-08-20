package main

import (
	"context"
	"log"
	"net/http"

	gh "github.com/99designs/gqlgen/graphql/handler"
	ge "github.com/99designs/gqlgen/graphql/handler/extension"
	gt "github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/jackc/pgx/v5/pgxpool"
	tq "github.com/takanoriyanagitani/go-pgtabsz"
	qp "github.com/takanoriyanagitani/go-pgtabsz/repo/pgx2tabsz"
	"github.com/takanoriyanagitani/go-pgtabsz2ql/graph"
	gm "github.com/takanoriyanagitani/go-pgtabsz2ql/graph/model"
)

var pgpool tq.IO[*pgxpool.Pool] = func(
	ctx context.Context,
) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, "")
}

var pool tq.IO[qp.Pool] = tq.Bind(
	pgpool,
	tq.Lift(func(p *pgxpool.Pool) (qp.Pool, error) {
		return qp.Pool{Pool: p}, nil
	}),
)

type PgxSource struct {
	tq.TableSizesSource
	tq.TablesSource
}

func (x PgxSource) ToRawSizeSource() graph.RawSizeSource {
	return ts2(x.TableSizesSource).ToRawSource()
}

func (x PgxSource) ToRawTableSource() graph.RawTableSource {
	return ts(x.TablesSource).ToRawSource()
}

var psource tq.IO[PgxSource] = tq.Bind(
	pool,
	tq.Lift(func(p qp.Pool) (PgxSource, error) {
		return PgxSource{
			TableSizesSource: p.AsSizeSource(),
			TablesSource:     p.AsTablesSource(),
		}, nil
	}),
)

type RawSource struct {
	graph.RawTableSource
	graph.RawSizeSource
}

type tsi tq.TableSizeInfo

func (s tsi) ToRaw() graph.RawSizeInfo {
	var tb *int64
	if s.ToastBytes.Valid {
		tb = &s.ToastBytes.V
	}
	return graph.RawSizeInfo{
		Oid:         s.OID,
		RowEstimate: float64(s.RowEstimate),
		TotalBytes:  s.TotalBytes,
		IndexBytes:  s.IndexBytes,
		ToastBytes:  tb,
		TableSchema: s.TableSchema,
		TableName:   s.TableName,
	}
}

type tinfo tq.TableInfo

func (t tinfo) ToRaw() graph.RawTableInfo {
	var sch string = t.TableSchema
	var tab string = t.TableName
	return graph.RawTableInfo{
		TableSchema: sch,
		TableName:   tab,
	}
}

type ts2 tq.TableSizesSource

func (s ts2) ToRawSource() func(gm.TableSizeInput) graph.IO[[]graph.RawSizeInfo] {
	return func(i gm.TableSizeInput) graph.IO[[]graph.RawSizeInfo] {
		return func(ctx context.Context) ([]graph.RawSizeInfo, error) {
			var spat string = i.SchemaPattern
			var names []string = i.TableNames
			i := tq.TableSizeInput{
				SchemaPattern: spat,
				TableNames:    names,
			}
			var itsi tq.IO[[]tq.TableSizeInfo] = s(i)
			var iraw tq.IO[[]graph.RawSizeInfo] = tq.Bind(
				itsi,
				tq.Lift(func(t []tq.TableSizeInfo) ([]graph.RawSizeInfo, error) {
					var ret []graph.RawSizeInfo = make([]graph.RawSizeInfo, 0, len(t))
					for _, ti := range t {
						ret = append(ret, tsi(ti).ToRaw())
					}
					return ret, nil
				}),
			)
			return iraw(ctx)
		}
	}
}

type ts tq.TablesSource

func (s ts) ToRawSource() func(gm.TablesInput) graph.IO[[]graph.RawTableInfo] {
	return func(i gm.TablesInput) graph.IO[[]graph.RawTableInfo] {
		var spat string = i.SchemaPattern
		var tpat string = i.TablePattern
		ti := tq.TablesInput{
			SchemaPattern: spat,
			TablePattern:  tpat,
		}
		var iti tq.IO[[]tq.TableInfo] = s(ti)
		var irt tq.IO[[]graph.RawTableInfo] = tq.Bind(
			iti,
			tq.Lift(func(ti []tq.TableInfo) ([]graph.RawTableInfo, error) {
				ret := make([]graph.RawTableInfo, 0, len(ti))
				for _, i := range ti {
					ret = append(ret, tinfo(i).ToRaw())
				}
				return ret, nil
			}),
		)
		return graph.IO[[]graph.RawTableInfo](irt)
	}
}

var irsource tq.IO[RawSource] = tq.Bind(
	psource,
	tq.Lift(func(r PgxSource) (RawSource, error) {
		return RawSource{
			RawTableSource: r.ToRawTableSource(),
			RawSizeSource:  r.ToRawSizeSource(),
		}, nil
	}),
)

var sub tq.IO[tq.Void] = func(ctx context.Context) (tq.Void, error) {
	rsrc, e := irsource(ctx)
	if nil != e {
		return tq.Empty, nil
	}

	var rts graph.RawTableSource = rsrc.RawTableSource
	var rs2 graph.RawSizeSource = rsrc.RawSizeSource

	var res *graph.Resolver = &graph.Resolver{
		RawTableSource: rts,
		RawSizeSource:  rs2,
	}

	var sdl graph.SizeDataLoader = res.ToSizeLoaderDefault()
	sl := graph.SizeLoader{Loader: sdl}

	srv := gh.New(
		graph.NewExecutableSchema(graph.Config{Resolvers: res}),
	)

	srv.AddTransport(gt.GET{})
	srv.AddTransport(gt.POST{})

	srv.Use(ge.Introspection{})

	router := http.NewServeMux()

	router.Handle("/query", srv)

	var wh http.Handler = sl.WrapHandler(router)

	var addrPort string = ":8189"

	e = http.ListenAndServe(addrPort, wh)

	return tq.Empty, e
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}

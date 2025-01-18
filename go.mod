module github.com/inngest/inngest

go 1.23.2

toolchain go1.23.3

replace github.com/tencentcloud/tencentcloud-sdk-go v3.0.82+incompatible => github.com/tencentcloud/tencentcloud-sdk-go v1.0.191

require (
	connectrpc.com/connect v1.16.1
	cuelang.org/go v0.4.2
	github.com/99designs/gqlgen v0.17.27
	github.com/MauriceGit/skiplist v0.0.0-20211105230623-77f5c8d3e145
	github.com/VividCortex/ewma v1.2.0
	github.com/alicebob/miniredis/v2 v2.30.3-0.20230520070231-a946a99f2c60
	github.com/aws/aws-lambda-go v1.41.0
	github.com/aws/smithy-go v1.13.3
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/charmbracelet/lipgloss v0.5.0
	github.com/coder/websocket v1.8.12
	github.com/coocood/freecache v1.2.3
	github.com/davecgh/go-spew v1.1.1
	github.com/dmarkham/enumer v1.5.8
	github.com/doug-martin/goqu/v9 v9.19.0
	github.com/dustinkirkland/golang-petname v0.0.0-20191129215211-8e5a1ed0cff0
	github.com/eko/gocache/lib/v4 v4.1.5
	github.com/eko/gocache/store/freecache/v4 v4.2.1
	github.com/eko/gocache/store/rueidis/v4 v4.1.5
	github.com/fatih/structs v1.1.0
	github.com/go-chi/chi/v5 v5.0.10
	github.com/go-chi/cors v1.2.1
	github.com/gogo/protobuf v1.3.2
	github.com/golang-jwt/jwt/v5 v5.2.1
	github.com/golang-migrate/migrate/v4 v4.16.2
	github.com/google/cel-go v0.21.0
	github.com/google/uuid v1.6.0
	github.com/gosimple/slug v1.12.0
	github.com/gowebpki/jcs v1.0.0
	github.com/graph-gophers/dataloader v5.0.0+incompatible
	github.com/hashicorp/go-multierror v1.1.1
	github.com/inngest/expr v0.0.0-20241106234328-863dff7deec0
	github.com/inngest/inngestgo v0.7.5-0.20250117112209-0199fc7b08a1
	github.com/jackc/pgx/v5 v5.5.4
	github.com/jedib0t/go-pretty/v6 v6.3.0
	github.com/jinzhu/copier v0.3.5
	github.com/jonboulle/clockwork v0.4.0
	github.com/karlseguin/ccache/v2 v2.0.8
	github.com/lib/pq v1.10.6
	github.com/liushuangls/go-anthropic/v2 v2.12.2
	github.com/mattn/go-isatty v0.0.20
	github.com/mitchellh/go-homedir v1.1.0
	github.com/nats-io/nats.go v1.37.0
	github.com/oklog/ulid/v2 v2.1.0
	github.com/pkg/errors v0.9.1
	github.com/pquerna/cachecontrol v0.2.0
	github.com/redis/rueidis v1.0.51
	github.com/robfig/cron/v3 v3.0.1
	github.com/rs/zerolog v1.26.1
	github.com/sashabaranov/go-openai v1.35.6
	github.com/shirou/gopsutil v3.21.11+incompatible
	github.com/spf13/cobra v1.8.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.10.1
	github.com/stretchr/testify v1.9.0
	github.com/throttled/throttled/v2 v2.11.0
	github.com/valyala/fastjson v1.6.4
	github.com/vektah/gqlparser/v2 v2.5.15
	github.com/xhit/go-str2duration/v2 v2.1.0
	go.opentelemetry.io/collector/pdata v1.6.0
	go.opentelemetry.io/otel v1.28.0
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.28.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.28.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.28.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.28.0
	go.opentelemetry.io/otel/exporters/prometheus v0.44.0
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.28.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.28.0
	go.opentelemetry.io/otel/metric v1.28.0
	go.opentelemetry.io/otel/sdk v1.28.0
	go.opentelemetry.io/otel/sdk/metric v1.28.0
	go.opentelemetry.io/otel/trace v1.28.0
	gocloud.dev v0.25.0
	gocloud.dev/pubsub/natspubsub v0.25.0
	golang.org/x/mod v0.21.0
	golang.org/x/sync v0.10.0
	golang.org/x/term v0.27.0
	gonum.org/v1/gonum v0.12.0
	google.golang.org/genproto/googleapis/api v0.0.0-20241021214115-324edc3d5d38
	google.golang.org/grpc v1.67.1
	google.golang.org/protobuf v1.35.1
	lukechampine.com/frand v1.4.2
	modernc.org/sqlite v1.25.0
)

require (
	cloud.google.com/go v0.112.1 // indirect
	cloud.google.com/go/compute/metadata v0.5.0 // indirect
	cloud.google.com/go/iam v1.1.6 // indirect
	cloud.google.com/go/pubsub v1.36.1 // indirect
	github.com/aead/chacha20 v0.0.0-20180709150244-8b13a72661da // indirect
	github.com/agnivade/levenshtein v1.1.1 // indirect
	github.com/alicebob/gopher-json v0.0.0-20200520072559-a9ecdc9d1d3a // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/aws/aws-sdk-go v1.43.31 // indirect
	github.com/aws/aws-sdk-go-v2 v1.16.16 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.15.3 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.12.20 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.12.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/sns v1.17.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sqs v1.18.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.11.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.13.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.16.19 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cockroachdb/apd/v2 v2.0.2 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.4 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/emicklei/proto v1.6.15 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/golang/glog v1.2.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/google/wire v0.5.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.2 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/gosimple/unidecode v1.0.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.20.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.1 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/klauspost/compress v1.17.2 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/magiconair/properties v1.8.5 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/matttproud/golang_protobuf_extensions/v2 v2.0.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mpvl/unique v0.0.0-20150818121801-cbe035fff7de // indirect
	github.com/muesli/reflow v0.3.0 // indirect
	github.com/muesli/termenv v0.11.1-0.20220212125758-44cd13922739 // indirect
	github.com/nats-io/nkeys v0.4.7 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/ohler55/ojg v1.24.1 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pascaldekloe/name v1.0.1 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/pelletier/go-toml v1.9.4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.17.0 // indirect
	github.com/prometheus/client_model v0.5.0 // indirect
	github.com/prometheus/common v0.45.0 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	github.com/protocolbuffers/txtpbfmt v0.0.0-20201118171849-f6a6b3f636fc // indirect
	github.com/redis/rueidis/rueidiscompat v1.0.51 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spf13/afero v1.6.0 // indirect
	github.com/spf13/cast v1.4.1 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/stoewer/go-strcase v1.3.0 // indirect
	github.com/subosito/gotenv v1.2.0 // indirect
	github.com/tidwall/btree v1.7.0 // indirect
	github.com/tklauser/go-sysconf v0.3.10 // indirect
	github.com/tklauser/numcpus v0.4.0 // indirect
	github.com/urfave/cli/v2 v2.25.1 // indirect
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	github.com/yuin/gopher-lua v1.1.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.49.0 // indirect
	go.opentelemetry.io/proto/otlp v1.3.1 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/exp v0.0.0-20241009180824-f66d83c29e7c // indirect
	golang.org/x/net v0.30.0 // indirect
	golang.org/x/oauth2 v0.24.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	golang.org/x/tools v0.26.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/api v0.169.0 // indirect
	google.golang.org/genproto v0.0.0-20240227224415-6ceb2ff114de // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241021214115-324edc3d5d38 // indirect
	gopkg.in/ini.v1 v1.66.2 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	lukechampine.com/uint128 v1.2.0 // indirect
	modernc.org/cc/v3 v3.40.0 // indirect
	modernc.org/ccgo/v3 v3.16.13 // indirect
	modernc.org/libc v1.24.1 // indirect
	modernc.org/mathutil v1.5.0 // indirect
	modernc.org/memory v1.6.0 // indirect
	modernc.org/opt v0.1.3 // indirect
	modernc.org/strutil v1.1.3 // indirect
	modernc.org/token v1.0.1 // indirect
)

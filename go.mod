module gitlab.cebc.tools/go/tp.git

go 1.24

toolchain go1.24.1

tool (
	gitlab.cebc.tools/go/iproto.git/cmd/iprotogen
	go.uber.org/mock/mockgen
)

require (
	github.com/gofrs/uuid v4.4.0+incompatible
	github.com/golang/mock v1.6.0
	github.com/google/go-cmp v0.6.0
	github.com/onlineconf/onlineconf-go v1.2.0
	github.com/rs/zerolog v1.30.0
	github.com/stretchr/testify v1.10.0
	gitlab.cebc.tools/go/easygo.git v0.0.7
	gitlab.cebc.tools/go/iproto.git v0.0.6
	gitlab.cebc.tools/go/queuedproto.git v0.0.5
	gitlab.cebc.tools/go/utils.git v0.0.10
	go.uber.org/mock v0.3.0
	google.golang.org/protobuf v1.36.0
	gopkg.in/yaml.v3 v3.0.1
	kythe.io v0.0.67
)

require (
	github.com/fatih/structtag v1.2.0 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	golang.org/x/mod v0.22.0 // indirect
	golang.org/x/sync v0.10.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	golang.org/x/tools v0.28.0 // indirect
)

require (
	github.com/antchfx/jsonquery v1.3.6 // indirect
	github.com/antchfx/xpath v1.3.3 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/colinmarc/cdb v0.0.0-20190223170904-60f317823f70 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/fsnotify/fsnotify v1.8.0 // indirect
	github.com/go-faker/faker/v4 v4.2.0
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/mailru/easyjson v0.9.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/my-mail-ru/exp/mmap v0.0.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.20.5 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.61.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/tidwall/gjson v1.18.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
	go4.org v0.0.0-20230225012048-214862532bf5 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/term v0.27.0 // indirect
	golang.org/x/time v0.8.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
